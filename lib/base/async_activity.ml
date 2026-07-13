(** Correctness-first state machine for a retained asynchronous activity
    completion capability.

    The handle starts dormant because the callback may return a handle before
    the worker has accepted the [WillCompleteAsync] handoff. Every operation
    reserves a request key under [mutex], releases the lock while entering the
    supervisor, and commits the result under the lock. Consequently a second
    Domain cannot submit a conflicting terminal operation, while a transport
    failure leaves the exact operation available for a later retry. *)

type operation =
  | Complete of Payload.t
  | Fail of Error.t
  | Cancel of Payload.t list
  | Heartbeat of Payload.t list

type submit_result = (unit, Error.t) result

type lifecycle = Dormant | Handoff_pending | Active | Terminal | Closed

type pending = {
  key : string;
  mutable in_flight : bool;
}

type 'output handle = {
  mutex : Mutex.t;
  mutable lifecycle : lifecycle;
  mutable pending : pending option;
  submit : operation -> submit_result;
  encode_output : 'output -> (Payload.t, Error.t) result;
}

type 'output context = { handle : 'output handle }

type 'output async_result =
  | Completed of 'output
  | Failed of Error.t
  | Will_complete_async of 'output handle

type ('input, 'output) implementation =
  'output context -> 'input -> 'output async_result

let lifecycle_error message =
  Error (Error.make ~non_retryable:true ~category:`Activity ~message ())

let with_mutex mutex operation =
  Mutex.lock mutex;
  Fun.protect ~finally:(fun () -> Mutex.unlock mutex) operation

let create ~submit ~encode_output =
  {
    mutex = Mutex.create ();
    lifecycle = Dormant;
    pending = None;
    submit;
    encode_output;
  }

let context handle = { handle }
let handle context = context.handle

let activate handle =
  with_mutex handle.mutex (fun () ->
      match handle.lifecycle with
      | Dormant | Handoff_pending ->
          handle.lifecycle <- Active;
          Ok ()
      | Active | Terminal | Closed ->
          lifecycle_error "asynchronous activity handle is already activated")

let prepare_handoff ~expected handle =
  if not (expected == handle) then
    lifecycle_error
      "asynchronous activity handle belongs to another activity attempt"
  else
    with_mutex handle.mutex (fun () ->
        match handle.lifecycle with
        | Dormant ->
            handle.lifecycle <- Handoff_pending;
            Ok ()
        | Handoff_pending ->
            lifecycle_error
              "asynchronous activity handle is already reserved for a handoff"
        | Active | Terminal | Closed ->
            lifecycle_error
              "asynchronous activity handle cannot be reserved for a handoff")

let begin_operation handle ~key operation =
  with_mutex handle.mutex (fun () ->
      match handle.lifecycle with
      | Dormant ->
          Error
            (Error.make ~non_retryable:true ~category:`Activity
               ~message:
                 "asynchronous activity handle is not active; return Will_complete_async first"
               ())
      | Closed -> lifecycle_error "asynchronous activity handle is closed"
      | Handoff_pending ->
          Error
            (Error.make ~non_retryable:true ~category:`Activity
               ~message:
                 "asynchronous activity handle is waiting for the worker handoff"
               ())
      | Terminal -> lifecycle_error "asynchronous activity handle is terminal"
      | Active -> (
          match handle.pending with
          | Some pending when not (String.equal pending.key key) ->
              lifecycle_error
                "a different asynchronous activity operation is already pending"
          | Some pending when pending.in_flight ->
              lifecycle_error
                "the same asynchronous activity operation is already in flight"
          | Some pending ->
              pending.in_flight <- true;
              Ok (pending, operation)
          | None ->
              let pending = { key; in_flight = true } in
              handle.pending <- Some pending;
              Ok (pending, operation)))

(* The request key is an internal idempotency token.  Length-prefixing every
   field avoids ambiguities such as ["ab", "c"] versus ["a", "bc"] and keeps
   retries tied to the exact operation without exposing a protocol detail in
   the public API.  This is not a cryptographic identifier; it only compares
   requests retained by one handle. *)
let add_field buffer value =
  Buffer.add_string buffer (string_of_int (String.length value));
  Buffer.add_char buffer ':';
  Buffer.add_string buffer value

let add_payload buffer ({ Payload.metadata; data } : Payload.t) =
  add_field buffer "payload";
  add_field buffer (string_of_int (List.length metadata));
  List.iter
    (fun (key, value) ->
      add_field buffer "metadata";
      add_field buffer key;
      add_field buffer value)
    metadata;
  add_field buffer "data";
  add_field buffer (string_of_int (Bytes.length data));
  add_field buffer (Bytes.to_string data)

let add_payloads buffer payloads =
  add_field buffer "payloads";
  add_field buffer (string_of_int (List.length payloads));
  List.iter (add_payload buffer) payloads

let operation_key operation =
  let buffer = Buffer.create 64 in
  (match operation with
  | Complete payload ->
      add_field buffer "complete";
      add_payload buffer payload
  | Fail error ->
      add_field buffer "fail";
      let ({ Error.message; non_retryable; details; _ } : Error.view) =
        Error.view error
      in
      add_field buffer (Error.kind error);
      add_field buffer message;
      add_field buffer (if non_retryable then "1" else "0");
      add_payloads buffer details
  | Cancel payloads ->
      add_field buffer "cancel";
      add_payloads buffer payloads
  | Heartbeat payloads ->
      add_field buffer "heartbeat";
      add_payloads buffer payloads);
  Buffer.contents buffer

(* A non-retryable bridge result means the native side has proved that this
   retained capability cannot be used again (for example, Temporal returned
   NotFound for an expired or already-completed task token). Closing the local
   state machine as well as the adapter lease prevents a caller from retaining
   an otherwise live callback closure and accidentally issuing a duplicate
   request after the remote operation is terminal. Application and codec
   errors are either raised before [submit_operation] or are deliberately
   fail-closed at this boundary; retryable transport failures keep the exact
   pending request for an explicit retry. *)
let should_close_after_error (error : Error.t) =
  let view = Error.view error in
  view.category = `Bridge && view.non_retryable

let submit_operation handle ~terminal operation =
  let key = operation_key operation in
  match begin_operation handle ~key operation with
  | Error _ as error -> error
  | Ok (pending, operation) ->
      let result =
        try handle.submit operation with exception_ ->
          Error
            (Error.make ~non_retryable:false ~category:`Bridge
               ~message:
                 (Printf.sprintf
                    "asynchronous activity operation raised: %s"
                    (Printexc.to_string exception_))
               ())
      in
      with_mutex handle.mutex (fun () ->
          match result with
          | Error _ ->
              (* Keep the pending key so only the byte-identical request can
                 be retried. The callback is never rerun. A terminal bridge
                 error instead closes the capability and drops the pending
                 request, because retaining it could make a stale native token
                 appear retryable. *)
              pending.in_flight <- false;
              (match result with
              | Error error when should_close_after_error error ->
                  handle.lifecycle <- Closed;
                  handle.pending <- None
              | Error _ -> ()
              | Ok () -> ());
              result
          | Ok () ->
              pending.in_flight <- false;
              handle.pending <- None;
              if terminal then handle.lifecycle <- Terminal;
              Ok ())

let complete handle output =
  match handle.encode_output output with
  | Error error -> Error error
  | Ok payload -> submit_operation handle ~terminal:true (Complete payload)

let fail handle error = submit_operation handle ~terminal:true (Fail error)
let cancel handle details =
  submit_operation handle ~terminal:true (Cancel details)

let heartbeat handle details =
  submit_operation handle ~terminal:false (Heartbeat details)

let close handle =
  with_mutex handle.mutex (fun () ->
      match handle.pending with
      | Some { in_flight = true; _ } ->
          Error
            (Error.make ~non_retryable:true ~category:`Activity
               ~message:
                 "cannot close asynchronous activity handle while an operation is in flight"
               ())
      | _ ->
          handle.lifecycle <- Closed;
          handle.pending <- None;
          Ok ())
