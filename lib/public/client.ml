(** Implements typed client handles over the private semantic backend. *)

(** The client state is intentionally opaque in the public interface. A single
    backend value owns all native resources for this SDK instance. *)
type t = {
  backend : Backend.client;
  closed : bool Atomic.t;
  shutdown_mutex : Mutex.t;
  mutable shutdown_result : (unit, Error.t) result option;
}

(** A handle retains the definition codecs and exact execution identity. *)
type ('input, 'output) handle = {
  client : t;
  workflow : ('input, 'output) Workflow.t;
  workflow_id : string;
  run_id : string;
}

(** Terminal outcomes mirror the backend while replacing payload bytes with the
    definition's typed output. *)
type 'output terminal_result =
  | Completed of 'output
  | Failed of Error.t
  | Cancelled of Error.t
  | Terminated of Error.t
  | Timed_out of Error.t
  | Continued_as_new of {
      workflow_id : string;
      run_id : string;
    }

(** The default identity is stable and descriptive without using process-global
    randomness, which keeps client construction straightforward in tests. *)
let default_identity = "ocaml-temporal-client"

(** Rejects empty strings and NUL bytes before they can enter a backend request. *)
let validate_name field value =
  if String.equal value "" then
    Error (Error.defect ~message:(field ^ " must not be empty"))
  else if String.contains value '\000' then
    Error (Error.defect ~message:(field ^ " must not contain NUL"))
  else Ok ()

(** Builds the private backend configuration after checking every user-facing
    connection field. Routine configuration failures remain [result] values. *)
let create ?(identity = default_identity) ~target_url ~namespace () =
  match validate_name "namespace" namespace with
  | Error error -> Error error
  | Ok () -> (
      match validate_name "identity" identity with
      | Error error -> Error error
      | Ok () ->
          let config : Backend.config =
            { target_url; namespace; identity; task_queue = None }
          in
          Result.map
            (fun backend ->
              {
                backend;
                closed = Atomic.make false;
                shutdown_mutex = Mutex.create ();
                shutdown_result = None;
              })
            (Backend.client_create config))

(** Validates the optional Temporal idempotency key, durable workflow ID, and
    task queue before encoding input or constructing a native request. Keeping
    these checks here makes malformed caller input a typed result and prevents
    it from crossing the supervisor boundary. *)
let validate_start_fields ~request_id ~id ~task_queue =
  let request_result =
    match request_id with
    | None -> Ok ()
    | Some request_id -> validate_name "request id" request_id
  in
  match request_result with
  | Error _ as error -> error
  | Ok () -> (
      match validate_name "workflow id" id with
      | Error _ as error -> error
      | Ok () -> validate_name "task queue" task_queue)

(** Starts a workflow after encoding its typed input and checking the backend's
    response still refers to the request. The response check prevents an
    adapter bug from creating a handle for a different execution. *)
let start client ?request_id ~workflow ~task_queue ~id ~input () =
  if Atomic.get client.closed then
    Error
      (Error.make ~category:`Bridge ~message:"client is shut down" ())
  else
    match validate_start_fields ~request_id ~id ~task_queue with
    | Error error -> Error error
    | Ok () -> (
        match Codec.encode (Workflow.input workflow) input with
        | Error error -> Error error
        | Ok encoded_input ->
            let request : Backend.start_request =
              {
                request_id;
                workflow_name = Workflow.name workflow;
                workflow_id = id;
                task_queue;
                input = encoded_input;
              }
            in
            Result.bind (Backend.client_start client.backend request) (fun response ->
                if not (String.equal response.workflow_id id) then
                  Error
                    (Error.make ~category:`Bridge
                       ~message:"backend returned a different workflow id" ())
                else if String.equal response.run_id "" then
                  Error
                    (Error.make ~category:`Bridge
                       ~message:"backend returned an empty run id" ())
                else
                  Ok
                    {
                      client;
                      workflow;
                      workflow_id = id;
                      run_id = response.run_id;
                    }))

(** Decodes a completed payload and maps terminal failures without exposing the
    private backend constructors. *)
let wait handle =
  if Atomic.get handle.client.closed then
    Error
      (Error.make ~category:`Bridge ~message:"client is shut down" ())
  else
    let request : Backend.wait_request =
      { workflow_id = handle.workflow_id; run_id = handle.run_id }
    in
    Result.bind (Backend.client_wait handle.client.backend request) (function
      | Backend.Completed payload ->
          Result.map
            (fun output -> Completed output)
            (Codec.decode (Workflow.output handle.workflow) payload)
      | Backend.Failed error -> Ok (Failed error)
      | Backend.Cancelled error -> Ok (Cancelled error)
      | Backend.Terminated error -> Ok (Terminated error)
      | Backend.Timed_out error -> Ok (Timed_out error)
      | Backend.Continued_as_new { workflow_id; run_id } ->
          Ok (Continued_as_new { workflow_id; run_id }))

(** Returns the durable workflow identity retained by a handle. *)
let workflow_id handle = handle.workflow_id

(** Returns the exact server run identity retained by a handle. *)
let run_id handle = handle.run_id

(** Closes backend resources once and returns the same cached result to later
    shutdown callers.
    Native supervisor shutdown is terminal and cached: even when its result is
    an error, the backend contract says the complete graph was consumed or
    invalidated, so the atomic state transition cannot hide a live resource. *)
let shutdown client =
  Mutex.lock client.shutdown_mutex;
  let result =
    match client.shutdown_result with
    | Some result -> result
    | None ->
        (* Close admission before entering native teardown. Concurrent starts
           that already passed their check are ordered by the supervisor; later
           callers observe the closed bit and cannot enqueue new work. *)
        Atomic.set client.closed true;
        let result =
          try Backend.client_shutdown client.backend with
          | exception_ ->
              Error
                (Error.defect
                   ~message:
                     (Printf.sprintf "client shutdown raised: %s"
                        (Printexc.to_string exception_)))
        in
        client.shutdown_result <- Some result;
        result
  in
  Mutex.unlock client.shutdown_mutex;
  result
