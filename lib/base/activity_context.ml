(** Lifetime-checked state shared by one activity implementation and its
    native heartbeat callback. The mutex is local to the context: the adapter's
    poll mutex serializes task execution, while this shorter lock protects a
    context retained accidentally by user code. *)
type t = {
  mutex : Mutex.t;
  mutable active : bool;
  mutable details : Payload.t list;
  heartbeat_timeout : Duration.t option;
  heartbeat_fn : Payload.t list -> (unit, Error.t) result;
}

(** Copies metadata and bytes so no callback can retain a buffer owned by the
    JSON decoder or another activity invocation. *)
let copy_payload ({ Payload.metadata; data } : Payload.t) : Payload.t =
  {
    Payload.metadata = List.map (fun (key, value) -> (key, value)) metadata;
    data = Bytes.copy data;
  }

(** Copies a heartbeat detail list without changing its order. *)
let copy_payloads values = List.map copy_payload values

(** Constructs an active context after the adapter has validated details and
    timeout values. *)
let create ~heartbeat ~details ~heartbeat_timeout =
  {
    mutex = Mutex.create ();
    active = true;
    details = copy_payloads details;
    heartbeat_timeout;
    heartbeat_fn = heartbeat;
  }

(** Supplies a typed bridge error for the deterministic mock backend, which
    has no native Core worker capable of recording a heartbeat. *)
let unavailable ~details ~heartbeat_timeout =
  create ~details ~heartbeat_timeout ~heartbeat:(fun _details ->
      Error
        (Error.make ~category:`Bridge
           ~message:"activity heartbeat is unavailable on this worker backend"
           ()))

(** Runs one callback only while the context is active. The callback and
    invalidation are serialized so a terminal completion cannot race a token
    submission. Unexpected callback exceptions become non-retryable typed
    defects. *)
let heartbeat context details =
  Mutex.lock context.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock context.mutex)
    (fun () ->
      if not context.active then
        Error
          (Error.make ~category:`Bridge
             ~message:"activity context is no longer active" ())
      else
        let details = copy_payloads details in
        let callback_result =
          try context.heartbeat_fn details with
          | exception_ ->
              Error
                (Error.make ~non_retryable:true ~category:`Defect
                   ~message:
                     (Printf.sprintf "activity heartbeat callback raised: %s"
                        (Printexc.to_string exception_))
                   ())
        in
        match callback_result with
        | Ok () ->
            context.details <- copy_payloads details;
            Ok ()
        | Error _ as error -> error)

(** Returns a private copy so callers cannot mutate retained [bytes] values. *)
let details context =
  Mutex.lock context.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock context.mutex)
    (fun () -> copy_payloads context.details)

(** Timeout values are immutable and need no lock to read. *)
let heartbeat_timeout context = context.heartbeat_timeout

(** Ends the context lifetime after waiting for a callback already in flight.
    This is the use-after-completion guard for the opaque task token captured by
    [heartbeat_fn]. *)
let invalidate context =
  Mutex.lock context.mutex;
  context.active <- false;
  Mutex.unlock context.mutex
