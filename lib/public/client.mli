(** Client operations for starting workflows and awaiting an exact run.

    The client is deliberately smaller than a worker: it owns a connection
    backend and typed handles, but never registers or executes workflow code. *)

(** An opaque client connection owned by the caller. *)
type t

(** A typed identity for one started workflow execution. The input parameter
    documents the value used at start; the output parameter controls decoding
    of the terminal payload. *)
type ('input, 'output) handle

(** Terminal outcomes are values so workflow failures do not become control
    flow exceptions. The outer [result] of [wait] is reserved for bridge or
    payload transport errors. *)
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

(** Connects to the configured Temporal endpoint. [target_url] is copied into
    the private backend graph and [namespace] is required for every operation. *)
val create :
  ?identity:string ->
  target_url:string ->
  namespace:string ->
  unit ->
  (t, Error.t) result

(** Starts a typed workflow execution and returns the exact server run handle.
    Encoding happens before the backend receives the request, so codec errors
    cannot create a partial workflow history entry. *)
val start :
  t ->
  workflow:('input, 'output) Workflow.t ->
  task_queue:string ->
  id:string ->
  input:'input ->
  (('input, 'output) handle, Error.t) result

(** Waits for the exact workflow ID and run ID returned by [start]. A
    continued-as-new result is returned as a value rather than followed
    implicitly, preserving the caller's run identity choice. *)
val wait :
  ('input, 'output) handle ->
  ('output terminal_result, Error.t) result

(** Returns the durable workflow ID supplied to [start]. *)
val workflow_id : ('input, 'output) handle -> string

(** Returns the server-issued run ID supplied to [start]. *)
val run_id : ('input, 'output) handle -> string

(** Shuts down the client graph. Repeated calls are idempotent and return the
    same successful result after resources have been released. *)
val shutdown : t -> (unit, Error.t) result
