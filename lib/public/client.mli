(** Client operations for starting workflows and awaiting an exact run.

    The client is deliberately smaller than a worker: it owns a connection
    backend and typed handles, but never registers or executes workflow code. *)

(** An opaque client connection owned by the caller. *)
type t

(** A typed identity for one started workflow execution. The input parameter
    documents the value used at start; the output parameter controls decoding
    of the terminal payload. *)
type ('input, 'output) handle

(** An exact workflow/run identity returned when a workflow continues as new.
    It contains no codec or client ownership; use [follow] with the original
    client and typed workflow definition to construct a handle for this run.
    The namespace is retained so a continuation cannot accidentally be used
    with a client connected to a different Temporal namespace. *)
type execution = {
  (* Namespace that owns the successor execution. *)
  namespace : string;
  (* Durable workflow identity shared by the original and successor runs. *)
  workflow_id : string;
  (* Server-issued identity of the successor run. *)
  run_id : string;
}

(** Terminal outcomes are values so workflow failures do not become control
    flow exceptions. The outer [result] of [wait] is reserved for bridge or
    payload transport errors. *)
type 'output terminal_result =
  (* The terminal payload decoded using the workflow definition's output codec. *)
  | Completed of 'output
  (* Temporal reported a workflow failure as a terminal value. *)
  | Failed of Error.t
  (* The exact run reached the cancellation state. *)
  | Cancelled of Error.t
  (* The exact run was terminated by an operator or another client. *)
  | Terminated of Error.t
  (* The exact run reached a Temporal timeout state. *)
  | Timed_out of Error.t
  (* The run continued as new; the caller decides whether to wait on the
     returned successor identity. *)
  | Continued_as_new of execution

(** One execution row returned by the Temporal visibility service. *)
type visibility_execution = {
  workflow_id : string;
  run_id : string;
  workflow_type : string;
  task_queue : string;
  status : string;
}

(** A bounded visibility page and its opaque continuation token. *)
type visibility_page = {
  executions : visibility_execution list;
  next_page_token : string option;
}

(** Connects to the configured Temporal endpoint. [target_url] is copied into
    the private backend graph and [namespace] is required for every operation.
    The namespace and optional identity must be non-empty, NUL-free, and no
    more than 65,536 bytes; invalid configuration is returned as a typed
    defect. *)
val create :
  ?identity:string ->
  target_url:string ->
  namespace:string ->
  unit ->
  (t, Error.t) result

(** Starts a typed workflow execution and returns the exact server run handle.
    Encoding happens before the backend receives the request, so codec errors
    cannot create a partial workflow history entry.

    [task_queue], [id], the workflow type name retained by [workflow], and an
    explicitly supplied [request_id] must be non-empty, NUL-free, and no more
    than 65,536 bytes. Invalid fields return a typed defect before transport
    selection, so the deterministic mock and the native JSON bridge enforce
    the same request boundary.

    [request_id] is an optional caller-owned Temporal idempotency key. When a
    start result is uncertain, retry the same logical start with the same
    [request_id] so Temporal can deduplicate an already accepted request. If
    omitted, the SDK allocates a fresh request ID for this call. Do not reuse
    one ID for unrelated workflow starts. *)
val start :
  t ->
  ?request_id:string ->
  workflow:('input, 'output) Workflow.t ->
  task_queue:string ->
  id:string ->
  input:'input ->
  unit ->
  (('input, 'output) handle, Error.t) result

(** Rebuilds a typed exact-run handle for a continuation returned by [wait].
    This does not start a workflow or follow a run implicitly: it only combines
    the caller's existing client, the supplied workflow definition's codecs,
    and the successor identity. The continuation namespace must equal the
    namespace used to create [client]. All identity fields must be non-empty,
    NUL-free, and no more than 65,536 bytes; malformed or cross-namespace
    values are returned as typed defects before any backend operation. *)
val follow :
  t ->
  workflow:('input, 'output) Workflow.t ->
  execution ->
  (('input, 'output) handle, Error.t) result

(** Waits for the exact workflow ID and run ID returned by [start]. A
    continued-as-new result is returned as a value rather than followed
    implicitly, preserving the caller's run identity choice. *)
val wait :
  ('input, 'output) handle ->
  ('output terminal_result, Error.t) result

(** Requests cancellation of the exact run retained by [handle]. A successful
    call acknowledges Temporal's cancellation RPC; it does not wait for the
    workflow to stop. Call [wait handle] to observe [Cancelled]. [request_id]
    is the idempotency key for this logical control operation and should be
    supplied again if the caller retries after an uncertain transport error.
    Both [request_id] and [reason] are limited to 65,536 bytes and may not
    contain NUL; [reason] may be empty. *)
val cancel :
  ?request_id:string ->
  ?reason:string ->
  ('input, 'output) handle ->
  (unit, Error.t) result

(** Sends one typed signal to the exact run retained by [handle]. A successful
    call acknowledges Temporal's signal RPC; it does not wait for workflow code
    to process the message. [request_id] is optional: when omitted, the SDK
    allocates a fresh process-wide ID shared by all client handles. Supply the
    same ID when retrying an uncertain transport result. Signal names are
    validated when their definitions are created and input is encoded before
    transport. *)
val signal :
  ?request_id:string ->
  ('workflow_input, 'workflow_output) handle ->
  signal:'signal Signal.t ->
  input:'signal ->
  (unit, Error.t) result

(** Executes an output-only query against the exact run retained by [handle].
    The query handler receives no input in this first client slice. A
    successful result is decoded with [query]'s output codec; routine Temporal
    query failures and codec failures are returned as typed [Error.t] values. *)
val query :
  ('workflow_input, 'workflow_output) handle ->
  query:'query Query.t ->
  ('query, Error.t) result

(** Lists one bounded page of workflow executions using Temporal's visibility
    query language. [page_token] is opaque and may be passed unchanged to a
    later call. Invalid query metadata is returned as a typed defect. *)
val list_visibility :
  ?page_size:int ->
  ?page_token:string ->
  t ->
  query:string ->
  unit ->
  (visibility_page, Error.t) result

(** Returns the durable workflow ID supplied to [start]. *)
val workflow_id : ('input, 'output) handle -> string

(** Returns the server-issued run ID supplied to [start]. *)
val run_id : ('input, 'output) handle -> string

(** Shuts down the client graph. Repeated calls are idempotent and return the
    same cached result, including a terminal teardown error, after the first
    shutdown request has consumed or invalidated the backend resources. *)
val shutdown : t -> (unit, Error.t) result
