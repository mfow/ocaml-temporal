(** Controls how Core reports a parent operation after cancellation has been
    requested. [Abandon] reports immediately without asking the child worker;
    [Try_cancel] requests cancellation and reports immediately;
    [Wait_cancellation_completed] waits for child cancellation to complete; and
    [Wait_cancellation_requested] waits until Core confirms the request. *)
type cancellation_type =
  | Try_cancel
  | Wait_cancellation_completed
  | Abandon
  | Wait_cancellation_requested

(** An opaque typed child operation. The handle owns the result future and can
    emit one deterministic cancellation command for that exact child. *)
type 'output handle

(** Starts a child workflow and retains a handle that can cancel it. The
    command is emitted immediately; [future] remains pending until Core reports
    a start or terminal result. The default [Try_cancel] policy asks Core to
    cancel the child when [cancel] is called; choose [Abandon] explicitly when
    the child should keep running. Invalid input and detached calls return
    ready failed handles, so no command or sequence number is created. *)
val start_handle :
  ?cancellation_type:cancellation_type ->
  id:string ->
  ('input, 'output) Workflow.t ->
  'input ->
  'output handle

(** Returns the typed future owned by a child operation handle. *)
val future : 'output handle -> ('output, Error.t) Future.t

(** Requests cancellation of the exact child represented by [handle]. The
    default reason is stable and replay-safe. Repeated calls are idempotent;
    this includes a valid call that arrives after natural child completion or a
    start failure has retired the pending entry. Core determines when the
    future receives the typed cancellation result. *)
val cancel :
  ?reason:string ->
  'output handle ->
  (unit, Error.t) result

(** Starts [definition] as a durable child workflow and returns immediately.
    [id] is the child execution's application-owned durable identity. It must
    be non-empty, valid UTF-8, and no more than 65,536 UTF-8 bytes so it can
    cross the strict bridge boundary. Invalid identity or input encoding returns
    a ready failed future without emitting a command or consuming a sequence.
    Starting several operations before awaiting them lets Temporal run them
    concurrently. The default cancellation policy is [Try_cancel]. *)
val start :
  ?cancellation_type:cancellation_type ->
  id:string ->
  ('input, 'output) Workflow.t ->
  'input ->
  ('output, Error.t) Future.t

(** Starts a child workflow and waits for its result. This is equivalent to
    [Future.await (start ~id definition input)]. Child, codec, cancellation,
    and bridge failures are returned as structured values. The default
    cancellation policy is [Try_cancel]. *)
val execute :
  ?cancellation_type:cancellation_type ->
  id:string ->
  ('input, 'output) Workflow.t ->
  'input ->
  ('output, Error.t) result
