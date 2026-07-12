(** The activities, timers, and commands belonging to one workflow execution.
    The runtime temporarily makes this context current while running that
    workflow's OCaml code. *)
type t

(** Creates an empty context whose futures use [scheduler]. [task_queue] is the
    deterministic default used by activities that do not override their queue;
    a worker supplies its own queue when it creates an execution. *)
val create : ?task_queue:string -> Scheduler.t -> t

(** Checks a worker's implicit activity queue without allocating a context.
    [Ok ()] means that the byte string is non-empty, contains no NUL byte, is
    at most 65,536 bytes, and is valid UTF-8. The worker adapter uses this
    result to reject invalid configuration before accepting a worker; direct
    [create] callers still receive [Invalid_argument] for the same defect. *)
val validate_task_queue : string -> (unit, string) result

(** Returns the context installed on the current OCaml Domain, if any. *)
val current : unit -> t option

(** Runs [action] with [t] dynamically installed and restores the previous
    context even if [action] raises. Nested calls are supported. *)
val with_context : t -> (unit -> 'value) -> 'value

(** Runs [action] with no workflow context installed and restores the previous
    context afterward. Infrastructure callbacks that must not re-enter
    deterministic workflow state use this boundary. *)
val without_context : (unit -> 'value) -> 'value

(** Creates an already-resolved future owned by the context scheduler. *)
val resolved :
  t ->
  ('value, Temporal_base.Error.t) result ->
  ('value, Temporal_base.Error.t) Future_store.t

(** Creates a pending, scheduler-owned notification future and its resolver.
    This is an internal coordination primitive for deterministic workflow
    helpers such as cancellation scopes; it emits no Temporal command and is
    closed automatically with the workflow context. *)
val create_signal :
  t ->
  (unit, Temporal_base.Error.t) Future_store.t
  * (unit, Temporal_base.Error.t) Future_store.resolver

(** Creates a detached failed future for an operation attempted without an
    active workflow context. *)
val detached_error :
  message:string -> ('value, Temporal_base.Error.t) Future_store.t

(** Assigns a sequence number, records how to decode the eventual result,
    produces a complete schedule-activity command, and returns a future for the
    decoded output. [activity_id] and [task_queue] default deterministically;
    when all timeout options are absent, a 60-second start-to-close timeout is
    used because Temporal requires at least one activity timeout. *)
val schedule_activity :
  t ->
  name:string ->
  input:Temporal_base.Codec.payload ->
  ?activity_id:string ->
  ?task_queue:string ->
  ?schedule_to_close_timeout:int64 ->
  ?schedule_to_start_timeout:int64 ->
  ?start_to_close_timeout:int64 ->
  ?heartbeat_timeout:int64 ->
  ?retry_policy:Activation.retry_policy ->
  ?cancellation_type:Activation.activity_cancellation_type ->
  ?do_not_eagerly_execute:bool ->
  decode:(Temporal_base.Codec.payload -> ('output, Temporal_base.Error.t) result) ->
  unit ->
  ('output, Temporal_base.Error.t) Future_store.t

(** Assigns a private correlation sequence, records how to decode the child
    result, emits a command containing the application-supplied durable [id],
    and returns the child result future. *)
val start_child_workflow :
  t ->
  id:string ->
  name:string ->
  input:Temporal_base.Codec.payload ->
  decode:(Temporal_base.Codec.payload -> ('output, Temporal_base.Error.t) result) ->
  ('output, Temporal_base.Error.t) Future_store.t

(** Emits a durable timer command and returns a future resolved by [fire_timer].
    The duration is an exact non-negative millisecond count. *)
val start_timer : t -> int64 -> (unit, Temporal_base.Error.t) Future_store.t

(** Supplies an activity result to the pending future with this sequence number
    and then removes it. An unknown or repeated number returns a non-retryable
    bridge error because Core and the OCaml runtime disagree about state. *)
val resolve_activity :
  t ->
  seq:int64 ->
  (Temporal_base.Codec.payload, Temporal_base.Error.t) result ->
  (unit, Temporal_base.Error.t) result

(** Completes and removes the pending child workflow with this private
    correlation sequence. Unknown and repeated numbers are bridge defects. *)
val resolve_child_workflow :
  t ->
  seq:int64 ->
  (Temporal_base.Codec.payload, Temporal_base.Error.t) result ->
  (unit, Temporal_base.Error.t) result

(** Records a child start acknowledgment. [Ok run_id] advances the pending
    child without resolving its result future; [Error] retires the child and
    resolves that future with the typed start failure. *)
val resolve_child_workflow_start :
  t ->
  seq:int64 ->
  (string, Temporal_base.Error.t) result ->
  (unit, Temporal_base.Error.t) result

(** Completes and removes the pending timer with this sequence number. *)
val fire_timer : t -> seq:int64 -> (unit, Temporal_base.Error.t) result

(** Appends a command to the current activation output buffer. *)
val emit : t -> Activation.command -> unit

(** Returns buffered commands in emission order and atomically clears them. *)
val take_commands : t -> Activation.command list

(** Closes the scheduler and removes all saved activity, child workflow, and
    timer callbacks. Calling it more than once is safe. *)
val shutdown : t -> unit
