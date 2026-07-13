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

(** Records the timestamp attached to the activation that is about to run user
    workflow code. [None] is used for synthetic runtime activations, such as
    cache eviction, where no workflow clock is available. *)
val set_activation_timestamp :
  t -> Temporal_protocol.Workflow_protocol.timestamp option -> unit

(** Returns the timestamp captured for the current activation, if one exists.
    The public [Temporal.Workflow.now] wrapper converts it to its abstract
    deterministic time type. *)
val activation_timestamp :
  t -> Temporal_protocol.Workflow_protocol.timestamp option

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
    produces a complete schedule-activity command, and returns the decoded
    output future together with an owner-checked cancellation operation.
    [activity_id] and [task_queue] default deterministically; when all timeout
    options are absent, a 60-second start-to-close timeout is used because
    Temporal requires at least one activity timeout. The cancellation operation
    emits at most one [Request_cancel_activity] command for its sequence and
    remains a valid no-op after a terminal result or activity-start failure.
    Calls made without this context current are typed lifecycle defects. *)
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
  ( ('output, Temporal_base.Error.t) Future_store.t
  * (unit -> (unit, Temporal_base.Error.t) result) )

(** Assigns a private correlation sequence, records how to decode the child
    result, emits a command containing the application-supplied durable [id]
    and optional Core-owned retry policy, and returns the child result future
    together with a cancellation operation.
    The default policy is [Child_try_cancel], so cancellation requests the
    child unless a caller explicitly chooses [Child_abandon]. The operation is
    valid only while this context is current; it emits at most one cancellation
    command and leaves Core to resolve the future. A repeated valid call is a
    no-op, including after Core has delivered a terminal result or start
    failure; a call after context shutdown remains a typed lifecycle defect. *)
val start_child_workflow :
  t ->
  id:string ->
  name:string ->
  input:Temporal_base.Codec.payload ->
  ?retry_policy:Activation.retry_policy ->
  ?cancellation_type:Activation.child_workflow_cancellation_type ->
  decode:(Temporal_base.Codec.payload -> ('output, Temporal_base.Error.t) result) ->
  unit ->
  ( ('output, Temporal_base.Error.t) Future_store.t
  * (reason:string -> (unit, Temporal_base.Error.t) result) )

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

(** Buffers a terminal command and stops the current workflow fiber. This is a
    package-private control boundary used by terminal workflow operations. *)
val terminate : t -> Activation.command -> 'value

(** Buffers a continue-as-new command and stops the current workflow fiber.
    The successor input is encoded before it reaches this private operation. *)
val continue_as_new :
  t -> workflow_type:string -> input:Temporal_base.Codec.payload -> 'value

(** True when the pending command buffer already holds a terminal workflow
    command such as complete, fail, cancel, or continue-as-new. *)
val has_buffered_terminal : t -> bool

(** Returns buffered commands in emission order and atomically clears them. *)
val take_commands : t -> Activation.command list

(** Closes the scheduler and removes all saved activity, child workflow, and
    timer callbacks. Calling it more than once is safe. *)
val shutdown : t -> unit
