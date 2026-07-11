(** The activities, timers, and commands belonging to one workflow execution.
    The runtime temporarily makes this context current while running that
    workflow's OCaml code. *)
type t

(** Creates an empty context whose futures use [scheduler]. *)
val create : Scheduler.t -> t

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

(** Creates a detached failed future for an operation attempted without an
    active workflow context. *)
val detached_error :
  message:string -> ('value, Temporal_base.Error.t) Future_store.t

(** Assigns a sequence number, records how to decode the eventual result,
    produces a schedule-activity command, and returns a future for the decoded
    output. *)
val schedule_activity :
  t ->
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

(** Completes and removes the pending timer with this sequence number. *)
val fire_timer : t -> seq:int64 -> (unit, Temporal_base.Error.t) result

(** Appends a command to the current activation output buffer. *)
val emit : t -> Activation.command -> unit

(** Returns buffered commands in emission order and atomically clears them. *)
val take_commands : t -> Activation.command list

(** Closes the scheduler and removes all saved activity and timer callbacks.
    Calling it more than once is safe. *)
val shutdown : t -> unit
