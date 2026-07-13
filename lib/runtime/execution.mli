(** The complete incoming event delivered to a registered workflow signal
    handler. Payload order, sender identity, and headers are retained exactly
    as validated by the native activation boundary. *)
type signal = {
  input : Temporal_base.Codec.payload list;
  identity : string;
  headers : (string * Temporal_base.Codec.payload) list;
}

(** An internal signal handler owned by one workflow execution. The callback is
    invoked only by that execution's scheduler; Rust never receives an OCaml
    closure or calls one from a native thread. *)
type signal_handler

(** Creates a handler for one validated signal name. The callback receives the
    complete runtime signal and returns a typed workflow error when delivery
    should fail the execution. *)
val make_signal_handler :
  name:string ->
  dispatch:(signal -> (unit, Temporal_base.Error.t) result) ->
  signal_handler

(** Returns the stable Temporal name used to look up a handler. *)
val signal_handler_name : signal_handler -> string

(** The in-memory state for one running workflow. It contains the workflow's
    scheduler and the activities and timers whose results are still pending. *)
type ('input, 'output) t

(** Creates the in-memory state but does not call the workflow function yet.
    The function starts only after [activate] receives [Start_workflow].
    [task_queue] is captured in the execution context and is used by activity
    commands when a workflow does not supply an explicit queue. *)
val start :
  ?task_queue:string ->
  ?signal_handlers:signal_handler list ->
  ( 'input,
    'output,
    'input -> ('output, Temporal_base.Error.t) result )
  Temporal_base.Definition.t ->
  'input ->
  ('input, 'output) t

(** Applies every job in list order, runs workflow fibers until none can make
    progress, and returns newly produced commands in their creation order. An
    execution removed from the cache ignores later calls. *)
val activate : ('input, 'output) t -> Activation.job list -> Activation.command list

(** Installs the timestamp carried by the activation currently being dispatched.
    The native protocol adapter calls this before [activate] so deterministic
    workflow clock reads observe the correct replay value. *)
val set_activation_timestamp :
  ('input, 'output) t -> Temporal_protocol.Workflow_protocol.timestamp option -> unit

(** Releases paused fibers and pending operation tables for this execution.
    Idempotent. Call when removing a run from a worker registry if a terminal
    or eviction path has not already shut the execution down. *)
val shutdown : ('input, 'output) t -> unit
