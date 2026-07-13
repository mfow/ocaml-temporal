(** The in-memory state for one running workflow. It contains the workflow's
    scheduler and the activities and timers whose results are still pending. *)
type ('input, 'output) t

(** Creates the in-memory state but does not call the workflow function yet.
    The function starts only after [activate] receives [Start_workflow].
    [task_queue] is captured in the execution context and is used by activity
    commands when a workflow does not supply an explicit queue. *)
val start :
  ?task_queue:string ->
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
