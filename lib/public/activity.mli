(** The type of an OCaml function that implements an activity. It receives the
    decoded input and returns either its output or a structured error. *)
type ('input, 'output) implementation =
  'input -> ('output, Error.t) result

(** A description of an activity and the OCaml types it accepts and returns.
    The definition stores the activity's Temporal name and its input and output
    codecs. It may also contain a local implementation. *)
type ('input, 'output) t

(** Creates a definition for an activity that this OCaml worker will run. *)
val define :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  ('input, 'output) implementation ->
  ('input, 'output) t

(** Creates a typed reference to an activity run by another worker. The name
    and codecs must agree with that worker's activity definition. *)
val remote :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  ('input, 'output) t

(** Returns the activity type name sent to Temporal when it is scheduled. *)
val name : ('input, 'output) t -> string

(** Returns the codec used to decode activity inputs while keeping the activity
    definition itself opaque. *)
val input : ('input, 'output) t -> 'input Codec.t

(** Returns the codec used to encode successful activity outputs. *)
val output : ('input, 'output) t -> 'output Codec.t

(** Returns executable code for a local definition, or [None] for a remote
    reference. *)
val implementation :
  ('input, 'output) t -> ('input, 'output) implementation option

(** The cancellation policy sent with an activity command. [Try_cancel] asks
    the activity worker to stop when possible, [Wait_cancellation_completed]
    waits for acknowledgement, and [Abandon] leaves the activity running. *)
type cancellation_type =
  | Try_cancel
  | Wait_cancellation_completed
  | Abandon

(** Schedules the activity and returns immediately with a future for its
    result. Start several independent activities before awaiting them to let
    Temporal run them concurrently. Optional labels make the activity command
    explicit; omitted IDs are deterministic, omitted queues use the worker's
    queue, and omitted timeouts use a 60-second start-to-close timeout because
    Temporal requires at least one activity timeout. If input or option
    validation fails, the returned future contains a typed error and no command
    is emitted. *)
val start :
  ?activity_id:string ->
  ?task_queue:string ->
  ?schedule_to_close_timeout:Duration.t ->
  ?schedule_to_start_timeout:Duration.t ->
  ?start_to_close_timeout:Duration.t ->
  ?heartbeat_timeout:Duration.t ->
  ?cancellation_type:cancellation_type ->
  ?do_not_eagerly_execute:bool ->
  ('input, 'output) t ->
  'input ->
  ('output, Error.t) Future.t

(** Schedules the activity and waits for its result. This is equivalent to
    calling [start] followed by [Future.await]. *)
val execute :
  ?activity_id:string ->
  ?task_queue:string ->
  ?schedule_to_close_timeout:Duration.t ->
  ?schedule_to_start_timeout:Duration.t ->
  ?start_to_close_timeout:Duration.t ->
  ?heartbeat_timeout:Duration.t ->
  ?cancellation_type:cancellation_type ->
  ?do_not_eagerly_execute:bool ->
  ('input, 'output) t -> 'input -> ('output, Error.t) result
