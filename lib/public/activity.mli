(** The type of an OCaml function that implements an activity. It receives the
    decoded input and returns either its output or a structured error. *)
type ('input, 'output) implementation =
  'input -> ('output, Error.t) result

(** Opaque context for one activity attempt. Calls after terminal completion
    return typed errors rather than using a released task token. *)
type context = Temporal_base.Activity_context.t

(** Context-aware implementation form for activity progress and prior details. *)
type ('input, 'output) contextual_implementation =
  context -> 'input -> ('output, Error.t) result

(** A description of an activity and the OCaml types it accepts and returns.
    The definition stores the activity's Temporal name and its input and output
    codecs. It contains either a plain local implementation, a context-aware
    implementation, or neither when it is a remote scheduling reference. *)
type ('input, 'output) t

(** Creates a definition for an activity that this OCaml worker will run. The
    name is validated immediately; it must be non-empty, valid UTF-8, NUL-free,
    and no more than 65,536 bytes because it crosses the native protocol into
    Temporal history. Violations raise [Invalid_argument] as construction
    defects. *)
val define :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  ('input, 'output) implementation ->
  ('input, 'output) t

(** Creates a local activity that receives an opaque attempt context. *)
val define_with_context :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  ('input, 'output) contextual_implementation ->
  ('input, 'output) t

(** Creates a typed reference to an activity run by another worker. The name
    has the same non-empty, valid UTF-8, NUL-free, 65,536-byte contract as
    [define]. The name and codecs must agree with that worker's activity
    definition; the returned value has no executable callback and must not be
    registered as local code. *)
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

(** Returns the context-aware callback, if present. *)
val implementation_with_context :
  ('input, 'output) t ->
  ('input, 'output) contextual_implementation option

(** Encodes and submits one typed heartbeat value for the current activity. The
    payload is copied before crossing into the private runtime, and stale or
    unavailable contexts return typed errors rather than retaining a released
    task token. *)
val heartbeat : context -> 'a Codec.t -> 'a -> (unit, Error.t) result

(** Operations available to a contextual activity attempt. *)
module Context : sig
  (** The attempt-scoped context passed to contextual activity helpers. *)
  type t = context

  (** Sends one typed heartbeat value and returns a typed error if this attempt
      is no longer active. *)
  val heartbeat : t -> 'a Codec.t -> 'a -> (unit, Error.t) result

  (** Sends already encoded detail payloads in order. *)
  val heartbeat_payloads : t -> Payload.t list -> (unit, Error.t) result

  (** Returns a copied list of details from the preceding heartbeat attempt. *)
  val details : t -> Payload.t list

  (** Returns the configured heartbeat interval, if one was supplied. *)
  val heartbeat_timeout : t -> Duration.t option
end

(** The cancellation policy sent with an activity command. [Try_cancel] asks
    the activity worker to stop when possible, [Wait_cancellation_completed]
    waits for acknowledgement, and [Abandon] leaves the activity running. The
    policy affects the parent future and is deterministic during replay. *)
type cancellation_type =
  | Try_cancel
  | Wait_cancellation_completed
  | Abandon

(** A deterministic retry policy for one scheduled activity.

    [initial_interval] and [maximum_interval] are positive whole-millisecond
    durations, with the maximum at least as large as the initial delay.
    [backoff_coefficient] must be finite and at least [1.0].  A
    [maximum_attempts] value of [0] means that Temporal imposes no attempt
    count limit; positive values include the initial attempt.  The constructor
    returns a typed defect instead of raising so callers can validate policy
    configuration while assembling a workflow definition. *)
module Retry_policy : sig
  (** Opaque immutable retry policy validated before command construction. *)
  type t

  (** Validates and constructs an immutable retry policy. *)
  val make :
    initial_interval:Duration.t ->
    backoff_coefficient:float ->
    maximum_interval:Duration.t ->
    maximum_attempts:int ->
    ?non_retryable_error_types:string list ->
    unit ->
    (t, Error.t) result

  (** Alias for [make] for callers that prefer constructor terminology. *)
  val create :
    initial_interval:Duration.t ->
    backoff_coefficient:float ->
    maximum_interval:Duration.t ->
    maximum_attempts:int ->
    ?non_retryable_error_types:string list ->
    unit ->
    (t, Error.t) result

  (** Returns the exact initial retry delay. *)
  val initial_interval : t -> Duration.t

  (** Returns the finite multiplier applied between retry attempts. *)
  val backoff_coefficient : t -> float

  (** Returns the cap applied to the retry delay. *)
  val maximum_interval : t -> Duration.t

  (** Returns the maximum number of attempts; [0] means unlimited. *)
  val maximum_attempts : t -> int

  (** Returns the immutable list of Temporal error type names that must not be
      retried. *)
  val non_retryable_error_types : t -> string list
end

(** Schedules the activity and returns immediately with a future for its
    result. Start several independent activities before awaiting them to let
    Temporal run them concurrently. Optional labels make the activity command
    explicit; omitted IDs are deterministic, omitted queues use the worker's
    queue, and omitted timeouts use a 60-second start-to-close timeout because
    Temporal requires at least one activity timeout. If input or option
    validation fails, the returned future contains a typed error and no command
    is emitted. [do_not_eagerly_execute] controls whether Core may run the
    activity inline with the scheduling activation; it defaults to [false]. *)
val start :
  ?activity_id:string ->
  ?task_queue:string ->
  ?schedule_to_close_timeout:Duration.t ->
  ?schedule_to_start_timeout:Duration.t ->
  ?start_to_close_timeout:Duration.t ->
  ?heartbeat_timeout:Duration.t ->
  ?retry_policy:Retry_policy.t ->
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
  ?retry_policy:Retry_policy.t ->
  ?cancellation_type:cancellation_type ->
  ?do_not_eagerly_execute:bool ->
  ('input, 'output) t -> 'input -> ('output, Error.t) result
