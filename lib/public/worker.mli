(** Worker registration and execution for OCaml workflow and activity code.

    Definitions are packed existentially only at the registration boundary;
    workflow bodies and activity bodies remain ordinary typed OCaml functions. *)

(** A heterogeneous workflow registration item. The existential package keeps
    each definition's input and output codecs paired with its implementation. *)
type registered_workflow

(** Packs a typed workflow definition for a worker registration list. *)
val workflow : ('input, 'output) Workflow.t -> registered_workflow

(** A heterogeneous activity registration item. *)
type registered_activity

(** Packs a typed activity definition for a worker registration list. *)
val activity : ('input, 'output) Activity.t -> registered_activity

(** An opaque worker instance owning one supervisor/backend graph and two
    deterministic registration maps. *)
type t

(** Creates and validates a worker. Duplicate names and remote-only definitions
    return typed defects before any backend graph is allocated. *)
val create :
  ?identity:string ->
  target_url:string ->
  namespace:string ->
  task_queue:string ->
  workflows:registered_workflow list ->
  activities:registered_activity list ->
  unit ->
  (t, Error.t) result

(** Runs the workflow and activity poll loops until the backend reports
    shutdown. Each accepted task is decoded, dispatched to its registered
    OCaml function, encoded, and completed before the next task is admitted. *)
val run : t -> (unit, Error.t) result

(** Initiates graceful worker shutdown. Repeated calls are safe and idempotent. *)
val shutdown : t -> (unit, Error.t) result
