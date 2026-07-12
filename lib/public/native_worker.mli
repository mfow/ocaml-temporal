(** Private production worker wiring.

    This module is kept behind the public library's [private_modules] boundary.
    It connects the typed workflow and activity execution adapters to the one
    owner-Domain supervisor, while [Temporal.Worker] remains responsible for
    the user-facing registration API. *)

(** Heterogeneous executable workflow registrations accepted by the private
    native adapter. The constructor is intentionally hidden. *)
type workflow_registration

(** Heterogeneous executable activity registrations accepted by the private
    native adapter. The constructor is intentionally hidden. *)
type activity_registration

(** Packs one typed workflow definition without exposing the existential
    constructor used by the runtime adapter. *)
val register_workflow :
  ('input, 'output,
   'input -> ('output, Temporal_base.Error.t) result)
  Temporal_base.Definition.t ->
  workflow_registration

(** Packs one typed activity definition without exposing the existential
    constructor used by the runtime adapter. *)
val register_activity :
  ('input, 'output,
   'input -> ('output, Temporal_base.Error.t) result)
  Temporal_base.Definition.t ->
  activity_registration

(** An opaque native worker containing the supervisor and both typed adapters.
    No Rust pointer, task token, or protocol buffer is exposed. *)
type t

(** Creates a real Temporal worker for an HTTP(S) endpoint.

    Registration validation happens before the native graph is published. If
    connection or worker startup fails after a graph exists, the graph is
    synchronously shut down before the typed error is returned. *)
val create :
  target_url:string ->
  namespace:string ->
  identity:string ->
  task_queue:string ->
  workflows:workflow_registration list ->
  activities:activity_registration list ->
  unit ->
  (t, Temporal_base.Error.t) result

(** Polls and executes both native workflow and activity lanes until [shutdown]
    is requested. A successful task-level failure is acknowledged by Temporal
    and does not terminate this loop. *)
val run : t -> (unit, Temporal_base.Error.t) result

(** Requests stop, waits for an active run loop to leave the adapter, and then
    releases the supervisor's worker, client, and Rust runtime graph exactly
    once. Repeated calls are idempotent. *)
val shutdown : t -> (unit, Temporal_base.Error.t) result
