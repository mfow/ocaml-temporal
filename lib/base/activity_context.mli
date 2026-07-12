(** Opaque execution context supplied to one local activity attempt.

    The context deliberately lives in the private base library. The public
    [Temporal.Activity.Context] module exposes only safe operations; native
    task tokens and supervisor handles never become OCaml values. *)
type t

(** Creates an active context after the adapter has validated all values
    received from Temporal Core. The callback receives owned payload copies. *)
val create :
  heartbeat:(Payload.t list -> (unit, Error.t) result) ->
  details:Payload.t list ->
  heartbeat_timeout:Duration.t option ->
  t

(** Creates a context for a backend that cannot submit native heartbeats. *)
val unavailable :
  details:Payload.t list -> heartbeat_timeout:Duration.t option -> t

(** Submits copied heartbeat details while the attempt is active. *)
val heartbeat : t -> Payload.t list -> (unit, Error.t) result

(** Returns copied details from the preceding heartbeat attempt. *)
val details : t -> Payload.t list

(** Returns the server-supplied heartbeat interval for this attempt. *)
val heartbeat_timeout : t -> Duration.t option

(** Invalidates the context after terminal completion or failure. The call
    waits for a callback already in flight before later calls fail. *)
val invalidate : t -> unit
