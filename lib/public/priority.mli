(** Scheduling priority shared by activity and child-workflow commands.

    Temporal uses a lower positive [priority_key] as a stronger priority.
    [fairness_key] groups work for fair matching and [fairness_weight] controls
    the group's relative share.  The constructor performs all validation before
    a value can be retained by a workflow command. *)
type t = private {
  priority_key : int;
  fairness_key : string;
  fairness_weight : float;
}

(** Creates a validated immutable priority. [priority_key] must fit Core's
    signed 32-bit field, [fairness_key] is at most 64 UTF-8 bytes, and
    [fairness_weight] must be finite and strictly positive. *)
val make :
  priority_key:int ->
  fairness_key:string ->
  fairness_weight:float ->
  (t, Error.t) result

(** Returns the key used by Temporal's matching priority queue. *)
val priority_key : t -> int

(** Returns a detached copy of the fairness group key. *)
val fairness_key : t -> string

(** Returns the positive fairness weight. *)
val fairness_weight : t -> float

(** Returns the exact IEEE-754 single-precision bits sent to Temporal Core. *)
val fairness_weight_bits : t -> int64
