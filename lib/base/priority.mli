(** Scheduling metadata shared by workflow commands and activity contexts.

    The record is intentionally opaque: callers construct it through the
    public [Temporal.Activity.Priority] validator, while the runtime and
    protocol layers can carry the exact integer and IEEE-754 single-precision
    bit values without converting through a locale-dependent float printer. *)
type t

(** Builds an already validated priority value from its wire fields. *)
val make :
  priority_key:int32 -> fairness_key:string -> fairness_weight_bits:int32 -> t

(** Returns the Core priority key; zero means that the server chooses the
    default or inherited priority. *)
val priority_key : t -> int32

(** Returns the fairness group key, or the empty string for the inherited
    server default. *)
val fairness_key : t -> string

(** Returns the exact IEEE-754 single-precision bits for the fairness weight. *)
val fairness_weight_bits : t -> int32
