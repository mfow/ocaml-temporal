(** Immutable wire-level scheduling metadata.  Validation belongs to the
    public API; this small module only keeps the exact representation shared by
    OCaml runtime layers. *)
type t = {
  priority_key : int32;
  fairness_key : string;
  fairness_weight_bits : int32;
}

(** Constructs a value after the public boundary has checked its invariants. *)
let make ~priority_key ~fairness_key ~fairness_weight_bits =
  { priority_key; fairness_key; fairness_weight_bits }

(** Returns the wire priority key. *)
let priority_key value = value.priority_key

(** Returns the wire fairness key. *)
let fairness_key value = value.fairness_key

(** Returns the exact wire fairness-weight bits. *)
let fairness_weight_bits value = value.fairness_weight_bits
