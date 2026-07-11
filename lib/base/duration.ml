(** Stores whole milliseconds directly. This avoids rounding differences when
    a workflow is replayed on another machine. *)
type t = int64

(** Rejects negative timer lengths at construction time. *)
let of_ms milliseconds =
  if Int64.compare milliseconds 0L < 0 then
    invalid_arg "Temporal duration cannot be negative";
  milliseconds

(** Returns the stored millisecond value for command generation. *)
let to_ms duration = duration
