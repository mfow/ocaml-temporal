(** Stores whole milliseconds in the public API without exposing the private
    base library's type path. The value is validated at construction and is
    converted to an integer only when a command is assembled. *)
type t = int64

let of_ms milliseconds =
  if Int64.compare milliseconds 0L < 0 then
    invalid_arg "Temporal duration cannot be negative";
  milliseconds

let to_ms duration = duration
