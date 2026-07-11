type t = Temporal_base.Duration.t

val of_ms : int64 -> t
(** Construct a nonnegative millisecond duration. *)

val to_ms : t -> int64
