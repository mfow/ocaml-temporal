(** A non-negative deterministic duration represented in whole milliseconds.
    Workflow code must use this value rather than consulting wall-clock time. *)
type t

(** [of_ms milliseconds] constructs a duration and rejects negative values
    with [Invalid_argument], because a negative duration is a configuration
    defect rather than an operational Temporal failure. *)
val of_ms : int64 -> t

(** [to_ms duration] returns the exact millisecond count without rounding. *)
val to_ms : t -> int64
