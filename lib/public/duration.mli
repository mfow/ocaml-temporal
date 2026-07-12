(** A non-negative length of time represented in whole milliseconds. Workflow
    timers use this type so their requested duration is recorded exactly and
    can be reproduced during replay. *)
type t

(** Creates a duration from milliseconds. A negative value raises
    [Invalid_argument] because it is a programming error. *)
val of_ms : int64 -> t

(** Returns the exact number of milliseconds supplied to [of_ms]. *)
val to_ms : t -> int64
