(** Workflow-local waits for replay-safe OCaml state.

    A condition does not contact Temporal and does not add a command or history
    event.  The predicate is checked immediately and again after the owning
    activation has run its queued workflow work.  A false predicate suspends
    only the current workflow fiber; it never blocks an OS thread. *)

(** A predicate that can report an expected, typed failure while deciding
    whether the condition is satisfied.  It must be deterministic, quick, and
    non-suspending: use activities or ordinary futures for external work. *)
type predicate = unit -> (bool, Error.t) result

(** Waits until a pure boolean predicate is true.  Exceptions raised by the
    predicate are caught and returned as non-retryable [`Defect] errors. *)
val wait_until : (unit -> bool) -> (unit, Error.t) result

(** Result-aware form of [wait_until].  [Error error] is returned unchanged;
    an exception is converted to a typed non-retryable defect. *)
val wait_until_result : predicate -> (unit, Error.t) result
