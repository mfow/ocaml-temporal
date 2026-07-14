(** Scheduler-owned state for workflow-local condition waits.

    A condition is a replay-safe predicate over OCaml workflow state.  It does
    not create a Temporal command or history event: the store only retains a
    one-shot continuation until the owning activation loop asks it to check
    the predicate again. *)

(** The callback returns [Ok true] when the wait is satisfied, [Ok false] when
    it must remain pending, or a typed error when evaluating it is not safe. *)
type predicate = unit -> (bool, Temporal_base.Error.t) result

(** The private state for one workflow execution's condition waiters. *)
type t

(** Creates an empty store attached to one workflow scheduler. *)
val create : Scheduler.t -> t

(** Evaluates [predicate] immediately and, when false, suspends the current
    workflow fiber until a later [notify] observes that it has become true.
    Predicate exceptions are converted to non-retryable defects.  The call
    must run on the owning scheduler Domain; it never blocks an OS thread. *)
val wait_until : t -> predicate:predicate -> (unit, Temporal_base.Error.t) result

(** Re-evaluates all registered predicates in registration order.  A satisfied
    or failed waiter is removed before its continuation is queued, so each
    registration can wake at most once.  The result is [true] when at least one
    continuation was queued and [false] otherwise. *)
val notify : t -> bool

(** Retires every waiter and releases predicate closures.  Later notifications
    and registrations are ignored or return a typed lifecycle defect. *)
val shutdown : t -> unit
