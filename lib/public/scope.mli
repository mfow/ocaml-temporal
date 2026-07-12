(** Workflow-local structured cancellation boundaries.

    A scope provides a deterministic cancellation signal for code that waits
    on public futures. It is deliberately cooperative: cancelling a scope
    changes the result returned by [await], but does not emit an activity or
    child-workflow cancellation command. Pending Temporal operations are still
    owned and cleaned up by the workflow execution. *)
type t

(** Creates a scope for the workflow execution currently running on this
    Domain. Outside workflow execution this returns a typed defect. *)
val create : unit -> (t, Error.t) result

(** Creates a scope, runs [body], and cancels the scope during cleanup. The
    body should use [await] for every operation whose observation belongs to
    this scope. Cleanup is idempotent and also runs when [body] raises an
    unexpected exception. *)
val with_scope :
  (t -> ('value, Error.t) result) ->
  ('value, Error.t) result

(** Requests cancellation of [scope]. The operation is idempotent. It must be
    called by the owning workflow scheduler while the scope is live; after the
    scheduler has shut down, it only records the terminal state. *)
val cancel : t -> (unit, Error.t) result

(** Reports whether cancellation has been requested for [scope]. *)
val is_cancelled : t -> bool

(** Checks the scope without waiting. An active scope returns [Ok ()]; a
    cancelled scope returns a typed [`Cancelled] error. *)
val check : t -> (unit, Error.t) result

(** Awaits [future] while observing [scope]. A cancellation requested before
    the future completes returns a typed [`Cancelled] error. Errors from the
    future itself pass through unchanged. The future must belong to the same
    workflow execution as the scope. *)
val await :
  t ->
  ('value, Error.t) Future.t ->
  ('value, Error.t) result
