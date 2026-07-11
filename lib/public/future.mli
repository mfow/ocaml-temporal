(** A result that may become available in a later Temporal activation. A future
    belongs to one workflow execution. It is not a general-purpose promise for
    coordinating operating-system threads. *)
type ('value, 'error) t = ('value, 'error) Temporal_runtime.Future_store.t

(** Identifies the input that completed a heterogeneous race. *)
type ('left, 'right) race = Left of 'left | Right of 'right

(** Returns the result if it is ready. Otherwise, suspends only the current
    workflow fiber and lets other runnable workflow fibers continue. *)
val await : ('value, 'error) t -> ('value, 'error) result

(** Returns a future that applies the function to a successful result. Errors
    pass through unchanged, and this call never waits. *)
val map : ('value -> 'mapped) -> ('value, 'error) t -> ('mapped, 'error) t

(** Returns a future that applies the function to an error. Successful values
    pass through unchanged, and this call never waits. *)
val map_error :
  ('error -> 'mapped_error) ->
  ('value, 'error) t ->
  ('value, 'mapped_error) t

(** Returns a future that completes after both inputs complete. If both fail,
    the left error is returned. Failure of one input does not cancel the other.
    Both inputs must belong to the same workflow execution; otherwise the
    returned future is a ready structured defect owned by [left]. *)
val both :
  ('left, Error.t) t ->
  ('right, Error.t) t ->
  ('left * 'right, Error.t) t

(** Returns a future that completes after every input. Successful values retain
    input order. When inputs fail, the first error in input order is returned
    after all siblings settle. The empty list succeeds immediately and, when
    called by workflow code, retains that execution's ownership so it can be
    composed with other futures. Inputs from different executions produce a
    ready structured defect owned by the first input. *)
val all : ('value, Error.t) t list -> ('value list, Error.t) t

(** Returns the first completion from two differently typed inputs. An error is
    a completion and therefore can win. Losing operations are not cancelled.
    When both inputs are already ready, [left] wins; otherwise deterministic
    scheduler callback order decides. Different execution owners produce a
    ready defect owned by [left]. *)
val race :
  ('left, Error.t) t ->
  ('right, Error.t) t ->
  (('left, 'right) race, Error.t) t

(** Returns the first completion from a non-empty homogeneous collection. The
    first argument and then list order determine precedence among ready inputs;
    otherwise deterministic scheduler callback order decides. An owner mismatch
    produces a ready structured defect owned by the first argument. *)
val first :
  ('value, Error.t) t ->
  ('value, Error.t) t list ->
  ('value, Error.t) t

(** Reports whether a result is available without waiting. *)
val is_ready : ('value, 'error) t -> bool

(** Returns [Some result] when ready and [None] otherwise. It does not wait or
    schedule any work. *)
val peek : ('value, 'error) t -> ('value, 'error) result option
