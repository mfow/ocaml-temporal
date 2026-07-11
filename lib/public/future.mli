(** A result that may become available in a later Temporal activation. A future
    belongs to one workflow execution. It is not a general-purpose promise for
    coordinating operating-system threads. *)
type ('value, 'error) t = ('value, 'error) Temporal_runtime.Future_store.t

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
    Both inputs must belong to the same workflow execution. *)
val both :
  ('left, 'error) t ->
  ('right, 'error) t ->
  ('left * 'right, 'error) t

(** Reports whether a result is available without waiting. *)
val is_ready : ('value, 'error) t -> bool

(** Returns [Some result] when ready and [None] otherwise. It does not wait or
    schedule any work. *)
val peek : ('value, 'error) t -> ('value, 'error) result option
