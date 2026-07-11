type ('value, 'error) t = ('value, 'error) Temporal_runtime.Future_store.t

val await : ('value, 'error) t -> ('value, 'error) result
(** Return immediately when ready or suspend the current workflow fiber. *)

val map : ('value -> 'mapped) -> ('value, 'error) t -> ('mapped, 'error) t

val map_error :
  ('error -> 'mapped_error) ->
  ('value, 'error) t ->
  ('value, 'mapped_error) t

val both :
  ('left, 'error) t ->
  ('right, 'error) t ->
  ('left * 'right, 'error) t
(** Wait for both futures without cancelling either sibling on failure. *)

val is_ready : ('value, 'error) t -> bool
val peek : ('value, 'error) t -> ('value, 'error) result option
