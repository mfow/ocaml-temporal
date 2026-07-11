type owner

val make_owner :
  id:int ->
  enqueue:((unit -> unit) -> unit) ->
  is_running:(unit -> bool) ->
  on_create:(unit -> unit) ->
  on_settled:(unit -> unit) ->
  register_teardown:((unit -> unit) -> unit) ->
  owner

type ('value, 'error) t
type ('value, 'error) resolver = ('value, 'error) result -> unit

type _ Effect.t +=
  | Await : ('value, 'error) t -> ('value, 'error) result Effect.t

val create :
  owner:owner ->
  outside_error:(unit -> 'error) ->
  ('value, 'error) t * ('value, 'error) resolver

val resolved :
  outside_error:(unit -> 'error) ->
  ('value, 'error) result ->
  ('value, 'error) t

val owner_id : ('value, 'error) t -> int
val await : ('value, 'error) t -> ('value, 'error) result

val add_waiter :
  ('value, 'error) t ->
  (('value, 'error) result, unit) Effect.Deep.continuation ->
  unit

val map : ('value -> 'mapped) -> ('value, 'error) t -> ('mapped, 'error) t

val map_error :
  ('error -> 'mapped_error) ->
  ('value, 'error) t ->
  ('value, 'mapped_error) t

val both :
  ('left, 'error) t ->
  ('right, 'error) t ->
  ('left * 'right, 'error) t

val is_ready : ('value, 'error) t -> bool
val peek : ('value, 'error) t -> ('value, 'error) result option
