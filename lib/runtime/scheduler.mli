type t
type status = Complete | Failed of exn | Blocked

val create : unit -> t

val promise :
  t ->
  outside_error:(unit -> 'error) ->
  ('value, 'error) Future_store.t * ('value, 'error) Future_store.resolver

val spawn : t -> (unit -> unit) -> unit
val run : t -> status
val run_label : t -> string
val trace : t -> int list
val shutdown : t -> unit
