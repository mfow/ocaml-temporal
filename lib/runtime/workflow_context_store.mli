type t

val create : Scheduler.t -> t
val current : unit -> t option
val with_context : t -> (unit -> 'value) -> 'value

val resolved :
  t ->
  ('value, Temporal_base.Error.t) result ->
  ('value, Temporal_base.Error.t) Future_store.t

val detached_error :
  message:string -> ('value, Temporal_base.Error.t) Future_store.t

val schedule_activity :
  t ->
  name:string ->
  input:Temporal_base.Codec.payload ->
  decode:(Temporal_base.Codec.payload -> ('output, Temporal_base.Error.t) result) ->
  ('output, Temporal_base.Error.t) Future_store.t

val start_timer : t -> int64 -> (unit, Temporal_base.Error.t) Future_store.t

val resolve_activity :
  t ->
  seq:int64 ->
  (Temporal_base.Codec.payload, Temporal_base.Error.t) result ->
  (unit, Temporal_base.Error.t) result

val fire_timer : t -> seq:int64 -> (unit, Temporal_base.Error.t) result
val emit : t -> Activation.command -> unit
val take_commands : t -> Activation.command list
val shutdown : t -> unit
