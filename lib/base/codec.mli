type payload = Payload.t = { metadata : (string * string) list; data : bytes }
type 'a t

val make :
  encoding:string ->
  encode:('a -> (bytes, Error.t) result) ->
  decode:(bytes -> ('a, Error.t) result) ->
  'a t

val encode : 'a t -> 'a -> (payload, Error.t) result
val decode : 'a t -> payload -> ('a, Error.t) result
val string : string t
val bytes : bytes t
val unit : unit t
val option : 'a t -> 'a option t
