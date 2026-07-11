type payload = Payload.t = { metadata : (string * string) list; data : bytes }
type 'a t
(** A bidirectional, typed Temporal payload codec. *)

val make :
  encoding:string ->
  encode:('a -> (bytes, Error.t) result) ->
  decode:(bytes -> ('a, Error.t) result) ->
  'a t

val encode : 'a t -> 'a -> (payload, Error.t) result
val decode : 'a t -> payload -> ('a, Error.t) result
val string : string t
(** UTF-8 JSON string payloads using [json/plain]. *)

val bytes : bytes t
(** Copied byte payloads using [binary/plain]. *)

val unit : unit t
(** Empty payloads using [binary/null]. *)

val option : 'a t -> 'a option t
(** [None] uses [binary/null]; [Some value] retains the nested encoding. *)
