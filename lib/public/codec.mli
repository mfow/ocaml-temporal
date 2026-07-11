(** The bytes and metadata that Temporal stores for one workflow or activity
    value. Most users work with typed codecs and do not construct this record
    directly. *)
type payload = Payload.t = { metadata : (string * string) list; data : bytes }

(** A codec converts values of type ['a] to and from Temporal payloads. *)
type 'a t = 'a Temporal_base.Codec.t

(** [make ~encoding ~encode ~decode] creates a codec from two functions. The
    [encode] function converts a value to bytes, and [decode] converts those
    bytes back to a value. The SDK writes [encoding] into the payload metadata
    and checks it before decoding, preventing the wrong codec from reading a
    payload. *)
val make :
  encoding:string ->
  encode:('a -> (bytes, Error.t) result) ->
  decode:(bytes -> ('a, Error.t) result) ->
  'a t

(** Converts a typed OCaml value into a Temporal payload. *)
val encode : 'a t -> 'a -> (payload, Error.t) result

(** Converts a Temporal payload back into a typed OCaml value after checking
    that its encoding metadata matches the codec. *)
val decode : 'a t -> payload -> ('a, Error.t) result

(** Encodes strings as JSON using the standard Temporal [json/plain] encoding
    name. JSON is a convenient interoperability format, not a Temporal
    requirement; applications may define other codecs with [make]. *)
val string : string t

(** Encodes raw bytes using [binary/plain]. The codec copies mutable byte
    buffers so later changes by a caller cannot alter a stored payload. *)
val bytes : bytes t

(** Encodes [()] as an empty [binary/null] payload. *)
val unit : unit t

(** Encodes [None] as [binary/null]. A [Some value] uses the supplied codec and
    therefore keeps that codec's encoding name. *)
val option : 'a t -> 'a option t
