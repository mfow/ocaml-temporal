type ('input, 'output) implementation =
  'input -> ('output, Error.t) result

type ('input, 'output) t

val define :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  ('input, 'output) implementation ->
  ('input, 'output) t
(** Define an activity implemented by this worker. *)

val remote :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  ('input, 'output) t
(** Declare a typed activity implemented by another worker. *)

val name : ('input, 'output) t -> string
