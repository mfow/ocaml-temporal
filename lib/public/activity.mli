type ('input, 'output) implementation =
  'input -> ('output, Error.t) result

type ('input, 'output) t =
  ( 'input,
    'output,
    ('input, 'output) implementation )
  Temporal_base.Definition.t

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

val start :
  ('input, 'output) t ->
  'input ->
  ('output, Error.t) Future.t
(** Schedule immediately and return a typed future. *)

val execute :
  ('input, 'output) t -> 'input -> ('output, Error.t) result
(** Schedule and await an activity in direct style. *)
