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
(** Define a workflow implemented by this worker. *)

val remote :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  ('input, 'output) t
(** Declare a typed child or external workflow implemented elsewhere. *)

val name : ('input, 'output) t -> string

val sleep : Duration.t -> (unit, Error.t) result
(** Start a durable timer and suspend; zero duration returns immediately. *)
