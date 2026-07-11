type ('input, 'output, 'implementation) t

val make :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  implementation:'implementation option ->
  ('input, 'output, 'implementation) t

val name : ('input, 'output, 'implementation) t -> string
val input : ('input, 'output, 'implementation) t -> 'input Codec.t
val output : ('input, 'output, 'implementation) t -> 'output Codec.t

val implementation :
  ('input, 'output, 'implementation) t -> 'implementation option
