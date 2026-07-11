(** The shared representation of a workflow or activity definition. The first
    two type parameters are its input and output types. The third describes the
    OCaml implementation stored for a local definition. *)
type ('input, 'output, 'implementation) t

(** Creates a definition after validating its Temporal type name. Pass
    [implementation = None] for code that runs in another worker. *)
val make :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  implementation:'implementation option ->
  ('input, 'output, 'implementation) t

(** Returns the stable Temporal registration or command name. *)
val name : ('input, 'output, 'implementation) t -> string

(** Returns the codec for values entering the definition. *)
val input : ('input, 'output, 'implementation) t -> 'input Codec.t

(** Returns the codec for successful values produced by the definition. *)
val output : ('input, 'output, 'implementation) t -> 'output Codec.t

(** Returns local executable code when this is a local definition. *)
val implementation :
  ('input, 'output, 'implementation) t -> 'implementation option
