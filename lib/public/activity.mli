(** The type of an OCaml function that implements an activity. It receives the
    decoded input and returns either its output or a structured error. *)
type ('input, 'output) implementation =
  'input -> ('output, Error.t) result

(** A description of an activity and the OCaml types it accepts and returns.
    The definition stores the activity's Temporal name and its input and output
    codecs. It may also contain a local implementation. *)
type ('input, 'output) t =
  ( 'input,
    'output,
    ('input, 'output) implementation )
  Temporal_base.Definition.t

(** Creates a definition for an activity that this OCaml worker will run. *)
val define :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  ('input, 'output) implementation ->
  ('input, 'output) t

(** Creates a typed reference to an activity run by another worker. The name
    and codecs must agree with that worker's activity definition. *)
val remote :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  ('input, 'output) t

(** Returns the activity type name sent to Temporal when it is scheduled. *)
val name : ('input, 'output) t -> string

(** Schedules the activity and returns immediately with a future for its
    result. Start several independent activities before awaiting them to let
    Temporal run them concurrently. If input encoding fails, the returned
    future contains that error and no activity command is emitted. *)
val start :
  ('input, 'output) t ->
  'input ->
  ('output, Error.t) Future.t

(** Schedules the activity and waits for its result. This is equivalent to
    calling [start] followed by [Future.await]. *)
val execute :
  ('input, 'output) t -> 'input -> ('output, Error.t) result
