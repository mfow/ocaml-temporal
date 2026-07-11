(** The type of an OCaml function that implements a workflow. It receives a
    decoded input and returns either the workflow output or a structured error.
    The function must obey Temporal's workflow determinism rules. *)
type ('input, 'output) implementation =
  'input -> ('output, Error.t) result

(** A description of a workflow and the OCaml types it accepts and returns. A
    definition stores the Temporal workflow type name, its codecs, and
    optionally the OCaml function that implements it. *)
type ('input, 'output) t =
  ( 'input,
    'output,
    ('input, 'output) implementation )
  Temporal_base.Definition.t

(** Creates a workflow definition implemented by this OCaml worker. *)
val define :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  ('input, 'output) implementation ->
  ('input, 'output) t

(** Creates a typed reference to a workflow implemented by another worker. Use
    it with [Child_workflow.start] or [Child_workflow.execute] when invoking the
    workflow as a child. *)
val remote :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  ('input, 'output) t

(** Returns the workflow type name used for registration and commands. *)
val name : ('input, 'output) t -> string

(** Starts a durable Temporal timer and returns immediately. Starting several
    timers before awaiting one emits them in call order. A zero duration returns
    a ready future without recording a timer. *)
val start_sleep : Duration.t -> (unit, Error.t) Future.t

(** Starts a durable Temporal timer and waits until it fires. This is equivalent
    to [Future.await (start_sleep duration)]. A zero duration returns
    immediately without recording a timer. *)
val sleep : Duration.t -> (unit, Error.t) result
