(** The type of an OCaml function that implements a workflow. It receives a
    decoded input and returns either the workflow output or a structured error.
    The function must obey Temporal's workflow determinism rules. *)
type ('input, 'output) implementation =
  'input -> ('output, Error.t) result

(** A description of a workflow and the OCaml types it accepts and returns. A
    definition stores the Temporal workflow type name, its codecs, and
    optionally the OCaml function that implements it. *)
type ('input, 'output) t

(** Creates a workflow definition implemented by this OCaml worker. [name] is
    validated immediately; an empty or NUL-containing name raises
    [Invalid_argument] because it cannot be represented safely in Temporal
    history. The implementation must remain deterministic and return expected
    failures as [Error.t] values. *)
val define :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  ('input, 'output) implementation ->
  ('input, 'output) t

(** Creates a typed reference to a workflow implemented by another worker. Use
    it with [Child_workflow.start] or [Child_workflow.execute] when invoking the
    workflow as a child. The reference has no local implementation and cannot
    be registered as executable worker code. *)
val remote :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  ('input, 'output) t

(** Returns the workflow type name used for registration and commands. *)
val name : ('input, 'output) t -> string

(** Returns the codec used to decode workflow inputs. This accessor is useful
    to worker adapters and generic workflow tooling; the definition itself
    remains opaque and cannot be constructed by record syntax. *)
val input : ('input, 'output) t -> 'input Codec.t

(** Returns the codec used to encode successful workflow outputs. *)
val output : ('input, 'output) t -> 'output Codec.t

(** Returns executable code for a local definition, or [None] for a remote
    reference. The callback still returns typed [Error.t] values rather than
    raising expected workflow failures. *)
val implementation :
  ('input, 'output) t -> ('input, 'output) implementation option

(** Starts a durable Temporal timer and returns immediately. Starting several
    timers before awaiting one emits them in call order. A zero duration returns
    a ready future without recording a timer. Outside workflow execution the
    returned future is ready with a typed defect rather than touching global
    time or creating an unowned timer. *)
val start_sleep : Duration.t -> (unit, Error.t) Future.t

(** Starts a durable Temporal timer and waits until it fires. This is equivalent
    to [Future.await (start_sleep duration)]. A zero duration returns
    immediately without recording a timer; outside workflow execution it
    returns a typed defect. *)
val sleep : Duration.t -> (unit, Error.t) result

(** Ends the current run and starts a new run of [definition] with [input].
    This operation never returns to the calling workflow fiber. It is
    deterministic: the input is encoded through the definition's codec before
    the successor command is emitted. A codec failure fails the current run
    with a typed error; calling it outside workflow execution is programmer
    misuse and raises [Invalid_argument]. *)
val continue_as_new : ('input, 'output) t -> 'input -> 'value
