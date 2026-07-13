(** Typed workflow update definitions.

    An update is a named, durable request that validates input, changes
    workflow state, and returns a typed result. Validation is run before the
    update callback and must not mutate state or emit commands. Native workers
    use the same dispatcher after the Rust/Core bridge decodes a [DoUpdate]
    activation. The first native slice requires one input payload and a
    non-suspending handler; suspended update continuations are reserved for a
    later runtime milestone. *)

(** A validated update name paired with input and output codecs. *)
type ('input, 'output) definition

(** Public name for an update definition. *)
type ('input, 'output) t = ('input, 'output) definition

(** Creates an update definition after validating its stable name. *)
val define :
  name:string -> input:'input Codec.t -> output:'output Codec.t ->
  ('input, 'output) t

(** Returns the stable update name. *)
val name : ('input, 'output) t -> string

(** Returns the codec used to decode update arguments. *)
val input : ('input, 'output) t -> 'input Codec.t

(** Returns the codec used to encode successful update results. *)
val output : ('input, 'output) t -> 'output Codec.t

(** An existentially packaged update validator and handler. *)
module Handler : sig
  (** A handler retains input/output codecs with both callbacks. *)
  type t

  (** Builds an update handler. [validator] runs first when supplied. A
      validator may reject with a typed [Error.t]; in that case [run] is not
      called and no update-side state change can occur. *)
  val make :
    ?validator:('input -> (unit, Error.t) result) ->
    ('input, 'output) definition ->
    ('input -> ('output, Error.t) result) ->
    t

  (** Convenience alias for [make]. *)
  val handle :
    ?validator:('input -> (unit, Error.t) result) ->
    ('input, 'output) definition ->
    ('input -> ('output, Error.t) result) ->
    t

  (** Returns the update name used to index an interaction registry. *)
  val name : t -> string

  (** Decodes input, optionally validates it, runs the update, and encodes the
      result. Native replay passes [~run_validator:false] because validation
      is a live-request check and must not run against historical input. *)
  val dispatch :
    ?run_validator:bool -> t -> Payload.t -> (Payload.t, Error.t) result
end
