(** Translation between the checked JSON workflow protocol and the private
    deterministic OCaml execution model.

    This module deliberately does not know about the supervisor or the Rust
    bridge. It is the narrow, pure-OCaml boundary that turns one validated
    [Workflow_protocol.activation] into [Activation.job] values and turns
    commands emitted by [Execution] back into protocol commands. Values that the
    current synthetic runtime cannot represent are rejected explicitly; they are
    never replaced with guessed Temporal defaults. *)

type error
(** A semantic translation failure. The representation stays private so a caller
    can log the stable view without depending on implementation fields. *)

type error_view = { code : string; path : string; message : string }
(** Safe diagnostics for a translation failure. Payload bytes are never copied
    into this record. *)

val error_view : error -> error_view
(** Returns the stable classification, path, and diagnostic for [error]. *)

type initialization = {
  workflow_id : string;
  workflow_type : string;
  arguments : Temporal_protocol.Workflow_protocol.payload list;
  randomness_seed : string;
  attempt : int;
  context : Temporal_protocol.Workflow_protocol.initialize_context option;
}
(** Initialization data retained alongside the runtime's [Start_workflow]
    marker. [Runtime.Activation.job] intentionally carries no initialization
    arguments, so retaining this record prevents the adapter from discarding
    Core's workflow identity, attempt, seed, and context. *)

type cache_removal = {
  message : string;
  reason : Temporal_protocol.Workflow_protocol.eviction_reason;
}
(** Cache-removal details retained because the runtime job is only a marker. *)

type translated_activation = {
  run_id : string;
  timestamp : Temporal_protocol.Workflow_protocol.timestamp option;
  is_replaying : bool;
  history_length : int64;
  metadata : Temporal_protocol.Workflow_protocol.activation_metadata option;
  initialization : initialization option;
  cancellation_reason : string option;
  cache_removal : cache_removal option;
  jobs : Activation.job list;
}
(** Activation after translation. [jobs] has exactly the source ordering, while
    the optional fields retain protocol facts that the small runtime job algebra
    cannot yet carry. *)

val translate_activation :
  Temporal_protocol.Workflow_protocol.activation ->
  (translated_activation, error) result
(** Translates and validates one activation. The protocol's own strict encoder
    is run first, so programmatically constructed values receive the same bounds
    and closed-object checks as JSON received from Rust. *)

val activation_jobs :
  Temporal_protocol.Workflow_protocol.activation ->
  (Activation.job list, error) result
(** Convenience projection used by a worker loop that only needs jobs. *)

val command_to_protocol :
  Activation.command ->
  (Temporal_protocol.Workflow_protocol.completion_command, error) result
(** Converts one runtime command when every field has an exact protocol
    representation. Scheduling an activity or child workflow currently returns
    [Unsupported] because the synthetic command algebra lacks the required
    Temporal identifiers, arguments, and timeout/options fields. *)

val completion_of_commands :
  run_id:string ->
  Activation.command list ->
  (Temporal_protocol.Workflow_protocol.completion, error) result
(** Converts an ordered command batch into a checked protocol completion. *)

val activate :
  ('input, 'output) Execution.t ->
  Temporal_protocol.Workflow_protocol.activation ->
  (Temporal_protocol.Workflow_protocol.completion, error) result
(** Runs one translated activation through an existing deterministic execution
    and converts its commands into the protocol completion. The execution's
    input and definition are supplied by the caller because the protocol's
    initialization arguments are intentionally retained in
    [translated_activation] rather than guessed into an existential value. *)
