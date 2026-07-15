(** Defines the public workflow description. Its record is deliberately kept
    private so callers can only create a validated definition through [define]
    or a command-only reference through [remote]. *)
type ('input, 'output) implementation =
  'input -> ('output, Error.t) result

type ('input, 'output) t = {
  (* Stable Temporal workflow type name used in registrations and child
     workflow commands; it is validated before this record is published. *)
  name : string;
  (* Codec used to decode the payload supplied when this workflow starts. *)
  input : 'input Codec.t;
  (* Codec used to encode successful outputs returned to Temporal. *)
  output : 'output Codec.t;
  (* Local executable code, or [None] for a command-only remote reference. *)
  implementation : ('input, 'output) implementation option;
}

(** Maximum byte length accepted by the closed JSON/native identifier contract. *)
let max_name_bytes = 65_536

(** Rejects names that could not be represented safely in Temporal history.
    Definition construction is the earliest point at which a workflow type
    name is available, so enforcing the bridge's complete identifier contract
    here prevents a malformed name from surviving until worker registration or
    a child/continue-as-new command is emitted. *)
let validate_name name =
  if String.length name = 0 then invalid_arg "Temporal definition name is empty";
  if String.contains name '\000' then
    invalid_arg "Temporal definition name contains a NUL byte"
  else if String.length name > max_name_bytes then
    invalid_arg "Temporal definition name exceeds 65536 bytes"
  else if not (Temporal_base.Codec.valid_utf_8 name) then
    invalid_arg "Temporal definition name must be valid UTF-8"

(** Registers executable workflow code after validating its stable type name. *)
let define ~name ~input ~output implementation =
  validate_name name;
  { name; input; output; implementation = Some implementation }

(** Creates a typed reference to a workflow implemented by another worker. The
    reference retains codecs for child-result correlation but no executable
    callback, so it cannot accidentally be registered as local worker code. *)
let remote ~name ~input ~output =
  validate_name name;
  { name; input; output; implementation = None }

(** Returns the exact Temporal workflow type name used by registration and
    child-workflow commands. *)
let name definition = definition.name

(** Returns the input codec retained by an opaque workflow definition. *)
let input definition = definition.input

(** Returns the output codec retained by an opaque workflow definition. *)
let output definition = definition.output

(** Returns executable code for a local workflow, or [None] for a remote
    reference that can only be invoked as a child. *)
let implementation definition = definition.implementation

(** Starts a durable timer without waiting for it. The runtime future is
    wrapped before it crosses into the public API, converting native errors to
    public errors and preserving the workflow scheduler owner. *)
let start_sleep duration =
  match Temporal_runtime.Workflow_context_store.current () with
  | None ->
      Future_private.resolved
        ~outside_error:(fun () ->
          Error.defect ~message:"workflow sleep used outside a workflow execution")
        (Error
           (Error.defect ~message:"workflow sleep used outside a workflow execution"))
  | Some context ->
      let milliseconds = Duration.to_ms duration in
      if milliseconds = 0L then
        (* A zero-duration timer is already ready; omitting the command keeps
           replay history free of a timer that cannot suspend the workflow. *)
        Future_private.of_internal
          (Temporal_runtime.Workflow_context_store.resolved context (Ok ()))
      else
        Future_private.of_internal
          (Temporal_runtime.Workflow_context_store.start_timer context milliseconds)

(** Implements direct-style sleep as timer creation followed by a future wait. *)
let sleep duration = Future.await (start_sleep duration)

(** Reads the deterministic timestamp captured from the current Temporal
    activation. The runtime stores it in the workflow context before invoking
    user code, which means repeated calls during one activation observe the
    same value and replay does not consult local time. *)
let now () =
  match Temporal_runtime.Workflow_context_store.current () with
  | None ->
      Error
        (Error.defect
           ~message:"Temporal.Workflow.now used outside a workflow execution")
  | Some context -> (
      match
        Temporal_runtime.Workflow_context_store.activation_timestamp context
      with
      | None ->
          Error
            (Error.defect
               ~message:
                 "Temporal.Workflow.now is unavailable for this activation")
      | Some timestamp ->
          Time.of_unix ~seconds:timestamp.seconds
            ~nanoseconds:timestamp.nanoseconds)

(** Validates and snapshots a patch ID before consulting workflow state. The
    copy prevents a caller-created mutable string from changing the hash-table
    key or emitted command after this call returns. *)
let validated_patch_id ~operation id =
  let prefix = "Temporal.Workflow." ^ operation ^ " id " in
  if String.length id = 0 then
    invalid_arg (prefix ^ "is empty");
  if String.contains id '\000' then
    invalid_arg (prefix ^ "contains a NUL byte")
  else if String.length id > max_name_bytes then
    invalid_arg (prefix ^ "exceeds 65536 bytes")
  else if not (Temporal_base.Codec.valid_utf_8 id) then
    invalid_arg (prefix ^ "must be valid UTF-8");
  Bytes.to_string (Bytes.of_string id)

(** Returns the replay-safe branch decision for one named workflow patch.
    Core notifications and the activation replay flag determine the first
    decision; the execution context retains it for later calls in this run. *)
let patched ~id =
  let patch_id = validated_patch_id ~operation:"patched" id in
  match Temporal_runtime.Workflow_context_store.current () with
  | None ->
      invalid_arg "Temporal.Workflow.patched used outside a workflow execution"
  | Some context ->
      Temporal_runtime.Workflow_context_store.patched context ~patch_id

(** Records that one previously active patch is being phased out. The runtime
    retains Core's deterministic decision internally but exposes no branch
    result: application code should replace the old [patched] gate with this
    lifecycle marker while compatible histories drain. *)
let deprecate_patch ~id =
  let patch_id = validated_patch_id ~operation:"deprecate_patch" id in
  match Temporal_runtime.Workflow_context_store.current () with
  | None ->
      invalid_arg
        "Temporal.Workflow.deprecate_patch used outside a workflow execution"
  | Some context ->
      Temporal_runtime.Workflow_context_store.deprecate_patch context ~patch_id

(** Requests a fresh run of the same workflow type with [input]. This is a
    terminal direct-style operation: it encodes the successor input, buffers a
    Core continue-as-new command, and aborts the current private workflow
    fiber. If encoding fails, the current run is failed with that typed codec
    error instead of raising it through the worker loop. *)
let continue_as_new definition next_input =
  match Temporal_runtime.Workflow_context_store.current () with
  | None ->
      invalid_arg "Temporal.Workflow.continue_as_new used outside a workflow execution"
  | Some context -> (
      match Codec_private.encode_base (input definition) next_input with
      | Ok payload ->
          Temporal_runtime.Workflow_context_store.continue_as_new context
            ~workflow_type:(name definition) ~input:payload
      | Error error ->
          Temporal_runtime.Workflow_context_store.terminate context
            (Temporal_runtime.Activation.Fail_workflow error))
