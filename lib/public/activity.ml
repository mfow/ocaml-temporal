(** Defines the public activity description. The implementation is retained in
    a private record and converted to the base definition only when a native
    worker is created. *)
type ('input, 'output) implementation =
  'input -> ('output, Error.t) result

type ('input, 'output) t = {
  name : string;
  input : 'input Codec.t;
  output : 'output Codec.t;
  implementation : ('input, 'output) implementation option;
}

(* Validates a stable activity type name before it can enter registration or
   command history. Empty and NUL-containing names cannot be represented
   safely by the bridge protocol, so they are programmer-facing construction
   errors rather than typed activity failures. *)
let validate_name name =
  if String.length name = 0 then invalid_arg "Temporal definition name is empty";
  if String.contains name '\000' then
    invalid_arg "Temporal definition name contains a NUL byte"

(** Constructs an activity implemented by this worker. *)
let define ~name ~input ~output implementation =
  validate_name name;
  { name; input; output; implementation = Some implementation }

(** Constructs a command-only reference to an activity on another worker. *)
let remote ~name ~input ~output =
  validate_name name;
  { name; input; output; implementation = None }

(* Returns the exact Temporal activity type name used by registration and
   schedule commands. *)
let name definition = definition.name

(* Returns the input codec retained by an opaque activity definition. *)
let input definition = definition.input

(* Returns the output codec retained by an opaque activity definition. *)
let output definition = definition.output

(* Returns executable code for a local activity, or [None] for a remote
   reference that can only be scheduled. *)
let implementation definition = definition.implementation

type cancellation_type =
  | Try_cancel
  | Wait_cancellation_completed
  | Abandon

(* Converts the public cancellation policy to the private runtime variant at
   the last boundary before a command is emitted. *)
let runtime_cancellation_type = function
  | Try_cancel -> Temporal_runtime.Activation.Try_cancel
  | Wait_cancellation_completed ->
      Temporal_runtime.Activation.Wait_cancellation_completed
  | Abandon -> Temporal_runtime.Activation.Abandon

(** Validates optional identifiers before a deterministic command sequence is
    allocated. *)
let validate_optional_identifier field = function
  | None -> Ok ()
  | Some value when String.equal value "" ->
      Error (Error.defect ~message:(field ^ " must not be empty"))
  | Some value when String.contains value '\000' ->
      Error (Error.defect ~message:(field ^ " must not contain NUL"))
  | Some value when String.length value > 65_536 ->
      Error (Error.defect ~message:(field ^ " exceeds 65536 bytes"))
  | Some value when not (Temporal_base.Codec.valid_utf_8 value) ->
      Error (Error.defect ~message:(field ^ " must be valid UTF-8"))
  | Some _ -> Ok ()

(* Supplies the typed error used when a caller tries to schedule an activity
   without an active workflow execution. *)
let outside_error () =
  Error.defect ~message:"activity operation used outside a workflow"

(** Builds a ready public future for validation failures and detached calls. *)
let resolved result = Future_private.resolved ~outside_error result

(** Schedules an activity after encoding input and validating all command
    options. The native runtime receives a base payload; its result decoder is
    converted back to public errors at the same boundary. *)
let start ?activity_id ?task_queue ?schedule_to_close_timeout
    ?schedule_to_start_timeout ?start_to_close_timeout ?heartbeat_timeout
    ?(cancellation_type = Try_cancel) ?(do_not_eagerly_execute = false)
    definition input =
  match Codec_private.encode_base definition.input input with
  | Error error -> resolved (Error (Error_private.of_base error))
  | Ok input -> (
      match validate_optional_identifier "activity id" activity_id with
      | Error error -> resolved (Error error)
      | Ok () -> (
          match validate_optional_identifier "task queue" task_queue with
          | Error error -> resolved (Error error)
          | Ok () ->
              match Temporal_runtime.Workflow_context_store.current () with
              | None ->
                  resolved
                    (Error
                       (Error.defect
                          ~message:"activity operation used outside a workflow"))
              | Some context ->
                  let schedule_to_close_timeout =
                    Option.map Duration.to_ms schedule_to_close_timeout
                  in
                  let schedule_to_start_timeout =
                    Option.map Duration.to_ms schedule_to_start_timeout
                  in
                  let start_to_close_timeout =
                    Option.map Duration.to_ms start_to_close_timeout
                  in
                  let heartbeat_timeout =
                    Option.map Duration.to_ms heartbeat_timeout
                  in
                  Future_private.of_internal
                    (Temporal_runtime.Workflow_context_store.schedule_activity
                       context ~name:(name definition) ~input ?activity_id
                       ?task_queue ?schedule_to_close_timeout
                       ?schedule_to_start_timeout ?start_to_close_timeout
                       ?heartbeat_timeout
                       ~cancellation_type:(runtime_cancellation_type cancellation_type)
                       ~do_not_eagerly_execute
                       ~decode:(Codec_private.decode_base definition.output)
                       ())) )

(** Direct-style convenience for scheduling and awaiting one activity. *)
let execute ?activity_id ?task_queue ?schedule_to_close_timeout
    ?schedule_to_start_timeout ?start_to_close_timeout ?heartbeat_timeout
    ?cancellation_type ?do_not_eagerly_execute definition input =
  Future.await
    (start ?activity_id ?task_queue ?schedule_to_close_timeout
       ?schedule_to_start_timeout ?start_to_close_timeout ?heartbeat_timeout
       ?cancellation_type ?do_not_eagerly_execute definition input)
