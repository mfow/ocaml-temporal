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

module Retry_policy = struct
  (** The public retry policy keeps the source float for accessors and its
      exact IEEE-754 representation for the JSON bridge.  Storing both avoids
      re-parsing a decimal value during every command while ensuring that
      replay sees precisely the bits selected by the caller. *)
  type t = {
    initial_interval : Duration.t;
    backoff_coefficient : float;
    backoff_coefficient_bits : string;
    maximum_interval : Duration.t;
    maximum_attempts : int;
    non_retryable_error_types : string list;
  }

  (** Formats an arbitrary 64-bit bit pattern as the canonical unsigned
      decimal required by the bridge protocol.  OCaml's [Int64.to_string]
      treats patterns with the high bit set as negative, so the conversion
      uses two base-[2^32] limbs and repeated division by ten instead. *)
  let unsigned_int64_decimal bits =
    if Int64.compare bits 0L >= 0 then Int64.to_string bits
    else
      let base = 4_294_967_296L in
      let high = Int64.logand (Int64.shift_right_logical bits 32) 0xffff_ffffL in
      let low = Int64.logand bits 0xffff_ffffL in
      let rec digits high low acc =
        if Int64.equal high 0L && Int64.equal low 0L then acc
        else
          let high_quotient = Int64.div high 10L in
          let high_remainder = Int64.rem high 10L in
          let combined =
            Int64.add (Int64.mul high_remainder base) low
          in
          let low_quotient = Int64.div combined 10L in
          let digit = Int64.to_int (Int64.rem combined 10L) in
          digits high_quotient low_quotient
            (Char.chr (Char.code '0' + digit) :: acc)
      in
      let characters = digits high low [] in
      String.concat "" (List.map (String.make 1) characters)

  (** Validates names embedded in the retry policy.  Keeping this validation at
      construction means a policy cannot later fail merely because a worker
      tries to serialize it. *)
  let validate_non_retryable_error_type index value =
    let field = "non_retryable_error_types[" ^ string_of_int index ^ "]" in
    if String.equal value "" then
      Error (Error.defect ~message:(field ^ " must not be empty"))
    else if String.contains value '\000' then
      Error (Error.defect ~message:(field ^ " must not contain NUL"))
    else if String.length value > 65_536 then
      Error (Error.defect ~message:(field ^ " exceeds 65536 bytes"))
    else if not (Temporal_base.Codec.valid_utf_8 value) then
      Error (Error.defect ~message:(field ^ " must be valid UTF-8"))
    else Ok ()

  (** Validates and constructs a policy without allowing invalid values to
      cross into workflow history.  The checks mirror Temporal Core's retry
      policy constraints and are deliberately expressed as [result] values so
      routine configuration failures never use exceptions as control flow. *)
  let make ~initial_interval ~backoff_coefficient ~maximum_interval
      ~maximum_attempts ?(non_retryable_error_types = []) () =
    let initial_ms = Duration.to_ms initial_interval in
    let maximum_ms = Duration.to_ms maximum_interval in
    let invalid message = Error (Error.defect ~message) in
    if Int64.compare initial_ms 0L <= 0 then
      invalid "retry policy initial_interval must be positive"
    else if Int64.compare maximum_ms initial_ms < 0 then
      invalid
        "retry policy maximum_interval must be at least initial_interval"
    else
      match classify_float backoff_coefficient with
      | FP_nan | FP_infinite ->
          invalid "retry policy backoff_coefficient must be finite"
      | FP_zero | FP_subnormal | FP_normal when backoff_coefficient < 1.0 ->
          invalid "retry policy backoff_coefficient must be at least 1.0"
      | FP_zero | FP_subnormal | FP_normal ->
          let maximum_int32 = Int32.to_int Int32.max_int in
          if maximum_attempts < 0 || maximum_attempts > maximum_int32 then
            invalid
              "retry policy maximum_attempts must be between 0 and Int32.max_int"
          else
            let rec validate_types index = function
              | [] -> Ok ()
              | value :: rest -> (
                  match validate_non_retryable_error_type index value with
                  | Error _ as error -> error
                  | Ok () -> validate_types (index + 1) rest)
            in
            (match validate_types 0 non_retryable_error_types with
            | Error _ as error -> error
            | Ok () ->
                Ok
                  {
                    initial_interval;
                    backoff_coefficient;
                    backoff_coefficient_bits =
                      unsigned_int64_decimal
                        (Int64.bits_of_float backoff_coefficient);
                    maximum_interval;
                    maximum_attempts;
                    non_retryable_error_types =
                      List.map Fun.id non_retryable_error_types;
                  })

  (** The [create] spelling is useful at call sites that treat policies as
      immutable values rather than mutable builders. *)
  let create ~initial_interval ~backoff_coefficient ~maximum_interval
      ~maximum_attempts ?non_retryable_error_types () =
    make ~initial_interval ~backoff_coefficient ~maximum_interval
      ~maximum_attempts ?non_retryable_error_types ()

  (** Returns the exact delay before the first retry. *)
  let initial_interval policy = policy.initial_interval

  (** Returns the multiplier applied to the previous retry delay. *)
  let backoff_coefficient policy = policy.backoff_coefficient

  (** Returns the upper bound for a retry delay. *)
  let maximum_interval policy = policy.maximum_interval

  (** Returns the attempt cap, where zero means unlimited attempts. *)
  let maximum_attempts policy = policy.maximum_attempts

  (** A fresh list protects the policy's immutable representation from callers
      that retain and mutate a list value they received from an accessor. *)
  let non_retryable_error_types policy =
    List.map Fun.id policy.non_retryable_error_types
end

(* Copies a validated public policy into the private runtime record.  This is
   the only place where the public activity module knows the bridge's compact
   representation; the public signature keeps those details hidden. *)
let runtime_retry_policy policy =
  Temporal_runtime.Activation
  .{
    initial_interval = Duration.to_ms (Retry_policy.initial_interval policy);
    backoff_coefficient_bits = policy.backoff_coefficient_bits;
    maximum_interval = Duration.to_ms (Retry_policy.maximum_interval policy);
    maximum_attempts = Retry_policy.maximum_attempts policy;
    non_retryable_error_types = Retry_policy.non_retryable_error_types policy;
  }

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

(** Builds a ready public future for validation failures and detached calls.
    Inside a workflow the current scheduler owns the future so combinators
    such as [Future.both] report the real defect instead of a cross-execution
    ownership error from an inert owner id. *)
let resolved result =
  match Temporal_runtime.Workflow_context_store.current () with
  | Some context ->
      Future_private.of_internal
        (Temporal_runtime.Workflow_context_store.resolved context
           (Result.map_error Error_private.to_base result))
  | None -> Future_private.resolved ~outside_error result

(** Schedules an activity after encoding input and validating all command
    options. The native runtime receives a base payload; its result decoder is
    converted back to public errors at the same boundary. *)
let start ?activity_id ?task_queue ?schedule_to_close_timeout
    ?schedule_to_start_timeout ?start_to_close_timeout ?heartbeat_timeout
    ?retry_policy ?(cancellation_type = Try_cancel)
    ?(do_not_eagerly_execute = false)
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
                  let retry_policy =
                    Option.map runtime_retry_policy retry_policy
                  in
                  Future_private.of_internal
                    (Temporal_runtime.Workflow_context_store.schedule_activity
                       context ~name:(name definition) ~input ?activity_id
                       ?task_queue ?schedule_to_close_timeout
                       ?schedule_to_start_timeout ?start_to_close_timeout
                       ?heartbeat_timeout ?retry_policy
                       ~cancellation_type:(runtime_cancellation_type cancellation_type)
                       ~do_not_eagerly_execute
                       ~decode:(Codec_private.decode_base definition.output)
                       ())) )

(** Direct-style convenience for scheduling and awaiting one activity. *)
let execute ?activity_id ?task_queue ?schedule_to_close_timeout
    ?schedule_to_start_timeout ?start_to_close_timeout ?heartbeat_timeout
    ?retry_policy ?cancellation_type ?do_not_eagerly_execute definition input =
  Future.await
    (start ?activity_id ?task_queue ?schedule_to_close_timeout
       ?schedule_to_start_timeout ?start_to_close_timeout ?heartbeat_timeout
       ?retry_policy ?cancellation_type ?do_not_eagerly_execute definition input)
