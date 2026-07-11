(** The checked, pure-OCaml half of the workflow activation boundary.

    Rust owns Core's protobuf and network machinery. This module intentionally
    stays below the supervisor: it validates a semantic activation, converts its
    payloads into the deterministic runtime representation, runs an existing
    [Execution.t], and converts only exactly representable commands back to the
    semantic protocol. In particular, it never invents activity timeouts, task
    queues, or child-workflow options merely to make a command fit an older
    protocol shape. *)

module Protocol = Temporal_protocol.Workflow_protocol

(** Result-bind notation keeps every boundary conversion on the typed error
    path; no protocol or runtime input is handled with an exception. *)
let ( let* ) = Result.bind

type error = { code : string; path : string; message : string }
(** Translation errors are immutable and contain no payload bytes. The private
    representation lets us change internal constructors without changing the
    diagnostics consumed by a worker loop. *)

type error_view = { code : string; path : string; message : string }

type initialization = {
  workflow_id : string;
  workflow_type : string;
  arguments : Protocol.payload list;
  randomness_seed : string;
  attempt : int;
  context : Protocol.initialize_context option;
}
(** Initialization data retained while the runtime uses a smaller start marker.
*)

type cache_removal = { message : string; reason : Protocol.eviction_reason }
(** Cache-removal facts retained next to the runtime eviction marker. *)

type translated_activation = {
  run_id : string;
  timestamp : Protocol.timestamp option;
  is_replaying : bool;
  history_length : int64;
  metadata : Protocol.activation_metadata option;
  initialization : initialization option;
  cancellation_reason : string option;
  cache_removal : cache_removal option;
  jobs : Activation.job list;
}
(** A protocol activation after its jobs have been converted to runtime jobs. *)

(** Copies a translation error into its stable public diagnostic shape. *)
let error_view (error : error) : error_view =
  { code = error.code; path = error.path; message = error.message }

(** Creates one bounded diagnostic. Callers pass only constant or already
    validated descriptions; source payload bytes never enter the message. *)
let make_error code path message : error = { code; path; message }

(** Errors caused by a malformed value constructed by the caller or decoded by a
    peer. The protocol validator supplies the same code for JSON failures. *)
let invalid path message = make_error "invalid_message" path message

(** Errors caused by a value that is valid in one layer but has no exact
    representation in the other layer. *)
let unsupported path message = make_error "unsupported" path message

(** Converts the protocol module's privacy-safe diagnostic without exposing any
    of its internal error representation. *)
let protocol_error error =
  let view = Protocol.error_view error in
  make_error view.code view.path view.message

(** Keeps sequence conversion explicit. Core uses an unsigned 32-bit sequence
    number while the OCaml runtime uses [int64] to avoid platform narrowing. *)
let validate_sequence path sequence : (unit, error) result =
  if Int64.compare sequence 0L < 0 then
    Error (invalid path "sequence number must not be negative")
  else if Int64.compare sequence 4_294_967_295L > 0 then
    Error (invalid path "sequence number is outside unsigned 32-bit range")
  else Ok ()

(** Identifiers in runtime commands are checked here as well as by the protocol
    encoder, because callers may invoke [command_to_protocol] directly without
    first constructing a completion. *)
let validate_identifier path value : (unit, error) result =
  if String.length value = 0 then
    Error (invalid path "identifier must not be empty")
  else if String.contains value '\000' then
    Error (invalid path "identifier must not contain NUL")
  else if not (Temporal_base.Codec.valid_utf_8 value) then
    Error (invalid path "identifier must be valid UTF-8")
  else Ok ()

(** Copies a protocol payload before retaining it in translated activation
    metadata. Protocol payload bodies and metadata values are mutable [bytes],
    so retaining the caller's record directly would let a later mutation alter
    replay inputs after validation. *)
let copy_protocol_payload (value : Protocol.payload) : Protocol.payload =
  Protocol.
    {
      metadata =
        List.map (fun (key, bytes) -> (key, Bytes.copy bytes)) value.metadata;
      data = Bytes.copy value.data;
    }

(** Copies the payload-bearing part of initialization context. The remaining
    context fields are immutable strings, options, integers, and small records,
    so a record copy is sufficient for them. *)
let copy_initialize_context (value : Protocol.initialize_context) =
  Protocol.
    {
      value with
      headers =
        List.map
          (fun (key, payload) -> (key, copy_protocol_payload payload))
          value.headers;
    }

(** Converts one protocol payload into the runtime payload type. Runtime codecs
    intentionally expose metadata values as strings; a binary metadata value
    therefore cannot be represented losslessly and is reported as an explicit
    unsupported boundary instead of being silently decoded with a replacement
    character. *)
let runtime_payload path (value : Protocol.payload) =
  let rec metadata_loop reversed = function
    | [] -> Ok (List.rev reversed)
    | (key, bytes) :: rest ->
        let* () = validate_identifier (path ^ ".metadata.key") key in
        let value = Bytes.to_string bytes in
        if not (Temporal_base.Codec.valid_utf_8 value) then
          Error
            (unsupported
               (path ^ ".metadata." ^ key)
               "binary metadata is not representable by the runtime string \
                metadata type")
        else metadata_loop ((key, value) :: reversed) rest
  in
  let* metadata = metadata_loop [] value.metadata in
  Ok { Temporal_base.Payload.metadata; data = Bytes.copy value.data }

(** Converts a runtime payload back to the protocol's binary-safe payload.
    Copying both metadata bytes and the body makes ownership independent of the
    mutable [bytes] values held by workflow code. *)
let protocol_payload path (value : Temporal_base.Codec.payload) =
  let rec metadata_loop reversed = function
    | [] -> Ok (List.rev reversed)
    | (key, text) :: rest ->
        let* () = validate_identifier (path ^ ".metadata.key") key in
        if not (Temporal_base.Codec.valid_utf_8 text) then
          Error
            (invalid (path ^ ".metadata." ^ key) "metadata must be valid UTF-8")
        else metadata_loop ((key, Bytes.of_string text) :: reversed) rest
  in
  let* metadata = metadata_loop [] value.metadata in
  Ok { Protocol.metadata; data = Bytes.copy value.data }

(** Canonical payload used when Core reports a completed operation without a
    result. It is the same representation produced by [Codec.unit] and keeps the
    optional protocol field's [None] semantics deterministic. *)
let null_runtime_payload : Temporal_base.Codec.payload =
  {
    Temporal_base.Payload.metadata = [ ("encoding", "binary/null") ];
    data = Bytes.empty;
  }

(** Recognizes the same canonical null marker after conversion to protocol
    payload bytes. *)
let is_null_protocol_payload (value : Protocol.payload) =
  Bytes.length value.data = 0
  &&
  match value.metadata with
  | [ ("encoding", bytes) ] -> Bytes.equal bytes (Bytes.of_string "binary/null")
  | _ -> false

(** Adds the structured fields that the small public error type cannot store to
    a diagnostic string. This preserves useful source, stack, identity, and
    cause information without inventing a second error representation. *)
let failure_info_summary = function
  | Protocol.Application { type_name; non_retryable; details } ->
      Printf.sprintf "application type=%s non_retryable=%b details=%d" type_name
        non_retryable (List.length details)
  | Protocol.Canceled { details; identity } ->
      Printf.sprintf "canceled identity=%s details=%d" identity
        (List.length details)
  | Protocol.Activity
      {
        scheduled_event_id;
        started_event_id;
        identity;
        activity_type;
        activity_id;
        retry_state;
      } ->
      let retry_state =
        match retry_state with
        | Protocol.Unspecified -> "unspecified"
        | In_progress -> "in_progress"
        | Non_retryable_failure -> "non_retryable_failure"
        | Timeout -> "timeout"
        | Maximum_attempts_reached -> "maximum_attempts_reached"
        | Retry_policy_not_set -> "retry_policy_not_set"
        | Internal_server_error -> "internal_server_error"
        | Cancel_requested -> "cancel_requested"
      in
      Printf.sprintf
        "activity id=%s type=%s identity=%s scheduled_event_id=%Ld \
         started_event_id=%Ld retry_state=%s"
        activity_id activity_type identity scheduled_event_id started_event_id
        retry_state

(** Flattens a bounded failure chain into one deterministic diagnostic. The JSON
    protocol already limits nesting; the depth guard also protects callers that
    construct a recursive value directly. *)
let failure_diagnostic (failure : Protocol.failure) =
  let rec loop depth reversed (value : Protocol.failure) =
    let current =
      let source =
        if String.length value.source = 0 then []
        else [ "source=" ^ value.source ]
      in
      let stack_trace =
        if String.length value.stack_trace = 0 then []
        else [ "stack_trace=" ^ value.stack_trace ]
      in
      let attributes =
        match value.encoded_attributes with
        | None -> []
        | Some _ -> [ "encoded_attributes_present=true" ]
      in
      String.concat " "
        ((if String.length value.message = 0 then [] else [ value.message ])
        @ source @ stack_trace
        @ [ failure_info_summary value.info ]
        @ attributes)
    in
    match value.cause with
    | None -> String.concat " | " (List.rev (current :: reversed))
    | Some _ when depth >= 128 ->
        String.concat " | "
          (List.rev ("cause_depth_limit_reached" :: current :: reversed))
    | Some cause -> loop (depth + 1) (current :: reversed) cause
  in
  loop 0 [] failure

(** Collects application and cancellation details through an activity wrapper.
    Temporal commonly reports an activity failure as an outer [Activity] record
    whose [cause] contains the application failure and its payloads. The bounded
    walk preserves details from every such layer without allowing a malformed
    recursive value to consume unbounded stack or memory. *)
let failure_details (failure : Protocol.failure) =
  let rec loop depth reversed (value : Protocol.failure) =
    let reversed =
      match value.info with
      | Protocol.Application { details; _ } | Protocol.Canceled { details; _ }
        ->
          List.rev_append details reversed
      | Protocol.Activity _ -> reversed
    in
    match value.cause with
    | Some cause when depth < 128 -> loop (depth + 1) reversed cause
    | None | Some _ -> List.rev reversed
  in
  loop 0 [] failure

(** Converts one protocol failure to the broad typed error used by workflow
    futures. The error view retains the primary category and details, while the
    protocol's richer recursive fields are included in its diagnostic. *)
let runtime_error_of_failure path ~category (failure : Protocol.failure) =
  let rec details_loop reversed = function
    | [] -> Ok (List.rev reversed)
    | payload :: rest ->
        let* payload = runtime_payload (path ^ ".details") payload in
        details_loop (payload :: reversed) rest
  in
  let details = failure_details failure in
  let* details = details_loop [] details in
  let non_retryable =
    match failure.info with
    | Protocol.Application { non_retryable; _ } -> non_retryable
    | Protocol.Canceled _ -> false
    | Protocol.Activity { retry_state; _ } -> (
        match retry_state with
        | Protocol.Non_retryable_failure | Maximum_attempts_reached -> true
        | Unspecified | In_progress | Timeout | Retry_policy_not_set
        | Internal_server_error | Cancel_requested ->
            false)
  in
  Ok
    (Temporal_base.Error.make ~non_retryable ~details ~category
       ~message:(failure_diagnostic failure)
       ())

(** Converts a protocol activity result while preserving the distinction between
    a successful empty result and a failure. *)
let runtime_activity_result path result =
  match result with
  | Protocol.Completed None -> Ok (Ok null_runtime_payload)
  | Protocol.Completed (Some payload) ->
      let* payload = runtime_payload (path ^ ".payload") payload in
      Ok (Ok payload)
  | Protocol.Failed failure ->
      let* error =
        runtime_error_of_failure (path ^ ".failure") ~category:`Activity failure
      in
      Ok (Error error)
  | Protocol.Cancelled failure ->
      let* error =
        runtime_error_of_failure (path ^ ".failure") ~category:`Cancelled
          failure
      in
      Ok (Error error)

(** Converts an activation job and checks its sequence number before any mutable
    execution state is touched. *)
let runtime_job path = function
  | Protocol.Initialize_workflow
      {
        workflow_id;
        workflow_type;
        arguments;
        randomness_seed;
        attempt;
        context;
      } ->
      let* () = validate_identifier (path ^ ".workflow_id") workflow_id in
      let* () = validate_identifier (path ^ ".workflow_type") workflow_type in
      if attempt < 1 then
        Error (invalid (path ^ ".attempt") "attempt must be positive")
      else
        Ok
          ( Activation.Start_workflow,
            Some
              {
                workflow_id;
                workflow_type;
                arguments = List.map copy_protocol_payload arguments;
                randomness_seed;
                attempt;
                context = Option.map copy_initialize_context context;
              },
            None,
            None,
            None )
  | Protocol.Resolve_activity { seq; result } ->
      let* () = validate_sequence (path ^ ".seq") seq in
      let* result = runtime_activity_result (path ^ ".result") result in
      Ok
        (Activation.Resolve_activity { seq; result }, None, None, None, Some seq)
  | Protocol.Fire_timer { seq } ->
      let* () = validate_sequence (path ^ ".seq") seq in
      Ok (Activation.Fire_timer { seq }, None, None, None, Some seq)
  | Protocol.Cancel_workflow { reason } ->
      Ok (Activation.Cancel_workflow, None, Some reason, None, None)
  | Protocol.Remove_from_cache { message; reason } ->
      Ok
        ( Activation.Remove_from_cache,
          None,
          None,
          Some { message; reason },
          None )

(** Validates one activation through the canonical semantic codec. This keeps
    direct OCaml construction subject to the same closed-object, payload-size,
    timestamp, and ordering rules as Rust input. *)
let validate_activation value =
  match Protocol.encode_activation value with
  | Ok _ -> Ok ()
  | Error error -> Error (protocol_error error)

(** Translates the entire activation while preserving source order and all
    protocol metadata that the runtime's smaller algebra cannot carry. *)
let translate_activation (value : Protocol.activation) =
  let* () = validate_activation value in
  let seen_sequences = Hashtbl.create (List.length value.jobs) in
  let initialization = ref None in
  let cancellation_reason = ref None in
  let cache_removal = ref None in
  let rec loop index reversed = function
    | [] ->
        Ok
          {
            run_id = value.run_id;
            timestamp = value.timestamp;
            is_replaying = value.is_replaying;
            history_length = value.history_length;
            metadata = value.metadata;
            initialization = !initialization;
            cancellation_reason = !cancellation_reason;
            cache_removal = !cache_removal;
            jobs = List.rev reversed;
          }
    | job :: rest ->
        let path = Printf.sprintf "$.jobs[%d]" index in
        let* runtime, init, cancel, eviction, sequence = runtime_job path job in
        let* () =
          match sequence with
          | None -> Ok ()
          | Some sequence ->
              if Hashtbl.mem seen_sequences sequence then
                Error
                  (invalid (path ^ ".seq")
                     "duplicate activation sequence number")
              else (
                Hashtbl.add seen_sequences sequence ();
                Ok ())
        in
        begin match init with
        | None -> ()
        | Some value -> initialization := Some value
        end;
        begin match cancel with
        | None -> ()
        | Some value -> cancellation_reason := Some value
        end;
        begin match eviction with
        | None -> ()
        | Some value -> cache_removal := Some value
        end;
        loop (index + 1) (runtime :: reversed) rest
  in
  loop 0 [] value.jobs

(** Projects the checked translation to only its runtime jobs. *)
let activation_jobs value =
  Result.map (fun translated -> translated.jobs) (translate_activation value)

(** Converts milliseconds into the normalized nonnegative protocol duration
    without floating-point rounding. *)
let duration_of_milliseconds path milliseconds =
  if Int64.compare milliseconds 0L < 0 then
    Error (invalid path "timer duration must not be negative")
  else
    let seconds = Int64.div milliseconds 1_000L in
    let remainder = Int64.rem milliseconds 1_000L in
    let nanoseconds = Int64.to_int (Int64.mul remainder 1_000_000L) in
    Ok Protocol.{ seconds; nanoseconds }

(** Converts the broad runtime error into the protocol's structured failure
    shape. Category and retryability remain explicit in the application-info
    variant, while details are copied as binary-safe protocol payloads. *)
let protocol_failure path (error : Temporal_base.Error.t) =
  let view = Temporal_base.Error.view error in
  let rec details_loop reversed = function
    | [] -> Ok (List.rev reversed)
    | payload :: rest ->
        let* payload = protocol_payload (path ^ ".details") payload in
        details_loop (payload :: reversed) rest
  in
  let* details = details_loop [] view.details in
  let failure_info =
    match view.category with
    | `Cancelled -> Protocol.Canceled { details; identity = "ocaml" }
    | _ ->
        Protocol.Application
          {
            type_name = Temporal_base.Error.kind error;
            non_retryable = view.non_retryable;
            details;
          }
  in
  Ok
    Protocol.
      {
        message = view.message;
        source = "ocaml";
        stack_trace = "";
        encoded_attributes = None;
        cause = None;
        info = failure_info;
      }

(** Converts one command without fabricating fields that the current runtime
    does not own. Activity and child-workflow commands are intentionally
    rejected until their richer protocol records are wired through. *)
let command_to_protocol command =
  match command with
  | Activation.Schedule_activity { seq; name; input } ->
      let* () = validate_sequence "$.command.seq" seq in
      let* () = validate_identifier "$.command.name" name in
      let _ = input in
      Error
        (unsupported "$.command"
           "schedule_activity needs activity id, task queue, arguments, \
            timeouts, and cancellation options")
  | Activation.Start_child_workflow { seq; id; name; input } ->
      let* () = validate_sequence "$.command.seq" seq in
      let* () = validate_identifier "$.command.id" id in
      let* () = validate_identifier "$.command.name" name in
      let _ = input in
      Error
        (unsupported "$.command"
           "start_child_workflow is not represented by the current workflow \
            protocol")
  | Activation.Request_cancel_activity { seq } ->
      let* () = validate_sequence "$.command.seq" seq in
      Ok (Protocol.Request_cancel_activity { seq })
  | Activation.Start_timer { seq; milliseconds } ->
      let* () = validate_sequence "$.command.seq" seq in
      let* start_to_fire_timeout =
        duration_of_milliseconds "$.command.milliseconds" milliseconds
      in
      Ok (Protocol.Start_timer { seq; start_to_fire_timeout })
  | Activation.Cancel_timer { seq } ->
      let* () = validate_sequence "$.command.seq" seq in
      Ok (Protocol.Cancel_timer { seq })
  | Activation.Complete_workflow payload ->
      let* payload = protocol_payload "$.command.result" payload in
      let result =
        if is_null_protocol_payload payload then None else Some payload
      in
      Ok (Protocol.Complete_workflow { result })
  | Activation.Fail_workflow error ->
      let* failure = protocol_failure "$.command.failure" error in
      Ok (Protocol.Fail_workflow { failure })
  | Activation.Cancel_workflow_execution ->
      Ok Protocol.Cancel_workflow_execution

(** Converts an ordered command list and lets the canonical protocol encoder
    re-check terminal ordering, run-id bounds, payload limits, and all nested
    fields before returning it to the bridge. *)
let completion_of_commands ~run_id commands =
  let rec loop reversed = function
    | [] -> Ok (List.rev reversed)
    | command :: rest ->
        let* command = command_to_protocol command in
        loop (command :: reversed) rest
  in
  let* commands = loop [] commands in
  let completion = Protocol.{ run_id; commands } in
  match Protocol.encode_completion completion with
  | Ok _ -> Ok completion
  | Error error -> Error (protocol_error error)

(** Applies one protocol activation to a pre-created deterministic execution.
    Initialization data remains available from [translate_activation] for the
    caller that constructs the typed [Execution.t]; this function only feeds the
    runtime jobs and translates its resulting command batch. *)
let activate execution activation =
  let* translated = translate_activation activation in
  let commands = Execution.activate execution translated.jobs in
  let* completion = completion_of_commands ~run_id:translated.run_id commands in
  match translated.cache_removal with
  | Some _ when completion.commands <> [] ->
      Error
        (invalid "$.commands"
           "cache eviction must acknowledge the activation with no workflow \
            commands")
  | _ -> Ok completion
