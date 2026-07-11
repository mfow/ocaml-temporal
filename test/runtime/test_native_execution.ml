module Protocol = Temporal_protocol.Workflow_protocol
module Activation = Temporal_runtime.Activation
module Execution = Temporal_runtime.Execution
module Native_execution = Temporal_runtime.Native_execution

(** Returns an owned protocol payload containing the supplied bytes. *)
let protocol_payload text : Protocol.payload =
  { Protocol.metadata = []; data = Bytes.of_string text }

(** Returns the same bytes in the runtime payload representation. *)
let runtime_payload text : Temporal.Payload.t =
  { metadata = []; data = Bytes.of_string text }

(** Returns a unit payload in the representation used by the public codec. *)
let runtime_unit_payload () =
  match Temporal.Codec.encode Temporal.Codec.unit () with
  | Ok payload -> payload
  | Error error -> failwith (Temporal.Error.message error)

(** Fails with the stable translation diagnostic when a result was expected. *)
let unwrap label = function
  | Ok value -> value
  | Error error ->
      let view = Native_execution.error_view error in
      failwith
        (Printf.sprintf "%s: %s at %s (%s)" label view.message view.path
           view.code)

(** Creates a valid ordinary activation envelope around an ordered job list. *)
let default_timestamp : Protocol.timestamp = { seconds = 1L; nanoseconds = 0 }

let activation ?(timestamp = Some default_timestamp) ?metadata jobs :
    Protocol.activation =
  {
    run_id = "run-native-translation";
    timestamp;
    is_replaying = true;
    history_length = 7L;
    jobs;
    metadata;
  }

(** Checks that one translation error has the expected stable classification. *)
let expect_error_code label code = function
  | Ok _ -> failwith (label ^ " unexpectedly succeeded")
  | Error error ->
      let view = Native_execution.error_view error in
      if not (String.equal view.code code) then
        failwith
          (Printf.sprintf "%s returned %s instead of %s" label view.code code)

(** Confirms initialization, replay metadata, and source ordering survive the
    conversion even though the runtime job algebra uses a small start marker. *)
let test_activation_metadata_and_order () =
  let metadata =
    Some
      {
        Protocol.available_internal_flags = [ 2L; 5L ];
        history_size_bytes = "42";
        continue_as_new_suggested = false;
        deployment_version_for_current_task = None;
        last_sdk_version = "test";
        suggest_continue_as_new_reasons = [];
        target_worker_deployment_version_changed = false;
      }
  in
  let value =
    activation ?metadata
      [
        Protocol.Initialize_workflow
          {
            workflow_id = "workflow-native";
            workflow_type = "native";
            arguments = [ protocol_payload "input" ];
            randomness_seed = "1";
            attempt = 1;
            context = None;
          };
        Protocol.Resolve_activity
          {
            seq = 3L;
            result = Protocol.Completed (Some (protocol_payload "ok"));
          };
        Protocol.Fire_timer { seq = 4L };
      ]
  in
  let translated =
    unwrap "activation translation"
      (Native_execution.translate_activation value)
  in
  if
    translated.jobs
    <> [
         Activation.Start_workflow;
         Activation.Resolve_activity
           { seq = 3L; result = Ok (runtime_payload "ok") };
         Activation.Fire_timer { seq = 4L };
       ]
  then failwith "activation job order or payload conversion differed";
  if not (translated.is_replaying && translated.history_length = 7L) then
    failwith "replay metadata was not retained";
  if translated.metadata <> metadata then
    failwith "activation metadata was not retained";
  match translated.initialization with
  | Some initialization
    when initialization.workflow_id = "workflow-native"
         && initialization.workflow_type = "native"
         && initialization.arguments = [ protocol_payload "input" ] ->
      ()
  | _ -> failwith "initialization metadata was not retained"

(** Confirms that retained protocol payloads are independent of mutable input
    bytes. This guards the Domain/lifetime boundary used by a later worker loop:
    validation must not turn caller-owned buffers into shared replay state. *)
let test_retained_payloads_are_copied () =
  let argument = protocol_payload "input" in
  let header = protocol_payload "header" in
  let context : Protocol.initialize_context =
    {
      headers = [ ("request", header) ];
      identity = "worker";
      parent_workflow = None;
      workflow_execution_timeout = None;
      workflow_run_timeout = None;
      workflow_task_timeout = None;
      first_execution_run_id = "first-run";
      start_time = None;
      root_workflow = None;
      priority = None;
    }
  in
  let translated =
    unwrap "payload copy translation"
      (Native_execution.translate_activation
         (activation
            [
              Protocol.Initialize_workflow
                {
                  workflow_id = "workflow-copy";
                  workflow_type = "copy";
                  arguments = [ argument ];
                  randomness_seed = "1";
                  attempt = 1;
                  context = Some context;
                };
            ]))
  in
  Bytes.set argument.data 0 'X';
  Bytes.set header.data 0 'Y';
  match translated.initialization with
  | Some { arguments = [ argument_copy ]; context = Some context_copy; _ } ->
      let header_copy =
        match context_copy.headers with
        | [ (_, payload) ] -> payload
        | _ -> failwith "copied context header disappeared"
      in
      if Bytes.to_string argument_copy.data <> "input" then
        failwith "initialization argument retained caller-owned bytes";
      if Bytes.to_string header_copy.data <> "header" then
        failwith "initialization context retained caller-owned bytes"
  | _ -> failwith "initialization payload copies were not retained"

(** Confirms that application details survive the common Temporal shape where an
    activity failure wraps the application failure in [cause]. *)
let test_activity_failure_details_are_preserved () =
  let failure : Protocol.failure =
    {
      message = "activity wrapper";
      source = "core";
      stack_trace = "";
      encoded_attributes = None;
      cause =
        Some
          {
            message = "application failure";
            source = "worker";
            stack_trace = "";
            encoded_attributes = None;
            cause = None;
            info =
              Protocol.Application
                {
                  type_name = "mock_failure";
                  non_retryable = false;
                  details = [ protocol_payload "failure-details" ];
                };
          };
      info =
        Protocol.Activity
          {
            scheduled_event_id = 1L;
            started_event_id = 2L;
            identity = "worker";
            activity_type = "mock";
            activity_id = "activity-1";
            retry_state = Protocol.Timeout;
          };
    }
  in
  let translated =
    unwrap "activity failure translation"
      (Native_execution.translate_activation
         (activation
            [ Protocol.Resolve_activity { seq = 6L; result = Failed failure } ]))
  in
  match translated.jobs with
  | [ Activation.Resolve_activity { result = Error error; _ } ] ->
      let view = Temporal.Error.view error in
      begin match view.details with
      | [ payload ] when Bytes.to_string payload.data = "failure-details" -> ()
      | _ -> failwith "wrapped activity failure details were discarded"
      end
  | _ -> failwith "activity failure did not become a typed runtime error"

(** Confirms cancellation and cache-removal facts are retained instead of being
    silently lost by marker-only runtime jobs. *)
let test_cancellation_and_eviction () =
  let cancelled =
    activation
      [
        Protocol.Cancel_workflow { reason = "operator requested cancellation" };
      ]
  in
  let translated =
    unwrap "cancellation translation"
      (Native_execution.translate_activation cancelled)
  in
  if translated.jobs <> [ Activation.Cancel_workflow ] then
    failwith "cancellation job was not translated";
  if translated.cancellation_reason <> Some "operator requested cancellation"
  then failwith "cancellation reason was discarded";
  let eviction =
    activation ~timestamp:None
      [
        Protocol.Remove_from_cache
          { message = "cache pressure"; reason = Protocol.Cache_full };
      ]
  in
  let translated =
    unwrap "eviction translation"
      (Native_execution.translate_activation eviction)
  in
  if translated.jobs <> [ Activation.Remove_from_cache ] then
    failwith "eviction marker was not translated";
  match translated.cache_removal with
  | Some
      {
        Native_execution.message = "cache pressure";
        reason = Protocol.Cache_full;
      } ->
      ()
  | _ -> failwith "eviction details were discarded"

(** A completed activation through the runtime produces the protocol's nullable
    result for the canonical unit payload. *)
let test_activate_terminal_completion () =
  let workflow =
    Temporal.Workflow.define ~name:"native_terminal" ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit (fun () -> Ok ())
  in
  let execution = Execution.start workflow () in
  let completion =
    unwrap "terminal activation"
      (Native_execution.activate execution
         (activation
            [
              Protocol.Initialize_workflow
                {
                  workflow_id = "workflow-native";
                  workflow_type = "native_terminal";
                  arguments = [];
                  randomness_seed = "1";
                  attempt = 1;
                  context = None;
                };
            ]))
  in
  match completion.commands with
  | [ Protocol.Complete_workflow { result = None } ] -> ()
  | _ -> failwith "unit workflow did not produce nullable protocol completion"

(** Cache eviction acknowledges Core without running or emitting workflow
    commands, while retaining the exact run identity. *)
let test_activate_eviction_completion () =
  let workflow =
    Temporal.Workflow.define ~name:"native_eviction" ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit (fun () -> Ok ())
  in
  let execution = Execution.start workflow () in
  let completion =
    unwrap "eviction activation"
      (Native_execution.activate execution
         (activation ~timestamp:None
            [
              Protocol.Remove_from_cache
                { message = "cache miss"; reason = Protocol.Cache_miss };
            ]))
  in
  if completion.run_id <> "run-native-translation" || completion.commands <> []
  then failwith "eviction completion did not preserve the empty acknowledgement"

(** Confirms timer conversion keeps exact millisecond ordering and rejects
    negative or out-of-range sequence values without exceptions. *)
let test_command_order_and_validation () =
  let commands =
    Native_execution.completion_of_commands ~run_id:"run-native-translation"
      [
        Activation.Start_timer { seq = 9L; milliseconds = 1_234L };
        Activation.Cancel_timer { seq = 9L };
        Activation.Cancel_workflow_execution;
      ]
  in
  let completion = unwrap "timer command conversion" commands in
  begin match completion.commands with
  | [
   Protocol.Start_timer
     {
       seq = 9L;
       start_to_fire_timeout = { seconds = 1L; nanoseconds = 234_000_000 };
     };
   Protocol.Cancel_timer { seq = 9L };
   Protocol.Cancel_workflow_execution;
  ] ->
      ()
  | _ -> failwith "command order or duration conversion differed"
  end;
  expect_error_code "negative timer" "invalid_message"
    (Native_execution.command_to_protocol
       (Activation.Start_timer { seq = 1L; milliseconds = -1L }));
  expect_error_code "too-large sequence" "invalid_message"
    (Native_execution.command_to_protocol
       (Activation.Cancel_timer { seq = 4_294_967_296L }))

(** Activity commands retain every Core-required field through translation, and
    malformed timeout/identity options fail before a completion is returned. *)
let test_activity_command_translation_and_validation () =
  let input = runtime_unit_payload () in
  let command =
    Activation.Schedule_activity
      {
        seq = 1L;
        activity_id = "lookup-1";
        activity_type = "lookup";
        task_queue = "activities";
        arguments = [ input ];
        schedule_to_close_timeout = Some 30_000L;
        schedule_to_start_timeout = None;
        start_to_close_timeout = Some 10_000L;
        heartbeat_timeout = None;
        cancellation_type = Activation.Wait_cancellation_completed;
        do_not_eagerly_execute = true;
      }
  in
  let translated =
    unwrap "activity command translation"
      (Native_execution.command_to_protocol command)
  in
  begin match translated with
  | Protocol.Schedule_activity
      {
        seq = 1L;
        activity_id = "lookup-1";
        activity_type = "lookup";
        task_queue = "activities";
        arguments = [ argument ];
        schedule_to_close_timeout =
          Some { seconds = 30L; nanoseconds = 0 };
        schedule_to_start_timeout = None;
        start_to_close_timeout = Some { seconds = 10L; nanoseconds = 0 };
        heartbeat_timeout = None;
        cancellation_type = Protocol.Wait_cancellation_completed;
        do_not_eagerly_execute = true;
      }
    when argument =
      {
        Protocol.metadata = [ ("encoding", Bytes.of_string "binary/null") ];
        data = Bytes.empty;
      } ->
      ()
  | _ -> failwith "activity command fields were not preserved"
  end;
  expect_error_code "activity timeout requirement" "invalid_message"
    (Native_execution.command_to_protocol
       (Activation.Schedule_activity
          {
            seq = 2L;
            activity_id = "lookup-2";
            activity_type = "lookup";
            task_queue = "activities";
            arguments = [ input ];
            schedule_to_close_timeout = None;
            schedule_to_start_timeout = None;
            start_to_close_timeout = None;
            heartbeat_timeout = None;
            cancellation_type = Activation.Try_cancel;
            do_not_eagerly_execute = false;
          }));
  expect_error_code "negative activity timeout" "invalid_message"
    (Native_execution.command_to_protocol
       (Activation.Schedule_activity
          {
            seq = 3L;
            activity_id = "lookup-3";
            activity_type = "lookup";
            task_queue = "activities";
            arguments = [ input ];
            schedule_to_close_timeout = None;
            schedule_to_start_timeout = None;
            start_to_close_timeout = Some (-1L);
            heartbeat_timeout = None;
            cancellation_type = Activation.Try_cancel;
            do_not_eagerly_execute = false;
          }));
  expect_error_code "empty activity queue" "invalid_message"
    (Native_execution.command_to_protocol
       (Activation.Schedule_activity
          {
            seq = 4L;
            activity_id = "lookup-4";
            activity_type = "lookup";
            task_queue = "";
            arguments = [ input ];
            schedule_to_close_timeout = Some 1L;
            schedule_to_start_timeout = None;
            start_to_close_timeout = None;
            heartbeat_timeout = None;
            cancellation_type = Activation.Try_cancel;
            do_not_eagerly_execute = false;
          }));
  expect_error_code "invalid UTF-8 activity queue" "invalid_message"
    (Native_execution.command_to_protocol
       (Activation.Schedule_activity
          {
            seq = 5L;
            activity_id = "lookup-5";
            activity_type = "lookup";
            task_queue = String.make 1 (Char.chr 0xff);
            arguments = [ input ];
            schedule_to_close_timeout = Some 1L;
            schedule_to_start_timeout = None;
            start_to_close_timeout = None;
            heartbeat_timeout = None;
            cancellation_type = Activation.Try_cancel;
            do_not_eagerly_execute = false;
          }));
  expect_error_code "overlong activity ID" "invalid_message"
    (Native_execution.command_to_protocol
       (Activation.Schedule_activity
          {
            seq = 6L;
            activity_id = String.make 65_537 'x';
            activity_type = "lookup";
            task_queue = "activities";
            arguments = [ input ];
            schedule_to_close_timeout = Some 1L;
            schedule_to_start_timeout = None;
            start_to_close_timeout = None;
            heartbeat_timeout = None;
            cancellation_type = Activation.Try_cancel;
            do_not_eagerly_execute = false;
          }));
  (* Child workflows remain unsupported until the semantic protocol gains the
     complete Core command record; retaining this assertion prevents an
     accidental lossy conversion while activity support evolves. *)
  expect_error_code "child command" "unsupported"
    (Native_execution.command_to_protocol
       (Activation.Start_child_workflow
          { seq = 2L; id = "child/1"; name = "child"; input }));
  let invalid_metadata : Temporal.Payload.t =
    { metadata = [ ("encoding", "\255") ]; data = Bytes.empty }
  in
  expect_error_code "binary metadata" "invalid_message"
    (Native_execution.command_to_protocol
       (Activation.Complete_workflow invalid_metadata))

(** Duplicate sequence numbers in one protocol activation are rejected before an
    execution can be mutated. *)
let test_duplicate_sequence_rejected () =
  expect_error_code "duplicate sequence" "invalid_message"
    (Native_execution.translate_activation
       (activation
          [
            Protocol.Fire_timer { seq = 5L };
            Protocol.Resolve_activity
              { seq = 5L; result = Protocol.Completed None };
          ]))

(** An unknown sequence delivered to an already-created runtime becomes one
    typed terminal bridge failure, preserving Core's completion semantics. *)
let test_unknown_sequence_becomes_failure () =
  let workflow =
    Temporal.Workflow.define ~name:"native_unknown" ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit (fun () -> Ok ())
  in
  let execution = Execution.start workflow () in
  let completion =
    unwrap "unknown sequence activation"
      (Native_execution.activate execution
         (activation [ Protocol.Fire_timer { seq = 99L } ]))
  in
  match completion.commands with
  | [
   Protocol.Fail_workflow { failure = { info = Protocol.Application _; _ } };
  ] ->
      ()
  | _ -> failwith "unknown sequence did not become a protocol failure"

(** Runs every native-execution translation assertion. *)
let () =
  test_activation_metadata_and_order ();
  test_retained_payloads_are_copied ();
  test_activity_failure_details_are_preserved ();
  test_cancellation_and_eviction ();
  test_activate_terminal_completion ();
  test_activate_eviction_completion ();
  test_command_order_and_validation ();
  test_activity_command_translation_and_validation ();
  test_duplicate_sequence_rejected ();
  test_unknown_sequence_becomes_failure ()
