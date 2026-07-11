module Control = Control_protocol

type payload = { metadata : (string * bytes) list; data : bytes }
type timestamp = { seconds : int64; nanoseconds : int }
type duration = { seconds : int64; nanoseconds : int }
type workflow_execution = { workflow_id : string; run_id : string }
type namespaced_workflow_execution = { namespace : string; workflow_id : string; run_id : string }
type workflow_priority = { priority_key : int; fairness_key : string; fairness_weight_bits : int64 }

type initialize_context = {
  headers : (string * payload) list;
  identity : string;
  parent_workflow : namespaced_workflow_execution option;
  workflow_execution_timeout : duration option;
  workflow_run_timeout : duration option;
  workflow_task_timeout : duration option;
  first_execution_run_id : string;
  start_time : timestamp option;
  root_workflow : workflow_execution option;
  priority : workflow_priority option;
}

type worker_deployment_version = { deployment_name : string; build_id : string }

type suggest_continue_as_new_reason =
  | Suggest_unspecified
  | History_size_too_large
  | Too_many_history_events
  | Too_many_updates

type activation_metadata = {
  available_internal_flags : int64 list;
  history_size_bytes : string;
  continue_as_new_suggested : bool;
  deployment_version_for_current_task : worker_deployment_version option;
  last_sdk_version : string;
  suggest_continue_as_new_reasons : suggest_continue_as_new_reason list;
  target_worker_deployment_version_changed : bool;
}

type retry_state =
  | Unspecified
  | In_progress
  | Non_retryable_failure
  | Timeout
  | Maximum_attempts_reached
  | Retry_policy_not_set
  | Internal_server_error
  | Cancel_requested

type failure_info =
  | Application of {
      type_name : string;
      non_retryable : bool;
      details : payload list;
    }
  | Canceled of { details : payload list; identity : string }
  | Activity of {
      scheduled_event_id : int64;
      started_event_id : int64;
      identity : string;
      activity_type : string;
      activity_id : string;
      retry_state : retry_state;
    }

type failure = {
  message : string;
  source : string;
  stack_trace : string;
  encoded_attributes : payload option;
  cause : failure option;
  info : failure_info;
}

type activity_resolution =
  | Completed of payload option
  | Failed of failure
  | Cancelled of failure

type eviction_reason =
  | Eviction_unspecified
  | Cache_full
  | Cache_miss
  | Nondeterminism
  | Lang_fail
  | Lang_requested
  | Task_not_found
  | Unhandled_command
  | Fatal
  | Pagination_or_history_fetch
  | Workflow_execution_ending

type activation_job =
  | Initialize_workflow of {
      workflow_id : string;
      workflow_type : string;
      arguments : payload list;
      randomness_seed : string;
      attempt : int;
      context : initialize_context option;
    }
  | Resolve_activity of { seq : int64; result : activity_resolution }
  | Fire_timer of { seq : int64 }
  | Cancel_workflow of { reason : string }
  | Remove_from_cache of { message : string; reason : eviction_reason }

type activation = {
  run_id : string;
  timestamp : timestamp option;
  is_replaying : bool;
  history_length : int64;
  jobs : activation_job list;
  metadata : activation_metadata option;
}

type activity_cancellation_type =
  | Try_cancel
  | Wait_cancellation_completed
  | Abandon

type completion_command =
  | Schedule_activity of {
      seq : int64;
      activity_id : string;
      activity_type : string;
      task_queue : string;
      arguments : payload list;
      schedule_to_close_timeout : duration option;
      schedule_to_start_timeout : duration option;
      start_to_close_timeout : duration option;
      heartbeat_timeout : duration option;
      cancellation_type : activity_cancellation_type;
      do_not_eagerly_execute : bool;
    }
  | Request_cancel_activity of { seq : int64 }
  | Start_timer of { seq : int64; start_to_fire_timeout : duration }
  | Cancel_timer of { seq : int64 }
  | Complete_workflow of { result : payload option }
  | Fail_workflow of { failure : failure }
  | Cancel_workflow_execution

type completion = { run_id : string; commands : completion_command list }
type error = { code : string; path : string; message : string }
type error_view = { code : string; path : string; message : string }

(** Copies a semantic error without exposing its representation. *)
let error_view (error : error) : error_view =
  { code = error.code; path = error.path; message = error.message }

(** Constructs a privacy-safe semantic validation failure. *)
let invalid path message : error = { code = "invalid_message"; path; message }

(** Converts a foundation error while retaining only its safe diagnostic. *)
let of_control_error path error : error =
  let view = Control.error_view error in
  { code = view.code; path; message = view.message }

(** Sequences semantic validation without exceptions. *)
let ( let* ) = Result.bind

(** Matches the control protocol's ordinary decoded-string safety ceiling.
    Temporal identifier limits are server-configurable, so the bridge imposes
    no narrower identifier policy of its own. *)
let protocol_string_safety_limit = 65_536

(** Requires a closed object before individual fields are read. *)
let exact_object path expected = function
  | `Assoc entries
    when List.length entries = List.length expected
         && List.for_all (fun (name, _) -> List.mem name expected) entries ->
      Ok entries
  | `Assoc _ -> Error (invalid path "object has missing or unknown fields")
  | _ -> Error (invalid path "expected JSON object")

(** Reads one already-required object member. *)
let field path name entries =
  match List.assoc_opt name entries with
  | Some value -> Ok value
  | None -> Error (invalid path ("missing required field " ^ name))

(** Reads a JSON string. *)
let string path = function
  | `String value when String.length value <= 65_536 -> Ok value
  | `String _ -> Error (invalid path "decoded JSON string limit exceeded")
  | _ -> Error (invalid path "expected JSON string")

(** Reads a JSON Boolean. *)
let bool path = function
  | `Bool value -> Ok value
  | _ -> Error (invalid path "expected JSON boolean")

(** Reads an integral JSON value into the common signed 64-bit range. *)
let int64 path = function
  | `Int value -> Ok (Int64.of_int value)
  | `Intlit value -> (
      try Ok (Int64.of_string value)
      with _ -> Error (invalid path "integer is outside signed 64-bit range"))
  | _ -> Error (invalid path "expected JSON integer")

(** Reads a protobuf event identifier, whose signed wire representation must
    never carry a negative service event number. *)
let nonnegative_int64 path value =
  let* value = int64 path value in
  if Int64.compare value 0L < 0 then
    Error (invalid path "event identifier must not be negative")
  else Ok value

(** Reads an unsigned 32-bit protobuf number without narrowing it to OCaml
    [int] on platforms where that could be unsafe. *)
let uint32 path value =
  let* value = int64 path value in
  if Int64.compare value 0L < 0 || Int64.compare value 4_294_967_295L > 0 then
    Error (invalid path "integer is outside unsigned 32-bit range")
  else Ok value

(** Reads a bounded positive protobuf [int32]. *)
let positive_int32 path value =
  let* value = int64 path value in
  if Int64.compare value 1L < 0 || Int64.compare value 2_147_483_647L > 0 then
    Error (invalid path "integer is outside positive signed 32-bit range")
  else Ok (Int64.to_int value)

(** Reads a signed protobuf [int32] without accepting a wider JSON integer. *)
let int32 path value =
  let* value = int64 path value in
  if Int64.compare value (-2_147_483_648L) < 0 || Int64.compare value 2_147_483_647L > 0 then
    Error (invalid path "integer is outside signed 32-bit range")
  else Ok (Int64.to_int value)

(** Requires a non-empty bounded identifier without imposing undocumented
    character restrictions on valid Temporal names. *)
let identifier path value =
  let* value = string path value in
  if String.length value = 0 || String.length value > protocol_string_safety_limit then
    Error (invalid path "identifier is empty or exceeds the protocol string safety limit")
  else Ok value

(** Validates a canonical unsigned 64-bit decimal representation. *)
let uint64_decimal path value =
  let* value = string path value in
  let maximum = "18446744073709551615" in
  if
    String.length value = 0
    || (String.length value > 1 && Char.equal value.[0] '0')
    || not (String.for_all (function '0' .. '9' -> true | _ -> false) value)
    || String.length value > String.length maximum
    || (String.length value = String.length maximum
       && String.compare value maximum > 0)
  then Error (invalid path "value is not canonical unsigned 64-bit decimal")
  else Ok value

(** Decodes a list while adding a stable index to nested error paths. *)
let list path decode = function
  | `List values ->
      let rec loop index decoded = function
        | [] -> Ok (List.rev decoded)
        | value :: rest ->
            let* value = decode (Printf.sprintf "%s[%d]" path index) value in
            loop (index + 1) (value :: decoded) rest
      in
      loop 0 [] values
  | _ -> Error (invalid path "expected JSON array")

(** Decodes nullable JSON with the supplied non-null decoder. *)
let nullable path decode = function
  | `Null -> Ok None
  | value ->
      let* value = decode path value in
      Ok (Some value)

(** Decodes one canonical base64 wrapper by delegating to the shared binary
    validator. *)
let bytes_wrapper path json =
  match Control.decode_payload (Yojson.Safe.to_string json) with
  | Ok value -> Ok value
  | Error error -> Error (of_control_error path error)

(** Builds the canonical base64 wrapper produced by the shared validator. *)
let bytes_wrapper_json bytes =
  match Control.encode_payload bytes with
  | Error error -> Error (of_control_error "$" error)
  | Ok encoded -> (
      match Control.decode_payload_object encoded with
      | Ok json -> Ok json
      | Error error -> Error (of_control_error "$" error))

(** Decodes one binary-safe Temporal payload, including arbitrary binary
    metadata values. *)
let payload path json =
  let* entries = exact_object path [ "metadata"; "data" ] json in
  let* metadata_json = field path "metadata" entries in
  let* metadata_entries =
    match metadata_json with
    | `Assoc entries -> Ok entries
    | _ -> Error (invalid (path ^ ".metadata") "expected JSON object")
  in
  let rec decode_metadata decoded = function
    | [] -> Ok (List.rev decoded)
    | (key, value) :: rest ->
        if String.length key = 0 || String.length key > 65_536 then
          Error (invalid (path ^ ".metadata") "metadata key length is outside protocol limits")
        else
          let* bytes = bytes_wrapper (path ^ ".metadata." ^ key) value in
          decode_metadata ((key, bytes) :: decoded) rest
  in
  let* metadata = decode_metadata [] metadata_entries in
  let metadata =
    List.sort (fun (left, _) (right, _) -> String.compare left right) metadata
  in
  let* data_json = field path "data" entries in
  let* data = bytes_wrapper (path ^ ".data") data_json in
  Ok { metadata; data }

(** Encodes one payload without changing metadata key or byte content. *)
let payload_json (value : payload) =
  let rec metadata_json encoded = function
    | [] -> Ok (`Assoc (List.rev encoded))
    | (key, bytes) :: rest ->
        if String.length key = 0 || String.length key > 65_536 then
          Error (invalid "$.metadata" "metadata key length is outside protocol limits")
        else if List.exists (fun (existing, _) -> String.equal existing key) encoded
        then Error (invalid "$.metadata" "duplicate metadata key")
        else
          let* wrapped = bytes_wrapper_json bytes in
          metadata_json ((key, wrapped) :: encoded) rest
  in
  let* metadata = metadata_json [] value.metadata in
  let* data = bytes_wrapper_json value.data in
  Ok (`Assoc [ ("metadata", metadata); ("data", data) ])

(** Decodes a protobuf timestamp without applying wall-clock semantics. *)
let timestamp path json : (timestamp, error) result =
  let* entries = exact_object path [ "seconds"; "nanoseconds" ] json in
  let* seconds_json = field path "seconds" entries in
  let* seconds = int64 (path ^ ".seconds") seconds_json in
  let* nanos_json = field path "nanoseconds" entries in
  let* nanos = int64 (path ^ ".nanoseconds") nanos_json in
  if Int64.compare nanos 0L < 0 || Int64.compare nanos 999_999_999L > 0 then
    Error (invalid (path ^ ".nanoseconds") "nanoseconds are outside protobuf range")
  else
    let value : timestamp = { seconds; nanoseconds = Int64.to_int nanos } in
    Ok value

(** Decodes a normalized nonnegative protobuf duration. *)
let duration path json : (duration, error) result =
  let* value = timestamp path json in
  if Int64.compare value.seconds 0L < 0 then
    Error (invalid (path ^ ".seconds") "duration must not be negative")
  else
    let duration : duration =
      { seconds = value.seconds; nanoseconds = value.nanoseconds }
    in
    Ok duration

(** Encodes seconds/nanoseconds with no floating-point conversion. *)
let time_json seconds nanoseconds =
  `Assoc
    [
      ("seconds", `Intlit (Int64.to_string seconds));
      ("nanoseconds", `Int nanoseconds);
    ]

(** Maps the stable retry-state spelling used by the JSON contract. *)
let retry_state path = function
  | "unspecified" -> Ok Unspecified
  | "in_progress" -> Ok In_progress
  | "non_retryable_failure" -> Ok Non_retryable_failure
  | "timeout" -> Ok Timeout
  | "maximum_attempts_reached" -> Ok Maximum_attempts_reached
  | "retry_policy_not_set" -> Ok Retry_policy_not_set
  | "internal_server_error" -> Ok Internal_server_error
  | "cancel_requested" -> Ok Cancel_requested
  | _ -> Error (invalid path "unknown retry state")

(** Renders a retry state using the stable lowercase contract spelling. *)
let retry_state_string = function
  | Unspecified -> "unspecified"
  | In_progress -> "in_progress"
  | Non_retryable_failure -> "non_retryable_failure"
  | Timeout -> "timeout"
  | Maximum_attempts_reached -> "maximum_attempts_reached"
  | Retry_policy_not_set -> "retry_policy_not_set"
  | Internal_server_error -> "internal_server_error"
  | Cancel_requested -> "cancel_requested"

(** Decodes the supported closed failure-info union. *)
let failure_info path json =
  let* entries =
    match json with
    | `Assoc entries -> Ok entries
    | _ -> Error (invalid path "expected JSON object")
  in
  let* kind_json = field path "kind" entries in
  let* kind = string (path ^ ".kind") kind_json in
  match kind with
  | "application" ->
      let* entries =
        exact_object path [ "kind"; "type"; "non_retryable"; "details" ] json
      in
      let* type_json = field path "type" entries in
      let* type_name = string (path ^ ".type") type_json in
      let* non_retryable_json = field path "non_retryable" entries in
      let* non_retryable = bool (path ^ ".non_retryable") non_retryable_json in
      let* details_json = field path "details" entries in
      let* details = list (path ^ ".details") payload details_json in
      Ok (Application { type_name; non_retryable; details })
  | "canceled" ->
      let* entries = exact_object path [ "kind"; "details"; "identity" ] json in
      let* details_json = field path "details" entries in
      let* details = list (path ^ ".details") payload details_json in
      let* identity_json = field path "identity" entries in
      let* identity = string (path ^ ".identity") identity_json in
      Ok (Canceled { details; identity })
  | "activity" ->
      let* entries =
        exact_object path
          [
            "kind";
            "scheduled_event_id";
            "started_event_id";
            "identity";
            "activity_type";
            "activity_id";
            "retry_state";
          ]
          json
      in
      let* scheduled_json = field path "scheduled_event_id" entries in
      let* scheduled_event_id =
        nonnegative_int64 (path ^ ".scheduled_event_id") scheduled_json
      in
      let* started_json = field path "started_event_id" entries in
      let* started_event_id =
        nonnegative_int64 (path ^ ".started_event_id") started_json
      in
      let* identity_json = field path "identity" entries in
      let* identity = string (path ^ ".identity") identity_json in
      let* activity_type_json = field path "activity_type" entries in
      let* activity_type = identifier (path ^ ".activity_type") activity_type_json in
      let* activity_id_json = field path "activity_id" entries in
      let* activity_id = identifier (path ^ ".activity_id") activity_id_json in
      let* retry_json = field path "retry_state" entries in
      let* retry_name = string (path ^ ".retry_state") retry_json in
      let* retry_state = retry_state (path ^ ".retry_state") retry_name in
      Ok
        (Activity
           {
             scheduled_event_id;
             started_event_id;
             identity;
             activity_type;
             activity_id;
             retry_state;
           })
  | _ -> Error (invalid (path ^ ".kind") "unsupported failure info kind")

(** Decodes a recursive failure. JSON depth limits bound cause recursion before
    this function executes. *)
let rec failure path json =
  let* entries =
    exact_object path
      [ "message"; "source"; "stack_trace"; "encoded_attributes"; "cause"; "info" ]
      json
  in
  let* message_json = field path "message" entries in
  let* message = string (path ^ ".message") message_json in
  let* source_json = field path "source" entries in
  let* source = string (path ^ ".source") source_json in
  let* stack_json = field path "stack_trace" entries in
  let* stack_trace = string (path ^ ".stack_trace") stack_json in
  let* encoded_json = field path "encoded_attributes" entries in
  let* encoded_attributes = nullable (path ^ ".encoded_attributes") payload encoded_json in
  let* cause_json = field path "cause" entries in
  let* cause = nullable (path ^ ".cause") failure cause_json in
  let* info_json = field path "info" entries in
  let* info = failure_info (path ^ ".info") info_json in
  Ok { message; source; stack_trace; encoded_attributes; cause; info }

(** Encodes the supported failure-info union. *)
let rec failure_info_json = function
  | Application { type_name; non_retryable; details } ->
      let* details = payloads_json details in
      Ok
        (`Assoc
          [
            ("kind", `String "application");
            ("type", `String type_name);
            ("non_retryable", `Bool non_retryable);
            ("details", details);
          ])
  | Canceled { details; identity } ->
      let* details = payloads_json details in
      Ok
        (`Assoc
          [
            ("kind", `String "canceled");
            ("details", details);
            ("identity", `String identity);
          ])
  | Activity
      {
        scheduled_event_id;
        started_event_id;
        identity;
        activity_type;
        activity_id;
        retry_state;
      } ->
      Ok
        (`Assoc
          [
            ("kind", `String "activity");
            ("scheduled_event_id", `Intlit (Int64.to_string scheduled_event_id));
            ("started_event_id", `Intlit (Int64.to_string started_event_id));
            ("identity", `String identity);
            ("activity_type", `String activity_type);
            ("activity_id", `String activity_id);
            ("retry_state", `String (retry_state_string retry_state));
          ])

(** Encodes a payload list in source order. *)
and payloads_json values =
  let rec loop encoded = function
    | [] -> Ok (`List (List.rev encoded))
    | value :: rest ->
        let* value = payload_json value in
        loop (value :: encoded) rest
  in
  loop [] values

(** Encodes a recursive failure without exposing it to generic serializers. *)
and failure_json value =
  let* encoded_attributes =
    match value.encoded_attributes with
    | None -> Ok `Null
    | Some value -> payload_json value
  in
  let* cause =
    match value.cause with None -> Ok `Null | Some value -> failure_json value
  in
  let* info = failure_info_json value.info in
  Ok
    (`Assoc
      [
        ("message", `String value.message);
        ("source", `String value.source);
        ("stack_trace", `String value.stack_trace);
        ("encoded_attributes", encoded_attributes);
        ("cause", cause);
        ("info", info);
      ])

(** Decodes the activity-resolution oneof. *)
let activity_resolution path json =
  let* entries =
    match json with
    | `Assoc entries -> Ok entries
    | _ -> Error (invalid path "expected JSON object")
  in
  let* kind_json = field path "kind" entries in
  let* kind = string (path ^ ".kind") kind_json in
  match kind with
  | "completed" ->
      let* entries = exact_object path [ "kind"; "payload" ] json in
      let* payload_json = field path "payload" entries in
      let* value = nullable (path ^ ".payload") payload payload_json in
      Ok (Completed value)
  | "failed" | "cancelled" ->
      let* entries = exact_object path [ "kind"; "failure" ] json in
      let* failure_json = field path "failure" entries in
      let* value = failure (path ^ ".failure") failure_json in
      if String.equal kind "failed" then Ok (Failed value) else Ok (Cancelled value)
  | _ -> Error (invalid (path ^ ".kind") "unknown activity resolution kind")

(** Encodes the activity-resolution oneof. *)
let activity_resolution_json = function
  | Completed value ->
      let* payload =
        match value with None -> Ok `Null | Some value -> payload_json value
      in
      Ok (`Assoc [ ("kind", `String "completed"); ("payload", payload) ])
  | Failed value ->
      let* failure = failure_json value in
      Ok (`Assoc [ ("kind", `String "failed"); ("failure", failure) ])
  | Cancelled value ->
      let* failure = failure_json value in
      Ok (`Assoc [ ("kind", `String "cancelled"); ("failure", failure) ])

(** Maps a stable cache-eviction spelling. *)
let eviction_reason path = function
  | "unspecified" -> Ok Eviction_unspecified
  | "cache_full" -> Ok Cache_full
  | "cache_miss" -> Ok Cache_miss
  | "nondeterminism" -> Ok Nondeterminism
  | "lang_fail" -> Ok Lang_fail
  | "lang_requested" -> Ok Lang_requested
  | "task_not_found" -> Ok Task_not_found
  | "unhandled_command" -> Ok Unhandled_command
  | "fatal" -> Ok Fatal
  | "pagination_or_history_fetch" -> Ok Pagination_or_history_fetch
  | "workflow_execution_ending" -> Ok Workflow_execution_ending
  | _ -> Error (invalid path "unknown cache eviction reason")

(** Renders a cache-eviction reason. *)
let eviction_reason_string = function
  | Eviction_unspecified -> "unspecified"
  | Cache_full -> "cache_full"
  | Cache_miss -> "cache_miss"
  | Nondeterminism -> "nondeterminism"
  | Lang_fail -> "lang_fail"
  | Lang_requested -> "lang_requested"
  | Task_not_found -> "task_not_found"
  | Unhandled_command -> "unhandled_command"
  | Fatal -> "fatal"
  | Pagination_or_history_fetch -> "pagination_or_history_fetch"
  | Workflow_execution_ending -> "workflow_execution_ending"

(** Decodes a canonical object map whose values are Temporal payloads. *)
let payload_map path = function
  | `Assoc entries ->
      let rec loop decoded = function
        | [] -> Ok (List.rev decoded)
        | (key, value) :: rest ->
            if String.length key = 0 || String.length key > 65_536 then
              Error (invalid path "invalid payload-map key")
            else
              let* value = payload (path ^ "." ^ key) value in
              loop ((key, value) :: decoded) rest
      in
      let* values = loop [] entries in
      Ok (List.sort (fun (left, _) (right, _) -> String.compare left right) values)
  | _ -> Error (invalid path "expected JSON object")

(** Encodes a payload map and rejects duplicate keys before canonical sorting. *)
let payload_map_json path values =
  let rec loop encoded = function
    | [] -> Ok (`Assoc (List.rev encoded))
    | (key, value) :: rest ->
        if
          String.length key = 0
          || String.length key > 65_536
          || List.exists (fun (existing, _) -> String.equal existing key) encoded
        then Error (invalid path "invalid or duplicate payload-map key")
        else
          let* value = payload_json value in
          loop ((key, value) :: encoded) rest
  in
  loop [] values

(** Decodes a workflow/run pair used for a child workflow's root identity. *)
let workflow_execution path json =
  let* entries = exact_object path [ "workflow_id"; "run_id" ] json in
  let* workflow_id_json = field path "workflow_id" entries in
  let* workflow_id = identifier (path ^ ".workflow_id") workflow_id_json in
  let* run_id_json = field path "run_id" entries in
  let* run_id = identifier (path ^ ".run_id") run_id_json in
  Ok { workflow_id; run_id }

(** Decodes a parent workflow identity including its namespace. *)
let namespaced_workflow_execution path json =
  let* entries = exact_object path [ "namespace"; "workflow_id"; "run_id" ] json in
  let* namespace_json = field path "namespace" entries in
  let* namespace = identifier (path ^ ".namespace") namespace_json in
  let* workflow_id_json = field path "workflow_id" entries in
  let* workflow_id = identifier (path ^ ".workflow_id") workflow_id_json in
  let* run_id_json = field path "run_id" entries in
  let* run_id = identifier (path ^ ".run_id") run_id_json in
  Ok { namespace; workflow_id; run_id }

(** Decodes exact workflow priority, retaining the raw [f32] weight bits. *)
let workflow_priority path json =
  let* entries = exact_object path [ "priority_key"; "fairness_key"; "fairness_weight_bits" ] json in
  let* key_json = field path "priority_key" entries in
  let* priority_key = int32 (path ^ ".priority_key") key_json in
  let* fairness_json = field path "fairness_key" entries in
  let* fairness_key = string (path ^ ".fairness_key") fairness_json in
  if String.length fairness_key > 64 then Error (invalid (path ^ ".fairness_key") "fairness key exceeds Core's 64-byte limit")
  else
    let* bits_json = field path "fairness_weight_bits" entries in
    let* fairness_weight_bits = uint32 (path ^ ".fairness_weight_bits") bits_json in
    Ok { priority_key; fairness_key; fairness_weight_bits }

(** Decodes the normal fields attached to a first workflow initialization. *)
let initialize_context path json =
  let fields =
    [
      "headers";
      "identity";
      "parent_workflow";
      "workflow_execution_timeout";
      "workflow_run_timeout";
      "workflow_task_timeout";
      "first_execution_run_id";
      "start_time";
      "root_workflow";
      "priority";
    ]
  in
  let* entries = exact_object path fields json in
  let* headers_json = field path "headers" entries in
  let* headers = payload_map (path ^ ".headers") headers_json in
  let* identity_json = field path "identity" entries in
  let* identity = string (path ^ ".identity") identity_json in
  let* parent_json = field path "parent_workflow" entries in
  let* parent_workflow = nullable (path ^ ".parent_workflow") namespaced_workflow_execution parent_json in
  let timeout name =
    let* value = field path name entries in
    nullable (path ^ "." ^ name) duration value
  in
  let* workflow_execution_timeout = timeout "workflow_execution_timeout" in
  let* workflow_run_timeout = timeout "workflow_run_timeout" in
  let* workflow_task_timeout = timeout "workflow_task_timeout" in
  let* first_json = field path "first_execution_run_id" entries in
  let* first_execution_run_id = identifier (path ^ ".first_execution_run_id") first_json in
  let* start_json = field path "start_time" entries in
  let* start_time = nullable (path ^ ".start_time") timestamp start_json in
  let* root_json = field path "root_workflow" entries in
  let* root_workflow = nullable (path ^ ".root_workflow") workflow_execution root_json in
  let* priority_json = field path "priority" entries in
  let* priority = nullable (path ^ ".priority") workflow_priority priority_json in
  Ok
    {
      headers;
      identity;
      parent_workflow;
      workflow_execution_timeout;
      workflow_run_timeout;
      workflow_task_timeout;
      first_execution_run_id;
      start_time;
      root_workflow;
      priority;
    }

(** Encodes a first-workflow initialization context. *)
let initialize_context_json value =
  let* headers = payload_map_json "$.context.headers" value.headers in
  let optional_time = function
    | None -> `Null
    | Some value -> time_json value.seconds value.nanoseconds
  in
  let parent_workflow = match value.parent_workflow with
    | None -> `Null
    | Some value -> `Assoc [ ("namespace", `String value.namespace); ("workflow_id", `String value.workflow_id); ("run_id", `String value.run_id) ]
  in
  let root_workflow = match value.root_workflow with
    | None -> `Null
    | Some value -> `Assoc [ ("workflow_id", `String value.workflow_id); ("run_id", `String value.run_id) ]
  in
  let priority = match value.priority with
    | None -> `Null
    | Some value -> `Assoc [ ("priority_key", `Int value.priority_key); ("fairness_key", `String value.fairness_key); ("fairness_weight_bits", `Intlit (Int64.to_string value.fairness_weight_bits)) ]
  in
  Ok
    (`Assoc
      [
        ("headers", headers);
        ("identity", `String value.identity);
        ("parent_workflow", parent_workflow);
        ("workflow_execution_timeout", optional_time value.workflow_execution_timeout);
        ("workflow_run_timeout", optional_time value.workflow_run_timeout);
        ("workflow_task_timeout", optional_time value.workflow_task_timeout);
        ("first_execution_run_id", `String value.first_execution_run_id);
        ( "start_time",
          match value.start_time with
          | None -> `Null
          | Some value -> time_json value.seconds value.nanoseconds );
        ("root_workflow", root_workflow);
        ("priority", priority);
      ])

(** Maps a stable continue-as-new suggestion spelling. *)
let suggestion_reason path = function
  | "unspecified" -> Ok Suggest_unspecified
  | "history_size_too_large" -> Ok History_size_too_large
  | "too_many_history_events" -> Ok Too_many_history_events
  | "too_many_updates" -> Ok Too_many_updates
  | _ -> Error (invalid path "unknown continue-as-new suggestion reason")

(** Renders the stable continue-as-new suggestion spelling. *)
let suggestion_reason_string = function
  | Suggest_unspecified -> "unspecified"
  | History_size_too_large -> "history_size_too_large"
  | Too_many_history_events -> "too_many_history_events"
  | Too_many_updates -> "too_many_updates"

(** Decodes activation metadata required by normal SDK language layers. *)
let activation_metadata path json =
  let fields =
    [
      "available_internal_flags";
      "history_size_bytes";
      "continue_as_new_suggested";
      "deployment_version_for_current_task";
      "last_sdk_version";
      "suggest_continue_as_new_reasons";
      "target_worker_deployment_version_changed";
    ]
  in
  let* entries = exact_object path fields json in
  let* flags_json = field path "available_internal_flags" entries in
  let* available_internal_flags = list (path ^ ".available_internal_flags") uint32 flags_json in
  let* size_json = field path "history_size_bytes" entries in
  let* history_size_bytes = uint64_decimal (path ^ ".history_size_bytes") size_json in
  let* suggested_json = field path "continue_as_new_suggested" entries in
  let* continue_as_new_suggested = bool (path ^ ".continue_as_new_suggested") suggested_json in
  let deployment path json =
    let* entries = exact_object path [ "deployment_name"; "build_id" ] json in
    let* name_json = field path "deployment_name" entries in
    let* deployment_name = string (path ^ ".deployment_name") name_json in
    let* build_json = field path "build_id" entries in
    let* build_id = identifier (path ^ ".build_id") build_json in
    Ok { deployment_name; build_id }
  in
  let* deployment_json = field path "deployment_version_for_current_task" entries in
  let* deployment_version_for_current_task = nullable (path ^ ".deployment_version_for_current_task") deployment deployment_json in
  let* sdk_json = field path "last_sdk_version" entries in
  let* last_sdk_version = string (path ^ ".last_sdk_version") sdk_json in
  let* reasons_json = field path "suggest_continue_as_new_reasons" entries in
  let reason path json = let* value = string path json in suggestion_reason path value in
  let* suggest_continue_as_new_reasons = list (path ^ ".suggest_continue_as_new_reasons") reason reasons_json in
  let* changed_json = field path "target_worker_deployment_version_changed" entries in
  let* target_worker_deployment_version_changed = bool (path ^ ".target_worker_deployment_version_changed") changed_json in
  Ok { available_internal_flags; history_size_bytes; continue_as_new_suggested; deployment_version_for_current_task; last_sdk_version; suggest_continue_as_new_reasons; target_worker_deployment_version_changed }

(** Encodes activation metadata without narrowing uint64 history size. *)
let activation_metadata_json value =
  let deployment = match value.deployment_version_for_current_task with
    | None -> `Null
    | Some value -> `Assoc [ ("deployment_name", `String value.deployment_name); ("build_id", `String value.build_id) ]
  in
  Ok (`Assoc [
    ("available_internal_flags", `List (List.map (fun value -> `Intlit (Int64.to_string value)) value.available_internal_flags));
    ("history_size_bytes", `String value.history_size_bytes);
    ("continue_as_new_suggested", `Bool value.continue_as_new_suggested);
    ("deployment_version_for_current_task", deployment);
    ("last_sdk_version", `String value.last_sdk_version);
    ("suggest_continue_as_new_reasons", `List (List.map (fun value -> `String (suggestion_reason_string value)) value.suggest_continue_as_new_reasons));
    ("target_worker_deployment_version_changed", `Bool value.target_worker_deployment_version_changed)
  ])

(** Decodes one supported activation job after reading its discriminator. *)
let activation_job path json =
  let* entries =
    match json with
    | `Assoc entries -> Ok entries
    | _ -> Error (invalid path "expected JSON object")
  in
  let* kind_json = field path "kind" entries in
  let* kind = string (path ^ ".kind") kind_json in
  match kind with
  | "initialize_workflow" ->
      let has_context = List.mem_assoc "context" entries in
      let* entries =
        exact_object path
          ([ "kind"; "workflow_id"; "workflow_type"; "arguments"; "randomness_seed"; "attempt" ]
          @ if has_context then [ "context" ] else [])
          json
      in
      let* workflow_id_json = field path "workflow_id" entries in
      let* workflow_id = identifier (path ^ ".workflow_id") workflow_id_json in
      let* workflow_type_json = field path "workflow_type" entries in
      let* workflow_type = identifier (path ^ ".workflow_type") workflow_type_json in
      let* arguments_json = field path "arguments" entries in
      let* arguments = list (path ^ ".arguments") payload arguments_json in
      let* seed_json = field path "randomness_seed" entries in
      let* randomness_seed = uint64_decimal (path ^ ".randomness_seed") seed_json in
      let* attempt_json = field path "attempt" entries in
      let* attempt = positive_int32 (path ^ ".attempt") attempt_json in
      let* context =
        if has_context then
          let* value = field path "context" entries in
          let* value = initialize_context (path ^ ".context") value in
          Ok (Some value)
        else Ok None
      in
      Ok
        (Initialize_workflow
           { workflow_id; workflow_type; arguments; randomness_seed; attempt; context })
  | "resolve_activity" ->
      let* entries = exact_object path [ "kind"; "seq"; "result" ] json in
      let* seq_json = field path "seq" entries in
      let* seq = uint32 (path ^ ".seq") seq_json in
      let* result_json = field path "result" entries in
      let* result = activity_resolution (path ^ ".result") result_json in
      Ok (Resolve_activity { seq; result })
  | "fire_timer" ->
      let* entries = exact_object path [ "kind"; "seq" ] json in
      let* seq_json = field path "seq" entries in
      let* seq = uint32 (path ^ ".seq") seq_json in
      Ok (Fire_timer { seq })
  | "cancel_workflow" ->
      let* entries = exact_object path [ "kind"; "reason" ] json in
      let* reason_json = field path "reason" entries in
      let* reason = string (path ^ ".reason") reason_json in
      Ok (Cancel_workflow { reason })
  | "remove_from_cache" ->
      let* entries = exact_object path [ "kind"; "message"; "reason" ] json in
      let* message_json = field path "message" entries in
      let* message = string (path ^ ".message") message_json in
      let* reason_json = field path "reason" entries in
      let* reason_name = string (path ^ ".reason") reason_json in
      let* reason = eviction_reason (path ^ ".reason") reason_name in
      Ok (Remove_from_cache { message; reason })
  | _ -> Error (invalid (path ^ ".kind") "unsupported activation job kind")

(** Encodes one activation job. *)
let activation_job_json = function
  | Initialize_workflow
      { workflow_id; workflow_type; arguments; randomness_seed; attempt; context } ->
      let* arguments = payloads_json arguments in
      let* context = match context with None -> Ok [] | Some value -> let* value = initialize_context_json value in Ok [ ("context", value) ] in
      Ok
        (`Assoc
          ([
            ("kind", `String "initialize_workflow");
            ("workflow_id", `String workflow_id);
            ("workflow_type", `String workflow_type);
            ("arguments", arguments);
            ("randomness_seed", `String randomness_seed);
            ("attempt", `Int attempt);
          ] @ context))
  | Resolve_activity { seq; result } ->
      let* result = activity_resolution_json result in
      Ok
        (`Assoc
          [
            ("kind", `String "resolve_activity");
            ("seq", `Intlit (Int64.to_string seq));
            ("result", result);
          ])
  | Fire_timer { seq } ->
      Ok
        (`Assoc
          [ ("kind", `String "fire_timer"); ("seq", `Intlit (Int64.to_string seq)) ])
  | Cancel_workflow { reason } ->
      Ok (`Assoc [ ("kind", `String "cancel_workflow"); ("reason", `String reason) ])
  | Remove_from_cache { message; reason } ->
      Ok
        (`Assoc
          [
            ("kind", `String "remove_from_cache");
            ("message", `String message);
            ("reason", `String (eviction_reason_string reason));
          ])

(** Encodes a job list while preserving scheduler-visible order. *)
let activation_jobs_json jobs =
  let rec loop encoded = function
    | [] -> Ok (`List (List.rev encoded))
    | job :: rest ->
        let* job = activation_job_json job in
        loop (job :: encoded) rest
  in
  loop [] jobs

(** Enforces Core's invariant that an eviction activation contains only its
    single remove-from-cache job. *)
let validate_eviction_jobs path jobs =
  match List.filter (function Remove_from_cache _ -> true | _ -> false) jobs with
  | [] -> Ok ()
  | [ _ ] when List.length jobs = 1 -> Ok ()
  | _ -> Error (invalid path "cache eviction must be the activation's only job")

(** Requires initialization, when present, to occur exactly once at the head of
    the activation. Core sends initialization before any consequences for that
    workflow run, so accepting another order would hide malformed Core traffic. *)
let validate_initialize_jobs path jobs =
  let rec loop index seen = function
    | [] -> Ok ()
    | Initialize_workflow _ :: _ when seen || index <> 0 ->
        Error (invalid path "initialize_workflow must occur at most once and first")
    | Initialize_workflow _ :: rest -> loop (index + 1) true rest
    | _ :: rest -> loop (index + 1) seen rest
  in
  loop 0 false jobs

(** Accepts a missing activation timestamp only for Core's synthetic eviction
    activation, whose official constructor deliberately sets [timestamp] to
    [None]. *)
let validate_activation_timestamp path timestamp jobs =
  match (timestamp, jobs) with
  | Some _, _ -> Ok ()
  | None, [ Remove_from_cache _ ] -> Ok ()
  | None, _ -> Error (invalid path "timestamp may be null only for cache eviction")

(** Converts a strict activation object to typed values. *)
let activation_from_json json =
  let raw_entries = match json with `Assoc entries -> entries | _ -> [] in
  let has_metadata = List.mem_assoc "metadata" raw_entries in
  let* entries =
    exact_object "$"
      ([ "run_id"; "timestamp"; "is_replaying"; "history_length"; "jobs" ]
      @ if has_metadata then [ "metadata" ] else [])
      json
  in
  let* run_json = field "$" "run_id" entries in
  let* run_id = identifier "$.run_id" run_json in
  let* timestamp_json = field "$" "timestamp" entries in
  let* timestamp = nullable "$.timestamp" timestamp timestamp_json in
  let* replay_json = field "$" "is_replaying" entries in
  let* is_replaying = bool "$.is_replaying" replay_json in
  let* history_json = field "$" "history_length" entries in
  let* history_length = uint32 "$.history_length" history_json in
  let* jobs_json = field "$" "jobs" entries in
  let* jobs = list "$.jobs" activation_job jobs_json in
  let* () = validate_eviction_jobs "$.jobs" jobs in
  let* () = validate_initialize_jobs "$.jobs" jobs in
  let* () = validate_activation_timestamp "$.timestamp" timestamp jobs in
  let* metadata =
    if has_metadata then
      let* json = field "$" "metadata" entries in
      let* value = activation_metadata "$.metadata" json in
      Ok (Some value)
    else Ok None
  in
  Ok { run_id; timestamp; is_replaying; history_length; jobs; metadata }

(** Strictly decodes one activation through the shared JSON foundation. *)
let decode_activation input =
  match Control.decode_payload_object input with
  | Error error -> Error (of_control_error "$" error)
  | Ok json -> activation_from_json json

(** Encodes and semantically reparses one activation. *)
let encode_activation value =
  let* jobs = activation_jobs_json value.jobs in
  let* metadata =
    match value.metadata with
    | None -> Ok []
    | Some value ->
        let* value = activation_metadata_json value in
        Ok [ ("metadata", value) ]
  in
  let json =
    `Assoc
      ([
        ("run_id", `String value.run_id);
        ( "timestamp",
          match value.timestamp with
          | None -> `Null
          | Some value -> time_json value.seconds value.nanoseconds );
        ("is_replaying", `Bool value.is_replaying);
        ("history_length", `Intlit (Int64.to_string value.history_length));
        ("jobs", jobs);
      ] @ metadata)
  in
  match Control.encode_payload_object json with
  | Error error -> Error (of_control_error "$" error)
  | Ok output ->
      let* _ = decode_activation output in
      Ok output

(** Maps the activity-cancellation spelling from Core. *)
let cancellation_type path = function
  | "try_cancel" -> Ok Try_cancel
  | "wait_cancellation_completed" -> Ok Wait_cancellation_completed
  | "abandon" -> Ok Abandon
  | _ -> Error (invalid path "unknown activity cancellation type")

(** Renders the activity-cancellation spelling expected by Core. *)
let cancellation_type_string = function
  | Try_cancel -> "try_cancel"
  | Wait_cancellation_completed -> "wait_cancellation_completed"
  | Abandon -> "abandon"

(** Decodes a nullable duration. *)
let optional_duration path value = nullable path duration value

(** Encodes a nullable duration. *)
let optional_duration_json = function
  | None -> `Null
  | Some value -> time_json value.seconds value.nanoseconds

(** Decodes one supported completion command. *)
let completion_command path json =
  let* entries =
    match json with
    | `Assoc entries -> Ok entries
    | _ -> Error (invalid path "expected JSON object")
  in
  let* kind_json = field path "kind" entries in
  let* kind = string (path ^ ".kind") kind_json in
  match kind with
  | "schedule_activity" ->
      let* entries =
        exact_object path
          [
            "kind";
            "seq";
            "activity_id";
            "activity_type";
            "task_queue";
            "arguments";
            "schedule_to_close_timeout";
            "schedule_to_start_timeout";
            "start_to_close_timeout";
            "heartbeat_timeout";
            "cancellation_type";
            "do_not_eagerly_execute";
          ]
          json
      in
      let* seq_json = field path "seq" entries in
      let* seq = uint32 (path ^ ".seq") seq_json in
      let* activity_id_json = field path "activity_id" entries in
      let* activity_id = identifier (path ^ ".activity_id") activity_id_json in
      let* activity_type_json = field path "activity_type" entries in
      let* activity_type = identifier (path ^ ".activity_type") activity_type_json in
      let* task_queue_json = field path "task_queue" entries in
      let* task_queue = identifier (path ^ ".task_queue") task_queue_json in
      let* arguments_json = field path "arguments" entries in
      let* arguments = list (path ^ ".arguments") payload arguments_json in
      let decode_timeout name =
        let* json = field path name entries in
        optional_duration (path ^ "." ^ name) json
      in
      let* schedule_to_close_timeout = decode_timeout "schedule_to_close_timeout" in
      let* schedule_to_start_timeout = decode_timeout "schedule_to_start_timeout" in
      let* start_to_close_timeout = decode_timeout "start_to_close_timeout" in
      let* heartbeat_timeout = decode_timeout "heartbeat_timeout" in
      let* cancellation_json = field path "cancellation_type" entries in
      let* cancellation_name = string (path ^ ".cancellation_type") cancellation_json in
      let* cancellation_type = cancellation_type (path ^ ".cancellation_type") cancellation_name in
      let* eager_json = field path "do_not_eagerly_execute" entries in
      let* do_not_eagerly_execute = bool (path ^ ".do_not_eagerly_execute") eager_json in
      if Option.is_none schedule_to_close_timeout && Option.is_none start_to_close_timeout then
        Error
          (invalid path
             "activity requires schedule-to-close or start-to-close timeout")
      else
        Ok
          (Schedule_activity
             {
               seq;
               activity_id;
               activity_type;
               task_queue;
               arguments;
               schedule_to_close_timeout;
               schedule_to_start_timeout;
               start_to_close_timeout;
               heartbeat_timeout;
               cancellation_type;
               do_not_eagerly_execute;
             })
  | "request_cancel_activity" ->
      let* entries = exact_object path [ "kind"; "seq" ] json in
      let* seq_json = field path "seq" entries in
      let* seq = uint32 (path ^ ".seq") seq_json in
      Ok (Request_cancel_activity { seq })
  | "start_timer" ->
      let* entries = exact_object path [ "kind"; "seq"; "start_to_fire_timeout" ] json in
      let* seq_json = field path "seq" entries in
      let* seq = uint32 (path ^ ".seq") seq_json in
      let* timeout_json = field path "start_to_fire_timeout" entries in
      let* start_to_fire_timeout = duration (path ^ ".start_to_fire_timeout") timeout_json in
      Ok (Start_timer { seq; start_to_fire_timeout })
  | "cancel_timer" ->
      let* entries = exact_object path [ "kind"; "seq" ] json in
      let* seq_json = field path "seq" entries in
      let* seq = uint32 (path ^ ".seq") seq_json in
      Ok (Cancel_timer { seq })
  | "complete_workflow" ->
      let* entries = exact_object path [ "kind"; "result" ] json in
      let* result_json = field path "result" entries in
      let* result = nullable (path ^ ".result") payload result_json in
      Ok (Complete_workflow { result })
  | "fail_workflow" ->
      let* entries = exact_object path [ "kind"; "failure" ] json in
      let* failure_json = field path "failure" entries in
      let* failure = failure (path ^ ".failure") failure_json in
      Ok (Fail_workflow { failure })
  | "cancel_workflow" ->
      let* _ = exact_object path [ "kind" ] json in
      Ok Cancel_workflow_execution
  | _ -> Error (invalid (path ^ ".kind") "unsupported completion command kind")

(** Encodes one completion command. *)
let completion_command_json = function
  | Schedule_activity value ->
      let* arguments = payloads_json value.arguments in
      Ok
        (`Assoc
          [
            ("kind", `String "schedule_activity");
            ("seq", `Intlit (Int64.to_string value.seq));
            ("activity_id", `String value.activity_id);
            ("activity_type", `String value.activity_type);
            ("task_queue", `String value.task_queue);
            ("arguments", arguments);
            ("schedule_to_close_timeout", optional_duration_json value.schedule_to_close_timeout);
            ("schedule_to_start_timeout", optional_duration_json value.schedule_to_start_timeout);
            ("start_to_close_timeout", optional_duration_json value.start_to_close_timeout);
            ("heartbeat_timeout", optional_duration_json value.heartbeat_timeout);
            ("cancellation_type", `String (cancellation_type_string value.cancellation_type));
            ("do_not_eagerly_execute", `Bool value.do_not_eagerly_execute);
          ])
  | Request_cancel_activity { seq } ->
      Ok
        (`Assoc
          [
            ("kind", `String "request_cancel_activity");
            ("seq", `Intlit (Int64.to_string seq));
          ])
  | Start_timer { seq; start_to_fire_timeout } ->
      Ok
        (`Assoc
          [
            ("kind", `String "start_timer");
            ("seq", `Intlit (Int64.to_string seq));
            ( "start_to_fire_timeout",
              time_json start_to_fire_timeout.seconds start_to_fire_timeout.nanoseconds );
          ])
  | Cancel_timer { seq } ->
      Ok
        (`Assoc
          [ ("kind", `String "cancel_timer"); ("seq", `Intlit (Int64.to_string seq)) ])
  | Complete_workflow { result } ->
      let* result = match result with None -> Ok `Null | Some value -> payload_json value in
      Ok (`Assoc [ ("kind", `String "complete_workflow"); ("result", result) ])
  | Fail_workflow { failure } ->
      let* failure = failure_json failure in
      Ok (`Assoc [ ("kind", `String "fail_workflow"); ("failure", failure) ])
  | Cancel_workflow_execution -> Ok (`Assoc [ ("kind", `String "cancel_workflow") ])

(** Encodes completion commands while preserving deterministic source order. *)
let completion_commands_json commands =
  let rec loop encoded = function
    | [] -> Ok (`List (List.rev encoded))
    | command :: rest ->
        let* command = completion_command_json command in
        loop (command :: encoded) rest
  in
  loop [] commands

(** Identifies commands that end a workflow execution. *)
let terminal = function
  | Complete_workflow _ | Fail_workflow _ | Cancel_workflow_execution -> true
  | _ -> false

(** Requires a terminal command, if present, to be unique and final. *)
let validate_terminal_order path commands =
  let rec loop = function
    | [] -> Ok ()
    | [ command ] when terminal command -> Ok ()
    | command :: _ when terminal command ->
        Error (invalid path "terminal workflow command must be last")
    | _ :: rest -> loop rest
  in
  loop commands

(** Converts a strict completion object to typed values. *)
let completion_from_json json =
  let* entries = exact_object "$" [ "run_id"; "commands" ] json in
  let* run_json = field "$" "run_id" entries in
  let* run_id = identifier "$.run_id" run_json in
  let* commands_json = field "$" "commands" entries in
  let* commands = list "$.commands" completion_command commands_json in
  let* () = validate_terminal_order "$.commands" commands in
  Ok { run_id; commands }

(** Strictly decodes one completion through the shared JSON foundation. *)
let decode_completion input =
  match Control.decode_payload_object input with
  | Error error -> Error (of_control_error "$" error)
  | Ok json -> completion_from_json json

(** Encodes and semantically reparses one outgoing completion. *)
let encode_completion value =
  let* commands = completion_commands_json value.commands in
  let json = `Assoc [ ("run_id", `String value.run_id); ("commands", commands) ] in
  match Control.encode_payload_object json with
  | Error error -> Error (of_control_error "$" error)
  | Ok output ->
      let* _ = decode_completion output in
      Ok output

(** Private aliases make the canonical semantic codecs reusable without
    copying validation logic into each protocol adapter. *)
module Internal = struct
  let invalid = invalid
  let of_control_error = of_control_error
  let exact_object = exact_object
  let field = field
  let string = string
  let bool = bool
  let uint32 = uint32
  let int32 = int32
  let identifier = identifier
  let uint64_decimal = uint64_decimal
  let list = list
  let nullable = nullable
  let bytes_wrapper = bytes_wrapper
  let bytes_wrapper_json = bytes_wrapper_json
  let payload = payload
  let payload_json = payload_json
  let timestamp = timestamp
  let duration = duration
  let time_json = time_json
  let workflow_execution = workflow_execution
  let workflow_priority = workflow_priority
  let failure = failure
  let failure_json = failure_json
end
