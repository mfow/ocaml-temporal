(** Closed client-side JSON protocol implementation.

    The protocol deliberately reuses the workflow module's payload and failure
    codecs.  That keeps binary payload ownership, base64 canonicalization, and
    recursive failure validation identical on both client and worker paths. *)

module Control = Control_protocol
module Workflow = Workflow_protocol

type payload = Workflow.payload
type failure = Workflow.failure
type execution = { namespace : string; workflow_id : string; run_id : string }

type start_request = {
  namespace : string;
  workflow_id : string;
  workflow_type : string;
  task_queue : string;
  input : payload list;
}

type start_response = { execution : execution }
type wait_request = execution

type outcome =
  | Completed of { result : payload list; successor : execution option }
  | Failed of { failure : failure; successor : execution option }
  | Cancelled of { details : payload list }
  | Terminated of { details : payload list }
  | Timed_out of { successor : execution option }
  | Continued_as_new of { successor : execution }

type wait_response = { execution : execution; outcome : outcome }

type client_error =
  | Already_started of { workflow_id : string; existing_run_id : string option }
  | Rpc of { code : string }
  | Protocol of { code : string }

type error = { code : string; path : string; message : string }
type error_view = { code : string; path : string; message : string }

let error_view (error : error) : error_view =
  { code = error.code; path = error.path; message = error.message }

let ( let* ) = Result.bind

let of_workflow_error path (error : Workflow.error) : error =
  let view = Workflow.error_view error in
  { code = view.code; path; message = view.message }

let of_control_error path (error : Control.error) : error =
  let view = Control.error_view error in
  { code = view.code; path; message = view.message }

let invalid ?(path = "$") message : error =
  { code = "invalid_message"; path; message }

let exact_object path fields json =
  match json with
  | `Assoc _ -> (
      match Workflow.Internal.exact_object path fields json with
      | Ok entries -> Ok entries
      | Error error -> Error (of_workflow_error path error))
  | _ -> Error (invalid ~path "expected JSON object")

let field path name entries =
  match Workflow.Internal.field path name entries with
  | Ok value -> Ok value
  | Error error -> Error (of_workflow_error path error)

let string path json =
  match Workflow.Internal.string path json with
  | Ok value -> Ok value
  | Error error -> Error (of_workflow_error path error)

let identifier path json =
  match Workflow.Internal.identifier path json with
  | Ok value when String.contains value '\000' ->
      Error (invalid ~path "identifier contains a NUL byte")
  | Ok value -> Ok value
  | Error error -> Error (of_workflow_error path error)

(** Validates an identifier before it is serialized. The Rust bridge applies
    the same non-empty, bounded, NUL-free rule; enforcing it here keeps an
    invalid request from crossing the FFI boundary at all. *)
let validate_identifier path value =
  if String.length value = 0 then Error (invalid ~path "identifier is empty")
  else if String.length value > 65_536 then
    Error (invalid ~path "identifier exceeds the protocol string safety limit")
  else if String.contains value '\000' then
    Error (invalid ~path "identifier contains a NUL byte")
  else Ok ()

let nullable _path (decode : Yojson.Safe.t -> ('a, error) result)
    (json : Yojson.Safe.t) : ('a option, error) result =
  match json with
  | `Null -> Ok None
  | value -> Result.map (fun value -> Some value) (decode value)

let payload path json =
  match Workflow.Internal.payload path json with
  | Ok value -> Ok value
  | Error error -> Error (of_workflow_error path error)

let payloads path json =
  match json with
  | `List values ->
      let rec loop index reversed = function
        | [] -> Ok (List.rev reversed)
        | value :: rest ->
            let* value = payload (Printf.sprintf "%s[%d]" path index) value in
            loop (index + 1) (value :: reversed) rest
      in
      loop 0 [] values
  | _ -> Error (invalid ~path "expected JSON array")

let payload_json value =
  Workflow.Internal.payload_json value
  |> Result.map_error (of_workflow_error "$.payload")

let payloads_json values =
  let rec loop reversed = function
    | [] -> Ok (`List (List.rev reversed))
    | value :: rest ->
        let* encoded = payload_json value in
        loop (encoded :: reversed) rest
  in
  loop [] values

let json_string value = `String value

let decode_execution path json =
  let* entries = exact_object path [ "namespace"; "workflow_id"; "run_id" ] json in
  let* namespace_json = field path "namespace" entries in
  let* namespace = identifier (path ^ ".namespace") namespace_json in
  let* workflow_id_json = field path "workflow_id" entries in
  let* workflow_id = identifier (path ^ ".workflow_id") workflow_id_json in
  let* run_id_json = field path "run_id" entries in
  let* run_id = identifier (path ^ ".run_id") run_id_json in
  Ok { namespace; workflow_id; run_id }

let encode_object json =
  match Control.encode_payload_object json with
  | Ok value -> Ok value
  | Error error -> Error (of_control_error "$" error)

let decode_object input =
  match Control.decode_payload_object input with
  | Ok value -> Ok value
  | Error error -> Error (of_control_error "$" error)

let encode_start_request (value : start_request) =
  let* () = validate_identifier "$.namespace" value.namespace in
  let* () = validate_identifier "$.workflow_id" value.workflow_id in
  let* () = validate_identifier "$.workflow_type" value.workflow_type in
  let* () = validate_identifier "$.task_queue" value.task_queue in
  let* input = payloads_json value.input in
  encode_object
    (`Assoc
      [
        ("namespace", json_string value.namespace);
        ("workflow_id", json_string value.workflow_id);
        ("workflow_type", json_string value.workflow_type);
        ("task_queue", json_string value.task_queue);
        ("input", input);
      ])

let decode_start_response input =
  let* json = decode_object input in
  let* entries = exact_object "$" [ "execution" ] json in
  let* execution_json = field "$" "execution" entries in
  let* execution = decode_execution "$.execution" execution_json in
  Ok { execution }

let encode_wait_request (value : wait_request) =
  let* () = validate_identifier "$.namespace" value.namespace in
  let* () = validate_identifier "$.workflow_id" value.workflow_id in
  let* () = validate_identifier "$.run_id" value.run_id in
  encode_object
    (`Assoc
      [
        ("namespace", json_string value.namespace);
        ("workflow_id", json_string value.workflow_id);
        ("run_id", json_string value.run_id);
      ])

let decode_successor path entries =
  let* successor_json = field path "successor" entries in
  nullable (path ^ ".successor") (decode_execution (path ^ ".successor"))
    successor_json

let decode_outcome json =
  let path = "$.outcome" in
  let* kind_json =
    match json with
    | `Assoc entries -> field path "kind" entries
    | _ -> Error (invalid ~path "expected JSON object")
  in
  let* kind = string (path ^ ".kind") kind_json in
  match kind with
  | "completed" ->
      let* entries = exact_object path [ "kind"; "result"; "successor" ] json in
      let* result_json = field path "result" entries in
      let* result = payloads (path ^ ".result") result_json in
      let* successor = decode_successor path entries in
      Ok (Completed { result; successor })
  | "failed" ->
      let* entries = exact_object path [ "kind"; "failure"; "successor" ] json in
      let* failure_json = field path "failure" entries in
      let* failure =
        match Workflow.Internal.failure (path ^ ".failure") failure_json with
        | Ok value -> Ok value
        | Error error -> Error (of_workflow_error path error)
      in
      let* successor = decode_successor path entries in
      Ok (Failed { failure; successor })
  | "cancelled" ->
      let* entries = exact_object path [ "kind"; "details" ] json in
      let* details_json = field path "details" entries in
      let* details = payloads (path ^ ".details") details_json in
      Ok (Cancelled { details })
  | "terminated" ->
      let* entries = exact_object path [ "kind"; "details" ] json in
      let* details_json = field path "details" entries in
      let* details = payloads (path ^ ".details") details_json in
      Ok (Terminated { details })
  | "timed_out" ->
      let* entries = exact_object path [ "kind"; "successor" ] json in
      let* successor = decode_successor path entries in
      Ok (Timed_out { successor })
  | "continued_as_new" ->
      let* entries = exact_object path [ "kind"; "successor" ] json in
      let* successor_json = field path "successor" entries in
      let* successor = decode_execution (path ^ ".successor") successor_json in
      Ok (Continued_as_new { successor })
  | _ ->
      Error
        (invalid ~path:(path ^ ".kind") "unknown workflow outcome kind")

(** Checks that a successor stays in the same workflow chain and names a new
    run. This mirrors Rust's server-response validation before the result is
    allowed into public OCaml code. *)
let validate_successor_for_execution (execution : execution) path
    (successor : execution) =
  let* () = validate_identifier (path ^ ".namespace") successor.namespace in
  let* () = validate_identifier (path ^ ".workflow_id") successor.workflow_id in
  let* () = validate_identifier (path ^ ".run_id") successor.run_id in
  if not (String.equal successor.namespace execution.namespace) then
    Error
      (invalid ~path:(path ^ ".namespace")
         "successor namespace does not match the waited execution")
  else if not (String.equal successor.workflow_id execution.workflow_id) then
    Error
      (invalid ~path:(path ^ ".workflow_id")
         "successor workflow ID does not match the waited execution")
  else if String.equal successor.run_id execution.run_id then
    Error
      (invalid ~path:(path ^ ".run_id")
         "successor run ID must differ from the waited run")
  else Ok ()

(** Applies successor-chain validation to every outcome variant that carries
    optional or required successor metadata. *)
let validate_wait_successor execution outcome =
  let validate path successor = validate_successor_for_execution execution path successor in
  match outcome with
  | Completed { successor = Some successor; _ }
  | Failed { successor = Some successor; _ }
  | Timed_out { successor = Some successor } ->
      validate "$.outcome.successor" successor
  | Continued_as_new { successor } ->
      validate "$.outcome.successor" successor
  | Completed { successor = None; _ }
  | Failed { successor = None; _ }
  | Cancelled _
  | Terminated _
  | Timed_out { successor = None } -> Ok ()

(** Accepts exactly the stable status-code vocabulary emitted by the Rust
    client bridge. Unknown codes fail closed rather than being mistaken for a
    known operational category. *)
let client_error_code kind path value =
  let rpc_codes =
    [
      "ok";
      "cancelled";
      "unknown";
      "invalid_argument";
      "deadline_exceeded";
      "not_found";
      "already_exists";
      "permission_denied";
      "resource_exhausted";
      "failed_precondition";
      "aborted";
      "out_of_range";
      "unimplemented";
      "internal";
      "unavailable";
      "data_loss";
      "unauthenticated";
    ]
  in
  let protocol_codes = [ "core_unsupported"; "core_invalid" ] in
  let allowed = if String.equal kind "rpc" then rpc_codes else protocol_codes in
  if List.mem value allowed then Ok value
  else Error (invalid ~path "unknown client error code")

let decode_wait_response input =
  let* json = decode_object input in
  let* entries = exact_object "$" [ "execution"; "outcome" ] json in
  let* execution_json = field "$" "execution" entries in
  let* execution = decode_execution "$.execution" execution_json in
  let* outcome_json = field "$" "outcome" entries in
  let* outcome = decode_outcome outcome_json in
  let* () = validate_wait_successor execution outcome in
  Ok { execution; outcome }

let decode_client_error input =
  let* json = decode_object input in
  let path = "$" in
  let* entries =
    match json with
    | `Assoc entries -> Ok entries
    | _ -> Error (invalid "expected JSON object")
  in
  let* kind_json = field path "kind" entries in
  let* kind = string "$.kind" kind_json in
  match kind with
  | "already_started" ->
      let* entries = exact_object path [ "kind"; "workflow_id"; "existing_run_id" ] json in
      let* workflow_id_json = field path "workflow_id" entries in
      let* workflow_id = identifier "$.workflow_id" workflow_id_json in
      let* existing_json = field path "existing_run_id" entries in
      let* existing_run_id =
        nullable "$.existing_run_id"
          (fun value -> identifier "$.existing_run_id" value)
          existing_json
      in
      Ok (Already_started { workflow_id; existing_run_id })
  | "rpc" | "protocol" ->
      let* entries = exact_object path [ "kind"; "code" ] json in
      let* code_json = field path "code" entries in
      let* code = identifier "$.code" code_json in
      let* code = client_error_code kind "$.code" code in
      if String.equal kind "rpc" then Ok (Rpc { code })
      else Ok (Protocol { code })
  | _ -> Error (invalid ~path:"$.kind" "unknown client error kind")
