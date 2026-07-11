module Supervisor = Sdk_supervisor.Native
module Bridge = Temporal_core_bridge.Native_bridge
module Workflow = Temporal_protocol.Workflow_protocol
module Activity = Temporal_protocol.Activity_protocol

(** Requires two structural values to be equal and identifies the violated
    native-worker contract if they differ. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Reports whether [needle] occurs in [source] without requiring a
    standard-library substring helper newer than the oldest supported OCaml. *)
let contains_substring source needle =
  let source_length = String.length source in
  let needle_length = String.length needle in
  let rec loop offset =
    offset + needle_length <= source_length
    &&
    (String.equal (String.sub source offset needle_length) needle
    || loop (offset + 1))
  in
  needle_length = 0 || loop 0

(** Extracts a successful private adapter result for positive serialization
    cases while retaining a useful bridge diagnostic on failure. *)
let require_bridge = function
  | Ok value -> value
  | Error { Bridge.status; message } ->
      let status =
        match status with
        | Invalid_argument -> "invalid_argument"
        | Abi_mismatch -> "abi_mismatch"
        | Panic -> "panic"
        | Internal -> "internal"
        | Invalid_state -> "invalid_state"
        | Configuration -> "configuration"
        | Connection -> "connection"
        | Worker -> "worker"
        | Outstanding_tasks -> "outstanding_tasks"
        | Not_ready -> "not_ready"
        | Protocol -> "protocol"
        | Unknown code -> Printf.sprintf "unknown(%d)" code
      in
      failwith (Printf.sprintf "%s: %s" status message)

(** A minimal ordinary activation proves that polling bytes are validated and
    converted to the private typed workflow model before leaving the owner
    subsystem. *)
let workflow_activation_json =
  {|{"run_id":"run-1","timestamp":{"seconds":1,"nanoseconds":0},"is_replaying":false,"history_length":1,"jobs":[]}|}

(** A cancellation task keeps the activity fixture compact while still
    proving that opaque task-token bytes cross the adapter losslessly. *)
let activity_task_json =
  {|{"task_token":"AAEC","variant":{"kind":"cancel","reason":"worker_shutdown","details":null}}|}

(** Empty native lanes are an ordinary nonblocking readiness result, while a
    real bridge failure must remain distinguishable from an idle poll. *)
let test_nonblocking_readiness_results () =
  expect "empty workflow lane" (Ok None)
    (Supervisor.Protocol_adapter.workflow_poll_result
       (Error { Bridge.status = Not_ready; message = "lane empty" }));
  expect "empty activity lane" (Ok None)
    (Supervisor.Protocol_adapter.activity_poll_result
       (Error { Bridge.status = Not_ready; message = "lane empty" }));
  let failure = { Bridge.status = Worker; message = "poll lane stopped" } in
  expect "workflow poll failure" (Error failure)
    (Supervisor.Protocol_adapter.workflow_poll_result (Error failure))

(** Valid poll documents become typed values, and typed completions become the
    exact canonical JSON documents accepted by the Rust bridge. *)
let test_protocol_serialization () =
  let activation =
    require_bridge
      (Supervisor.Protocol_adapter.decode_workflow_activation
         (Bytes.of_string workflow_activation_json))
  in
  expect "workflow run id" "run-1" activation.run_id;
  let task =
    require_bridge
      (Supervisor.Protocol_adapter.decode_activity_task
         (Bytes.of_string activity_task_json))
  in
  if not (Bytes.equal task.task_token (Bytes.of_string "\000\001\002")) then
    failwith "activity token changed while decoding";
  let workflow_completion : Workflow.completion =
    { run_id = "run-1"; commands = [] }
  in
  expect "workflow completion JSON"
    (Bytes.of_string {|{"commands":[],"run_id":"run-1"}|})
    (require_bridge
       (Supervisor.Protocol_adapter.encode_workflow_completion
          workflow_completion));
  let activity_completion : Activity.completion =
    { task_token = Bytes.of_string "\000\001\002"; result = Will_complete_async }
  in
  expect "activity completion JSON"
    (Bytes.of_string
       {|{"result":{"kind":"will_complete_async"},"task_token":"AAEC"}|})
    (require_bridge
       (Supervisor.Protocol_adapter.encode_activity_completion
          activity_completion))

(** Invalid incoming and outgoing semantic documents are converted to the
    stable bridge [Protocol] status without including source payload bytes in
    the diagnostic. *)
let test_protocol_failures_are_typed () =
  (match
     Supervisor.Protocol_adapter.decode_workflow_activation
       (Bytes.of_string {|{"run_id":"secret-payload"}|})
   with
  | Error { Bridge.status = Protocol; message } ->
      if String.length message = 0 then failwith "empty workflow protocol error";
      if contains_substring message "secret-payload"
      then failwith "workflow protocol error exposed source JSON"
  | _ -> failwith "invalid workflow activation was not a protocol error");
  let invalid_completion : Workflow.completion =
    { run_id = ""; commands = [] }
  in
  match
    Supervisor.Protocol_adapter.encode_workflow_completion invalid_completion
  with
  | Error { Bridge.status = Protocol; message } ->
      if String.length message = 0 then failwith "empty completion protocol error"
  | _ -> failwith "invalid workflow completion was not a protocol error"

(** The production supervisor rejects polling before worker construction,
    validates completions before entering Rust, and closes every worker
    operation at the mailbox admission boundary after shutdown. *)
let test_native_lifecycle_guards () =
  let supervisor = Result.get_ok (Supervisor.create ~capacity:4 ()) in
  (match Supervisor.perform supervisor Supervisor.Try_poll_workflow with
  | Error (Supervisor.Backend { Bridge.status = Invalid_state; _ }) -> ()
  | _ -> failwith "workflow poll without worker was accepted");
  (match Supervisor.perform supervisor Supervisor.Try_poll_activity with
  | Error (Supervisor.Backend { Bridge.status = Invalid_state; _ }) -> ()
  | _ -> failwith "activity poll without worker was accepted");
  let invalid_completion : Workflow.completion =
    { run_id = ""; commands = [] }
  in
  (match
     Supervisor.perform supervisor
       (Supervisor.Complete_workflow invalid_completion)
   with
  | Error (Supervisor.Backend { Bridge.status = Protocol; _ }) -> ()
  | _ -> failwith "invalid completion reached the native worker");
  let workflow_completion : Workflow.completion =
    { run_id = "run-1"; commands = [] }
  in
  (match
     Supervisor.perform supervisor
       (Supervisor.Complete_workflow workflow_completion)
   with
  | Error (Supervisor.Backend { Bridge.status = Protocol; _ }) -> ()
  | _ -> failwith "unleased workflow completion was accepted");
  let invalid_activity_completion : Activity.completion =
    { task_token = Bytes.empty; result = Will_complete_async }
  in
  (match
     Supervisor.perform supervisor
       (Supervisor.Complete_activity invalid_activity_completion)
   with
  | Error (Supervisor.Backend { Bridge.status = Protocol; _ }) -> ()
  | _ -> failwith "invalid activity completion reached the native worker");
  let activity_completion : Activity.completion =
    { task_token = Bytes.of_string "token"; result = Will_complete_async }
  in
  (match
     Supervisor.perform supervisor
       (Supervisor.Complete_activity activity_completion)
   with
  | Error (Supervisor.Backend { Bridge.status = Invalid_state; _ }) -> ()
  | _ -> failwith "activity completion without worker was accepted");
  expect "native shutdown" (Ok ()) (Supervisor.shutdown supervisor);
  expect "poll after shutdown" (Error Supervisor.Closed)
    (Supervisor.perform supervisor Supervisor.Try_poll_workflow)

let () =
  test_nonblocking_readiness_results ();
  test_protocol_serialization ();
  test_protocol_failures_are_typed ();
  test_native_lifecycle_guards ()
