module Protocol = Temporal_protocol.Client_protocol
module Workflow = Temporal_protocol.Workflow_protocol

(** Extracts a valid protocol value or fails with the privacy-safe diagnostic. *)
let unwrap = function
  | Ok value -> value
  | Error error ->
      let view = Protocol.error_view error in
      failwith (Printf.sprintf "%s at %s: %s" view.code view.path view.message)

(** Requires a decoder or encoder to reject the supplied malformed value. *)
let require_error = function
  | Error _ -> ()
  | Ok _ -> failwith "expected client protocol validation to fail"

(** Checks that canonical JSON contains a small structural marker without
    coupling this test to association-list member ordering. *)
let require_fragment label fragment value =
  let fragment_length = String.length fragment in
  let rec contains offset =
    if offset + fragment_length > String.length value then false
    else if String.sub value offset fragment_length = fragment then true
    else contains (offset + 1)
  in
  if not (contains 0) then
    failwith (label ^ " did not contain its expected JSON marker")

(** Creates a binary-safe payload used by start requests and terminal results. *)
let payload data : Protocol.payload =
  { Workflow.metadata = [ ("encoding", Bytes.of_string "binary/plain") ]; data }

(** Shared execution identity used by all exact-run response fixtures. *)
let execution : Protocol.execution =
  { namespace = "default"; workflow_id = "workflow-1"; run_id = "run-1" }

(** Start requests use the same workflow identity as the response fixtures. *)
let start_request : Protocol.start_request =
  {
    request_id = "request-1";
    namespace = execution.namespace;
    workflow_id = execution.workflow_id;
    workflow_type = "Smoke";
    task_queue = "queue";
    input = [];
  }

(** The canonical payload wrapper for the bytes [ok]. *)
let ok_payload_json =
  {|{"metadata":{"encoding":{"encoding":"base64","data":"YmluYXJ5L3BsYWlu"}},"data":{"encoding":"base64","data":"b2s="}}|}

(** Decodes every terminal outcome supported by the closed native client ABI. *)
let test_terminal_outcomes () =
  let prefix =
    {|{"execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1"},"outcome":|}
  in
  let suffix = "}" in
  let documents =
    [
      (prefix
      ^ {|{"kind":"completed","result":|}
      ^ "[" ^ ok_payload_json ^ "]"
      ^ {|,"successor":null}|}
      ^ suffix);
      (prefix
      ^ {|{"kind":"failed","failure":{"message":"failed","source":"worker","stack_trace":"","encoded_attributes":null,"cause":null,"info":{"kind":"application","type":"Failure","non_retryable":true,"details":[]}},"successor":null}|}
      ^ suffix);
      (prefix ^ {|{"kind":"cancelled","details":[]}|} ^ suffix);
      (prefix ^ {|{"kind":"terminated","details":[]}|} ^ suffix);
      (prefix ^ {|{"kind":"timed_out","successor":null}|} ^ suffix);
      (prefix
      ^ {|{"kind":"continued_as_new","successor":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-2"}}|}
      ^ suffix);
    ]
  in
  List.iter
    (fun document ->
      ignore (unwrap (Protocol.decode_wait_response ~request:execution document)))
    documents;
  let completed =
    unwrap (Protocol.decode_wait_response ~request:execution (List.hd documents))
  in
  match completed.outcome with
  | Protocol.Completed { result = [ value ]; successor = None } ->
      if not (Bytes.equal value.data (Bytes.of_string "ok")) then
        failwith "completed result payload changed"
  | _ -> failwith "completed response did not retain its result shape"

(** Rejects successor identities that would silently leave or repeat the exact
    run selected by the wait request. *)
let test_successor_identity_validation () =
  let response suffix =
    {|{"execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1"},"outcome":{"kind":"continued_as_new","successor":|}
    ^ suffix ^ "}}"
  in
  List.iter
    (fun suffix ->
      require_error
        (Protocol.decode_wait_response ~request:execution (response suffix)))
    [
      {|{"namespace":"other","workflow_id":"workflow-1","run_id":"run-2"}|};
      {|{"namespace":"default","workflow_id":"other","run_id":"run-2"}|};
      {|{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1"}|};
    ]

(** Confirms request encoders validate identifiers before sending bytes to Rust. *)
let test_request_validation () =
  let valid_start : Protocol.start_request =
    { start_request with input = [ payload (Bytes.of_string "input") ] }
  in
  let encoded_start = unwrap (Protocol.encode_start_request valid_start) in
  require_fragment "start request" "{" encoded_start;
  let encoded_wait = unwrap (Protocol.encode_wait_request execution) in
  require_fragment "wait request" "{" encoded_wait;
  List.iter
    (fun request -> require_error (Protocol.encode_start_request request))
    [
      { valid_start with namespace = "" };
      { valid_start with workflow_id = "contains\000nul" };
      { valid_start with workflow_type = String.make 65_537 'x' };
      { valid_start with task_queue = "" };
    ];
  List.iter
    (fun request -> require_error (Protocol.encode_wait_request request))
    [
      { execution with namespace = "" };
      { execution with workflow_id = "contains\000nul" };
      { execution with run_id = String.make 65_537 'x' };
    ]

(** Exercises the asynchronous-start capability and all three terminal outcome
    variants. The request-bound ticket is deliberately opaque: this test can
    recover the retained request only through the protocol accessor, never by
    inspecting or constructing the native ticket string. *)
let test_async_start_protocol () =
  let ticket_json = {|{"ticket":"ticket-1"}|} in
  let ticket =
    unwrap (Protocol.decode_start_ticket ~request:start_request ticket_json)
  in
  if Protocol.start_ticket_request ticket <> start_request then
    failwith "start ticket lost its originating request";
  let encoded_ticket = unwrap (Protocol.encode_start_ticket ticket) in
  require_fragment "start ticket" "ticket-1" encoded_ticket;
  require_error
    (Protocol.decode_start_ticket ~request:start_request
       {|{"ticket":"ticket-1","extra":true}|});
  require_error
    (Protocol.decode_start_ticket ~request:start_request {|{"ticket":""}|});
  let accepted_json =
    {|{"kind":"accepted","execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-2"}}|}
  in
  let accepted =
    unwrap (Protocol.decode_start_outcome ~request:start_request accepted_json)
  in
  (match accepted with
  | Protocol.Accepted { execution = { run_id = "run-2"; _ } } -> ()
  | _ -> failwith "accepted start outcome changed shape");
  let rejected_json =
    {|{"kind":"rejected","error":{"kind":"already_started","workflow_id":"workflow-1","existing_run_id":"run-existing"}}|}
  in
  let rejected =
    unwrap (Protocol.decode_start_outcome ~request:start_request rejected_json)
  in
  (match rejected with
  | Protocol.Rejected
      (Protocol.Already_started
        { workflow_id = "workflow-1"; existing_run_id = Some "run-existing" }) ->
      ()
  | _ -> failwith "rejected start outcome changed shape");
  let unknown_json =
    {|{"kind":"unknown","request_id":"request-1","workflow_id":"workflow-1"}|}
  in
  let unknown =
    unwrap (Protocol.decode_start_outcome ~request:start_request unknown_json)
  in
  (match unknown with
  | Protocol.Unknown { request_id = "request-1"; workflow_id = "workflow-1" } ->
      ()
  | _ -> failwith "unknown start outcome changed shape");
  List.iter
    (fun outcome ->
      ignore (unwrap (Protocol.encode_start_outcome outcome)))
    [
      accepted;
      rejected;
      unknown;
    ];
  require_error
    (Protocol.decode_start_outcome ~request:start_request
       {|{"kind":"unknown","request_id":"other-request","workflow_id":"workflow-1"}|});
  require_error
    (Protocol.decode_start_outcome ~request:start_request
       {|{"kind":"accepted","execution":{"namespace":"other","workflow_id":"workflow-1","run_id":"run-2"}}|});
  require_error
    (Protocol.decode_start_outcome ~request:start_request
       {|{"kind":"rejected","error":{"kind":"already_started","workflow_id":"other-workflow","existing_run_id":null}}|})

(** Keeps duplicate and unknown response fields from being accepted by either
    the JSON foundation or the operation-specific decoder. *)
let test_closed_response_shape () =
  let valid =
    {|{"execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1"}}|}
  in
  ignore (unwrap (Protocol.decode_start_response ~request:start_request valid));
  require_error
    (Protocol.decode_start_response ~request:start_request
       {|{"execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1"},"extra":true}|});
  require_error
    (Protocol.decode_start_response ~request:start_request
       {|{"execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1","run_id":"run-2"}}|});
  require_error
    (Protocol.decode_wait_response ~request:execution
       {|{"execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1"},"outcome":{"kind":"cancelled","details":[],"extra":true}}|})

(** Ensures successful responses cannot be attributed to a different execution,
    even when their JSON shape and identifiers are individually valid. *)
let test_response_execution_correlation () =
  let response =
    {|{"execution":{"namespace":"default","workflow_id":"other-workflow","run_id":"run-1"}}|}
  in
  require_error (Protocol.decode_start_response ~request:start_request response);
  let wait_response =
    {|{"execution":{"namespace":"default","workflow_id":"workflow-1","run_id":"run-2"},"outcome":{"kind":"cancelled","details":[]}}|}
  in
  require_error
    (Protocol.decode_wait_response ~request:execution wait_response)

(** Decodes structured native errors and rejects categories or codes outside
    the bilateral closed vocabulary. *)
let test_client_errors () =
  (match
     unwrap
       (Protocol.decode_client_error
          {|{"kind":"already_started","workflow_id":"workflow-1","existing_run_id":null}|})
   with
  | Protocol.Already_started { existing_run_id = None; _ } -> ()
  | _ -> failwith "already-started body changed shape");
  (match
     unwrap
       (Protocol.decode_client_error
          {|{"kind":"rpc","code":"deadline_exceeded"}|})
   with
  | Protocol.Rpc { code = "deadline_exceeded" } -> ()
  | _ -> failwith "rpc error code was not retained");
  (match
     unwrap
       (Protocol.decode_client_error
          {|{"kind":"protocol","code":"core_invalid"}|})
   with
  | Protocol.Protocol { code = "core_invalid" } -> ()
  | _ -> failwith "protocol error code was not retained");
  List.iter
    (fun document -> require_error (Protocol.decode_client_error document))
    [
      {|{"kind":"rpc","code":"not-a-real-code"}|};
      {|{"kind":"rpc","code":"ok"}|};
      {|{"kind":"protocol","code":"unsupported-future-code"}|};
      {|{"kind":"unknown","code":"internal"}|};
      {|{"kind":"rpc","code":"internal","extra":true}|};
    ]

(** Operation-specific error decoders retain the closed error body while
    correlating identities and rejecting impossible categories. *)
let test_operation_error_correlation () =
  let already_started =
    {|{"kind":"already_started","workflow_id":"workflow-1","existing_run_id":null}|}
  in
  ignore (unwrap (Protocol.decode_start_error ~request:start_request already_started));
  require_error
    (Protocol.decode_start_error ~request:start_request
       {|{"kind":"already_started","workflow_id":"other-workflow","existing_run_id":null}|});
  require_error
    (Protocol.decode_wait_error ~request:execution already_started)

(** Runs one protocol test with a stable CI-visible name. *)
let run name test =
  try
    test ();
    Printf.printf "PASS %s\n%!" name
  with exn ->
    Printf.eprintf "FAIL %s: %s\n%!" name (Printexc.to_string exn);
    exit 1

let () =
  run "client terminal outcomes" test_terminal_outcomes;
  run "client successor identity" test_successor_identity_validation;
  run "client request validation" test_request_validation;
  run "client asynchronous starts" test_async_start_protocol;
  run "client closed response shape" test_closed_response_shape;
  run "client response correlation" test_response_execution_correlation;
  run "client structured errors" test_client_errors;
  run "client operation error correlation" test_operation_error_correlation
