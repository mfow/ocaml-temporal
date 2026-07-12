module Protocol = Temporal_protocol.Workflow_protocol

(** Reads a complete shared semantic fixture and closes its descriptor on all
    paths. *)
let read_file path =
  let channel = open_in_bin path in
  Fun.protect
    ~finally:(fun () -> close_in_noerr channel)
    (fun () -> really_input_string channel (in_channel_length channel))

(** Resolves a fixture under Dune's copied workflow-protocol source tree. *)
let fixture parts =
  List.fold_left Filename.concat "fixtures/workflow-protocol" parts |> read_file

(** Extracts a successful protocol result for positive fixtures. *)
let unwrap = function
  | Ok value -> value
  | Error error ->
      let view = Protocol.error_view error in
      failwith (Printf.sprintf "%s at %s: %s" view.code view.path view.message)

(** Requires malformed input to fail without rendering its potentially
    sensitive payload bytes. *)
let require_error = function
  | Error _ -> ()
  | Ok _ -> failwith "expected workflow protocol validation to fail"

(** Compares normalized JSON output exactly. *)
let check_string label expected actual =
  if not (String.equal expected actual) then failwith (label ^ " differed")

(** Verifies activation variants, ordering, and outgoing self-validation using
    the same fixtures as Rust. *)
let test_valid_activations () =
  List.iter
    (fun name ->
      let value =
        unwrap
          (Protocol.decode_activation
             (fixture [ "valid"; name ^ ".input.json" ]))
      in
      let expected =
        String.trim (fixture [ "valid"; name ^ ".normalized.json" ])
      in
      check_string name expected (unwrap (Protocol.encode_activation value));
      ignore (unwrap (Protocol.decode_activation expected)))
    [ "activation"; "eviction"; "realistic-initialize"; "child-initialize" ]

(** Verifies every first-slice completion command and command ordering. *)
let test_valid_completion () =
  let value =
    unwrap
      (Protocol.decode_completion
         (fixture [ "valid"; "completion.input.json" ]))
  in
  let expected =
    String.trim (fixture [ "valid"; "completion.normalized.json" ])
  in
  check_string "completion" expected (unwrap (Protocol.encode_completion value));
  ignore (unwrap (Protocol.decode_completion expected))

(** Proves the child-workflow start command uses the same canonical payload
    representation as activities while retaining its workflow identity. *)
let test_start_child_workflow_command () =
  let input : Protocol.payload =
    {
      metadata = [ ("encoding", Bytes.of_string "binary/null") ];
      data = Bytes.empty;
    }
  in
  let completion : Protocol.completion =
    {
      run_id = "parent-run";
      commands =
        [
          Start_child_workflow
            {
              seq = 2L;
              workflow_id = "child/1";
              workflow_type = "child";
              input = [ input ];
            };
        ];
    }
  in
  let encoded = unwrap (Protocol.encode_completion completion) in
  check_string "child command"
    {|{"commands":[{"input":[{"data":{"data":"","encoding":"base64"},"metadata":{"encoding":{"data":"YmluYXJ5L251bGw=","encoding":"base64"}}}],"kind":"start_child_workflow","seq":2,"workflow_id":"child/1","workflow_type":"child"}],"run_id":"parent-run"}|}
    encoded;
  if unwrap (Protocol.decode_completion encoded) <> completion then
    failwith "child command did not round-trip"

(** Proves closed nested objects, numeric bounds, canonical binary data, and
    workflow semantic invariants are rejected identically by both languages. *)
let test_invalid_documents () =
  List.iter
    (fun name ->
      require_error
        (Protocol.decode_activation
           (fixture [ "invalid"; name ^ ".json" ])))
    [
      "activation-duplicate-field";
      "activation-unknown-job";
      "activation-seq-negative";
      "activation-seq-too-large";
      "activation-invalid-base64";
      "activation-missing-field";
      "activation-eviction-mixed";
    ];
  List.iter
    (fun name ->
      require_error
        (Protocol.decode_completion
           (fixture [ "invalid"; name ^ ".json" ])))
    [
      "completion-unknown-command";
      "completion-terminal-not-last";
      "completion-invalid-duration";
      "completion-no-activity-timeout";
      "completion-unknown-nested";
      "completion-duplicate-field";
    ]

(** Proves the semantic parser admits payload data above the normal text limit
    while preserving that limit for ordinary user-visible text. *)
let test_large_nested_payload () =
  let payload : Protocol.payload =
    { metadata = []; data = Bytes.make 50_000 'x' }
  in
  let completion : Protocol.completion =
    {
      run_id = "run-large";
      commands = [ Complete_workflow { result = Some payload } ];
    }
  in
  let encoded = unwrap (Protocol.encode_completion completion) in
  ignore (unwrap (Protocol.decode_completion encoded));
  let oversized_reason = String.make 65_537 'x' in
  require_error
    (Protocol.decode_activation
       (Yojson.Safe.to_string
          (`Assoc
            [
              ("run_id", `String "r");
              ("timestamp", `Assoc [ ("seconds", `Int 0); ("nanoseconds", `Int 0) ]);
              ("is_replaying", `Bool false);
              ("history_length", `Int 0);
              ( "jobs",
                `List
                  [
                    `Assoc
                      [
                        ("kind", `String "cancel_workflow");
                        ("reason", `String oversized_reason);
                      ];
                  ] );
            ])))

(** Proves association-list order cannot affect deterministic wire output. *)
let test_metadata_key_canonicalization () =
  let payload : Protocol.payload =
    {
      metadata = [ ("z-key", Bytes.of_string "z"); ("a-key", Bytes.of_string "a") ];
      data = Bytes.empty;
    }
  in
  let completion : Protocol.completion =
    {
      run_id = "run-map";
      commands = [ Complete_workflow { result = Some payload } ];
    }
  in
  let encoded = unwrap (Protocol.encode_completion completion) in
  let find needle =
    let rec loop offset =
      if offset + String.length needle > String.length encoded then
        failwith ("missing normalized key " ^ needle)
      else if String.sub encoded offset (String.length needle) = needle then offset
      else loop (offset + 1)
    in
    loop 0
  in
  let a = find "\"a-key\"" in
  let z = find "\"z-key\"" in
  if a >= z then failwith "metadata keys were not normalized lexicographically";
  ignore (unwrap (Protocol.decode_completion encoded))

(** Proves configurable Temporal identifiers are not constrained by an
    invented server-default limit while the protocol safety ceiling remains
    enforced. *)
let test_identifier_safety_limit () =
  let long_id = String.make 300 'i' in
  let completion : Protocol.completion = { run_id = long_id; commands = [] } in
  let encoded = unwrap (Protocol.encode_completion completion) in
  let decoded = unwrap (Protocol.decode_completion encoded) in
  check_string "long identifier" long_id decoded.run_id;
  require_error
    (Protocol.encode_completion
       { run_id = String.make 65_537 'i'; commands = [] })

(** Proves Core initialization ordering and nullable eviction timestamps are
    checked independently of structural JSON validation. *)
let test_activation_cross_field_invariants () =
  let initialized =
    unwrap
      (Protocol.decode_activation
         (fixture [ "valid"; "realistic-initialize.input.json" ]))
  in
  let initialize = List.hd initialized.jobs in
  require_error
    (Protocol.encode_activation
       { initialized with jobs = [ initialize; initialize ] });
  require_error
    (Protocol.encode_activation
       {
         initialized with
         jobs = [ Fire_timer { seq = 0L }; initialize ];
       });
  require_error (Protocol.encode_activation { initialized with timestamp = None });
  let zero_sequence =
    {
      initialized with
      jobs = [ Fire_timer { seq = 0L } ];
    }
  in
  let encoded = unwrap (Protocol.encode_activation zero_sequence) in
  ignore (unwrap (Protocol.decode_activation encoded));
  let eviction =
    unwrap
      (Protocol.decode_activation
         (fixture [ "valid"; "eviction.input.json" ]))
  in
  match eviction.timestamp with
  | None -> ()
  | Some _ -> failwith "official Core eviction timestamp must remain absent"

(** Proves collection accounting does not impose a smaller workflow-job limit
    than the aggregate document boundary itself. *)
let test_large_activation_job_batch () =
  let activation =
    unwrap
      (Protocol.decode_activation
         (fixture [ "valid"; "realistic-initialize.input.json" ]))
  in
  let jobs = List.init 300 (fun seq -> Protocol.Fire_timer { seq = Int64.of_int seq }) in
  let value = { activation with jobs } in
  let encoded = unwrap (Protocol.encode_activation value) in
  let decoded = unwrap (Protocol.decode_activation encoded) in
  if List.length decoded.jobs <> 300 then failwith "large activation job batch was truncated"

(** Builds a failure whose nested info can vary independently of the common
    recursive failure fields. *)
let failure_with_info info : Protocol.failure =
  {
    message = "";
    source = "";
    stack_trace = "";
    encoded_attributes = None;
    cause = None;
    info;
  }

(** Builds a recursive application-failure chain used to exercise the shared
    parser's stack-safety depth boundary. *)
let nested_application_failure cause_count =
  let rec loop remaining cause =
    if remaining = 0 then cause
    else
      loop (remaining - 1)
        {
          (failure_with_info
             (Application
                { type_name = ""; non_retryable = false; details = [] }))
          with
          cause = Some cause;
        }
  in
  loop cause_count
    (failure_with_info
       (Application { type_name = ""; non_retryable = false; details = [] }))

(** Proves recursive failures can exceed the former 16-level limit while the
    serde_json-aligned stack-safety boundary still rejects hostile depth. *)
let test_recursive_failure_depth () =
  let completion cause_count : Protocol.completion =
    {
      run_id = "run-nested-failure";
      commands =
        [ Fail_workflow { failure = nested_application_failure cause_count } ];
    }
  in
  ignore (unwrap (Protocol.encode_completion (completion 32)));
  require_error (Protocol.encode_completion (completion 130))

(** Proves application types are bounded text and activity failures reject
    negative event IDs and oversized identities on outgoing validation. *)
let test_failure_field_semantics () =
  let application : Protocol.completion =
    {
      run_id = "run-failure";
      commands =
        [
          Fail_workflow
            {
              failure =
                failure_with_info
                  (Application
                     { type_name = ""; non_retryable = false; details = [] });
            };
        ];
    }
  in
  ignore (unwrap (Protocol.encode_completion application));
  let invalid_activity scheduled_event_id started_event_id identity =
    ({
       run_id = "run-failure";
       commands =
         [
           Fail_workflow
             {
               failure =
                 failure_with_info
                   (Activity
                      {
                        scheduled_event_id;
                        started_event_id;
                        identity;
                        activity_type = "activity";
                        activity_id = "activity-1";
                        retry_state = Unspecified;
                      });
             };
         ];
     }
      : Protocol.completion)
  in
  require_error (Protocol.encode_completion (invalid_activity (-1L) 0L ""));
  require_error (Protocol.encode_completion (invalid_activity 0L (-1L) ""));
  require_error
    (Protocol.encode_completion
       (invalid_activity 0L 0L (String.make 65_537 'i')))

(** Proves payload-aware parsing does not permit invalid initialization header
    keys through sender-side validation. *)
let test_initialize_header_keys () =
  let activation =
    unwrap
      (Protocol.decode_activation
         (fixture [ "valid"; "realistic-initialize.input.json" ]))
  in
  let jobs =
    match activation.jobs with
    | Protocol.Initialize_workflow value :: rest ->
        let context = Option.get value.context in
        let payload : Protocol.payload = { metadata = []; data = Bytes.empty } in
        Protocol.Initialize_workflow
          {
            value with
            context = Some { context with headers = [ ("", payload) ] };
          }
        :: rest
    | _ -> failwith "fixture must start with initialization"
  in
  require_error (Protocol.encode_activation { activation with jobs })

(** Proves two server-default-sized payload byte fields, and their base64
    expansion, fit in one validated semantic document. *)
let test_batched_default_temporal_payloads () =
  let bytes = Bytes.make (2 * 1024 * 1024) 'x' in
  let payload : Protocol.payload =
    { metadata = [ ("second", Bytes.copy bytes) ]; data = bytes }
  in
  let completion : Protocol.completion =
    {
      run_id = "run-batched-payloads";
      commands = [ Complete_workflow { result = Some payload } ];
    }
  in
  let encoded = unwrap (Protocol.encode_completion completion) in
  ignore (unwrap (Protocol.decode_completion encoded))

(** Proves required-nullable members are not interchangeable with omission on
    the OCaml side of the bilateral contract. *)
let test_required_nullable_fields () =
  require_error
    (Protocol.decode_completion
       {|{"run_id":"run-required-null","commands":[{"kind":"complete_workflow"}]}|});
  require_error
    (Protocol.decode_completion
       {|{"run_id":"run-required-null","commands":[{"kind":"schedule_activity","seq":0,"activity_id":"activity","activity_type":"activity","task_queue":"queue","arguments":[],"schedule_to_start_timeout":null,"start_to_close_timeout":null,"heartbeat_timeout":null,"cancellation_type":"try_cancel","do_not_eagerly_execute":false}]}|})

(** Runs one test with a stable name suitable for CI logs. *)
let run name test =
  try
    test ();
    Printf.printf "PASS %s\n%!" name
  with exn ->
    Printf.eprintf "FAIL %s: %s\n%!" name (Printexc.to_string exn);
    exit 1

let () =
  run "workflow activations" test_valid_activations;
  run "workflow completion" test_valid_completion;
  run "start child workflow command" test_start_child_workflow_command;
  run "malformed workflow documents" test_invalid_documents;
  run "large nested payload" test_large_nested_payload;
  run "metadata key canonicalization" test_metadata_key_canonicalization;
  run "identifier safety limit" test_identifier_safety_limit;
  run "activation cross-field invariants" test_activation_cross_field_invariants;
  run "large activation job batch" test_large_activation_job_batch;
  run "failure field semantics" test_failure_field_semantics;
  run "recursive failure depth" test_recursive_failure_depth;
  run "initialize header keys" test_initialize_header_keys;
  run "batched default Temporal payloads" test_batched_default_temporal_payloads;
  run "required nullable fields" test_required_nullable_fields
