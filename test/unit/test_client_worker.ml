(** Exercises the public client and worker surface without a live Temporal
    server. The [mock://] endpoint remains a deterministic unit-test seam;
    HTTP(S) routing is checked at the native configuration boundary so these
    tests do not require a running Temporal service. *)

(** Converts an expected SDK failure into a readable unit-test diagnostic. *)
let unwrap = function
  | Ok value -> value
  | Error error ->
      failwith
        (Printf.sprintf "%s: %s" (Temporal.Error.kind error)
           (Temporal.Error.message error))

(** Requires a structured failure of the requested category. *)
let expect_error category = function
  | Error error when Temporal.Error.kind error = category -> ()
  | Error error ->
      failwith
        (Printf.sprintf "expected %s, got %s" category
           (Temporal.Error.kind error))
  | Ok _ -> failwith (Printf.sprintf "expected %s error" category)

(** Finds [needle] in [source] without relying on optional string extensions.
    The helper is intentionally small because it is only used for stable error
    fragments in this test executable. *)
let contains_substring source needle =
  let source_length = String.length source in
  let needle_length = String.length needle in
  (* The bounded scan avoids depending on a newer String helper while making
     the assertion stable across the supported OCaml versions. *)
  let rec search index =
    if index + needle_length > source_length then false
    else if String.sub source index needle_length = needle then true
    else search (index + 1)
  in
  if needle_length = 0 then true else search 0

(** Requires a structured failure whose message retains a stable diagnostic
    fragment without coupling the test to the complete native wording. *)
let expect_error_message_contains category fragment = function
  | Error error
    when Temporal.Error.kind error = category
         && contains_substring (Temporal.Error.message error) fragment ->
      ()
  | Error error ->
      failwith
        (Printf.sprintf "expected %s error containing %S, got %s: %s" category
           fragment (Temporal.Error.kind error) (Temporal.Error.message error))
  | Ok _ ->
      failwith
        (Printf.sprintf "expected %s error containing %S" category fragment)

(** A string workflow used to prove that a client handle retains its input and
    output codecs across start and exact-run wait. *)
let echo_workflow =
  Temporal.Workflow.define ~name:"unit.echo"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun input ->
      Ok input)

(** Unit definitions keep the worker dispatch test independent of payload
    implementation details while still requiring decoding and re-encoding. *)
let unit_workflow calls =
  Temporal.Workflow.define ~name:"unit.workflow"
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
      Atomic.incr calls;
      Ok ())

(** A mock activity whose invocation is observable without process-global
    scheduler state or nondeterministic input. *)
let unit_activity calls =
  Temporal.Activity.define ~name:"unit.activity" ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.unit (fun () ->
      Atomic.incr calls;
      Ok ())

(** Definitions that return an expected failure. A worker must acknowledge
    these failures to the backend and continue serving later tasks. *)
let failing_workflow =
  Temporal.Workflow.define ~name:"unit.failing-workflow"
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
      Error (Temporal.Error.defect ~message:"synthetic workflow failure"))

(** Activity counterpart to [failing_workflow], used to check that an activity
    failure is acknowledged and does not stop later task dispatch. *)
let failing_activity =
  Temporal.Activity.define ~name:"unit.failing-activity"
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
      Error (Temporal.Error.defect ~message:"synthetic activity failure"))

(** Duplicate workflow names are rejected before a worker backend is started. *)
let test_duplicate_workflows () =
  let calls = Atomic.make 0 in
  let definition = unit_workflow calls in
  expect_error "defect"
    (Temporal.Worker.create ~target_url:"mock://dispatch"
       ~namespace:"unit-test" ~task_queue:"unit-test"
       ~workflows:
         [ Temporal.Worker.workflow definition; Temporal.Worker.workflow definition ]
       ~activities:[] ())

(** Duplicate activity names use the same registration invariant as workflows. *)
let test_duplicate_activities () =
  let calls = Atomic.make 0 in
  let definition = unit_activity calls in
  expect_error "defect"
    (Temporal.Worker.create ~target_url:"mock://dispatch"
       ~namespace:"unit-test" ~task_queue:"unit-test" ~workflows:[]
       ~activities:
         [ Temporal.Worker.activity definition; Temporal.Worker.activity definition ]
       ())

(** A remote-only reference cannot be registered as executable worker code. *)
let test_remote_registration_is_rejected () =
  let remote =
    Temporal.Workflow.remote ~name:"unit.remote" ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit
  in
  expect_error "defect"
    (Temporal.Worker.create ~target_url:"mock://dispatch"
       ~namespace:"unit-test" ~task_queue:"unit-test"
       ~workflows:[ Temporal.Worker.workflow remote ] ~activities:[] ())

(** The private fake poller supplies one workflow task and one activity task;
    [Worker.run] must decode, dispatch, encode, and acknowledge both before it
    observes the fake shutdown indication. *)
let test_worker_registration_and_dispatch () =
  let workflow_calls = Atomic.make 0 in
  let activity_calls = Atomic.make 0 in
  let workflow = unit_workflow workflow_calls in
  let activity = unit_activity activity_calls in
  let worker =
    unwrap
      (Temporal.Worker.create ~target_url:"mock://dispatch"
         ~namespace:"unit-test" ~task_queue:"unit-test"
         ~workflows:[ Temporal.Worker.workflow workflow ]
         ~activities:[ Temporal.Worker.activity activity ] ())
  in
  unwrap (Temporal.Worker.run worker);
  assert (Atomic.get workflow_calls = 1);
  assert (Atomic.get activity_calls = 1);
  unwrap (Temporal.Worker.shutdown worker);
  unwrap (Temporal.Worker.shutdown worker)

(** A task-level workflow or activity failure is not a worker-level failure:
    later successful tasks must still be dispatched after failure completion. *)
let test_worker_continues_after_task_failure () =
  let workflow_calls = Atomic.make 0 in
  let activity_calls = Atomic.make 0 in
  let worker =
    unwrap
      (Temporal.Worker.create ~target_url:"mock://dispatch"
         ~namespace:"unit-test" ~task_queue:"unit-test"
         ~workflows:
           [ Temporal.Worker.workflow failing_workflow;
             Temporal.Worker.workflow (unit_workflow workflow_calls) ]
         ~activities:
           [ Temporal.Worker.activity failing_activity;
             Temporal.Worker.activity (unit_activity activity_calls) ]
         ())
  in
  unwrap (Temporal.Worker.run worker);
  assert (Atomic.get workflow_calls = 1);
  assert (Atomic.get activity_calls = 1);
  unwrap (Temporal.Worker.shutdown worker)

(** A client start returns the server-issued run id and [wait] decodes the
    terminal payload using the definition's output codec. *)
let test_typed_start_and_wait_handle () =
  let client =
    unwrap
      (Temporal.Client.create ~target_url:"mock://client"
         ~namespace:"unit-test" ())
  in
  let handle =
    unwrap
      (Temporal.Client.start client ~workflow:echo_workflow
         ~request_id:"unit-start-1" ~task_queue:"unit-test" ~id:"unit-echo"
         ~input:"hello" ())
  in
  assert (Temporal.Client.workflow_id handle = "unit-echo");
  assert (String.length (Temporal.Client.run_id handle) > 0);
  (match Temporal.Client.wait handle with
  | Ok (Temporal.Client.Completed "hello") -> ()
  | Ok _ -> failwith "mock client returned an unexpected terminal result"
  | Error error -> failwith (Temporal.Error.message error));
  unwrap (Temporal.Client.shutdown client);
  unwrap (Temporal.Client.shutdown client);
  expect_error "bridge"
    (Temporal.Client.start client ~workflow:echo_workflow
       ~task_queue:"unit-test" ~id:"after-shutdown" ~input:"ignored" ())

(** Cancellation is acknowledged separately from waiting. The mock transport
    records the exact run cancellation, makes repeated requests idempotent,
    and exposes the eventual [Cancelled] value through the ordinary wait API. *)
let test_exact_run_cancellation () =
  let client =
    unwrap
      (Temporal.Client.create ~target_url:"mock://client"
         ~namespace:"unit-test" ())
  in
  let handle =
    unwrap
      (Temporal.Client.start client ~workflow:echo_workflow
         ~task_queue:"unit-test" ~id:"unit-cancel" ~input:"ignored" ())
  in
  unwrap
    (Temporal.Client.cancel ~request_id:"cancel-unit-1" ~reason:"unit test"
       handle);
  unwrap
    (Temporal.Client.cancel ~request_id:"cancel-unit-1" ~reason:"unit test"
       handle);
  (match Temporal.Client.wait handle with
  | Ok (Temporal.Client.Cancelled error) ->
      let view = Temporal.Error.view error in
      assert (Temporal.Error.kind error = "cancelled");
      assert (view.category = `Cancelled);
      assert (not view.non_retryable);
      assert (view.message = "workflow execution was cancelled")
  | Ok _ -> failwith "cancelled mock returned a non-cancelled terminal result"
  | Error error -> failwith (Temporal.Error.message error));
  unwrap (Temporal.Client.shutdown client)

(** A late cancellation request cannot rewrite a terminal result that the mock
    has already exposed. This protects the deterministic seam from modelling
    mutable terminal history unlike a real Temporal execution. *)
let test_completed_mock_run_is_immutable () =
  let client =
    unwrap
      (Temporal.Client.create ~target_url:"mock://client"
         ~namespace:"unit-test" ())
  in
  let handle =
    unwrap
      (Temporal.Client.start client ~workflow:echo_workflow
         ~task_queue:"unit-test" ~id:"unit-completed-cancel" ~input:"done"
         ())
  in
  (match Temporal.Client.wait handle with
  | Ok (Temporal.Client.Completed "done") -> ()
  | Ok _ -> failwith "mock completed run returned an unexpected first result"
  | Error error -> failwith (Temporal.Error.message error));
  unwrap
    (Temporal.Client.cancel ~request_id:"late-cancel" ~reason:"too late"
       handle);
  (match Temporal.Client.wait handle with
  | Ok (Temporal.Client.Completed "done") -> ()
  | Ok _ -> failwith "late cancellation rewrote a completed mock run"
  | Error error -> failwith (Temporal.Error.message error));
  unwrap (Temporal.Client.shutdown client)

(** Invalid client settings are values rather than exceptions, and a malformed
    durable workflow id is rejected before the backend receives it. *)
let test_client_validation_errors () =
  expect_error "defect"
    (Temporal.Client.create ~target_url:"not-a-url" ~namespace:"unit-test" ());
  let client =
    unwrap
      (Temporal.Client.create ~target_url:"mock://client"
         ~namespace:"unit-test" ())
  in
  expect_error "defect"
    (Temporal.Client.start client ~workflow:echo_workflow
       ~task_queue:"unit-test" ~id:"" ~input:"ignored" ());
  expect_error "defect"
    (Temporal.Client.start client ~workflow:echo_workflow ~request_id:""
       ~task_queue:"unit-test" ~id:"valid-id" ~input:"ignored" ());
  expect_error "defect"
    (Temporal.Client.start client ~workflow:echo_workflow
       ~request_id:"contains\000nul" ~task_queue:"unit-test" ~id:"valid-id"
       ~input:"ignored" ());
  let handle =
    unwrap
      (Temporal.Client.start client ~workflow:echo_workflow
         ~task_queue:"unit-test" ~id:"cancel-validation" ~input:"ignored" ())
  in
  expect_error "defect"
    (Temporal.Client.cancel ~request_id:"" handle);
  expect_error "defect"
    (Temporal.Client.cancel ~reason:"contains\000nul" handle);
  unwrap (Temporal.Client.shutdown client)

(** An HTTP-shaped endpoint is deliberately handed to the native configuration
    validator rather than the deterministic mock. The malformed host fails
    before a runtime or network connection is allocated, proving the public
    routing decision without needing Temporal Server in this unit executable.
*)
let test_native_client_configuration_boundary () =
  expect_error_message_contains "bridge" "native client configuration failed"
    (Temporal.Client.create ~target_url:"http://" ~namespace:"unit-test" ())

(** The worker task queue is required configuration and is passed through the
    private backend boundary before any worker graph is allocated. *)
let test_worker_validation_errors () =
  expect_error "defect"
    (Temporal.Worker.create ~target_url:"mock://dispatch"
       ~namespace:"unit-test" ~task_queue:"" ~workflows:[] ~activities:[] ())

(** A non-mock endpoint must enter the native configuration boundary rather
    than silently falling back to the deterministic fake. An invalid absolute
    URL fails before a runtime or network resource is allocated, which keeps
    this assertion independent from Temporal Server availability. *)
let test_native_worker_configuration_boundary () =
  expect_error "bridge"
    (Temporal.Worker.create ~target_url:"http://"
       ~namespace:"unit-test" ~task_queue:"unit-test" ~workflows:[]
       ~activities:[] ())

(** Runs all public worker and client regression assertions. *)
let () =
  test_duplicate_workflows ();
  test_duplicate_activities ();
  test_remote_registration_is_rejected ();
  test_worker_registration_and_dispatch ();
  test_worker_continues_after_task_failure ();
  test_typed_start_and_wait_handle ();
  test_exact_run_cancellation ();
  test_completed_mock_run_is_immutable ();
  test_client_validation_errors ();
  test_native_client_configuration_boundary ();
  test_worker_validation_errors ();
  test_native_worker_configuration_boundary ()
