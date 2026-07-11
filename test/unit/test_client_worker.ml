(** Exercises the public client and worker surface without a live Temporal
    server. The production adapter treats the in-memory backend as a private
    test transport; these assertions therefore cover typed API behavior while
    the Rust poll/complete implementation is developed independently. *)

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
         ~task_queue:"unit-test" ~id:"unit-echo" ~input:"hello")
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
       ~task_queue:"unit-test" ~id:"after-shutdown" ~input:"ignored")

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
       ~task_queue:"unit-test" ~id:"" ~input:"ignored");
  unwrap (Temporal.Client.shutdown client)

(** The worker task queue is required configuration and is passed through the
    private backend boundary before any worker graph is allocated. *)
let test_worker_validation_errors () =
  expect_error "defect"
    (Temporal.Worker.create ~target_url:"mock://dispatch"
       ~namespace:"unit-test" ~task_queue:"" ~workflows:[] ~activities:[] ())

let () =
  test_duplicate_workflows ();
  test_duplicate_activities ();
  test_remote_registration_is_rejected ();
  test_worker_registration_and_dispatch ();
  test_typed_start_and_wait_handle ();
  test_client_validation_errors ();
  test_worker_validation_errors ()
