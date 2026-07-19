(** Deterministic coverage for public external-workflow operations.

    The test installs the same execution context used by workflow activation
    tests, then observes the public futures and private command buffer. This
    keeps the assertion independent of a Temporal server while proving the
    boundary contract that live acceptance tests cannot isolate: command
    sequence allocation, typed completion, operation ownership, and validation
    before command emission. *)

module Activation = Temporal_runtime.Activation
module Scheduler = Temporal_runtime.Scheduler
module Workflow_context_store = Temporal_runtime.Workflow_context_store

(** Copies a public payload into the private representation carried by runtime
    commands, including its mutable byte buffer. *)
let private_payload (payload : Temporal.Payload.t) :
    Temporal_base.Codec.payload =
  {
    Temporal_base.Payload.metadata = List.map Fun.id payload.metadata;
    data = Bytes.copy payload.data;
  }

(** Compares two values and reports which lifecycle assertion diverged. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Checks that a private resolver rejected a malformed operation identity. *)
let expect_bridge_error label = function
  | Error error ->
      expect (label ^ " category") "bridge" (Temporal_base.Error.kind error)
  | Ok () -> failwith (label ^ " unexpectedly succeeded")

(** Checks the public error preserved by a ready future. *)
let expect_public_error label expected_kind expected_message = function
  | Some (Error error) ->
      expect (label ^ " category") expected_kind (Temporal.Error.kind error);
      expect (label ^ " message") expected_message (Temporal.Error.message error)
  | Some (Ok ()) -> failwith (label ^ " unexpectedly succeeded")
  | None -> failwith (label ^ " remained pending")

(** Encodes the signal payload once so the command assertion checks the exact
    bytes retained by the private runtime rather than only the signal metadata. *)
let encoded_signal_payload () =
  match Temporal.Codec.encode Temporal.Codec.string "ready" with
  | Ok payload -> private_payload payload
  | Error error -> failwith ("signal payload encoding failed: " ^ Temporal.Error.message error)

(** Proves that both public helpers allocate ordered commands and remain
    pending until the matching Core resolution arrives. Wrong-operation
    resolutions must leave the original future pending, while a duplicate after
    successful resolution must be rejected as a bridge defect. *)
let test_external_operation_lifecycle () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let signal =
    Temporal.Signal.define ~name:"refresh"
      ~input:Temporal.Codec.string
  in
  let signal_future =
    Workflow_context_store.with_context context (fun () ->
        Temporal.Workflow.signal_external_workflow
          ~workflow_id:"target-workflow" ~run_id:"run-42" ~signal ~input:"ready")
  in
  expect "signal starts pending" None (Temporal.Future.peek signal_future);
  let signal_payload = encoded_signal_payload () in
  expect "signal command"
    [ Activation.Signal_external_workflow
        {
          seq = 1L;
          workflow_id = "target-workflow";
          run_id = "run-42";
          signal_name = "refresh";
          input = [ signal_payload ];
          child_workflow_only = false;
          headers = [];
        } ]
    (Workflow_context_store.take_commands context);
  expect_bridge_error "signal resolved as cancellation"
    (Workflow_context_store.resolve_external_workflow context
       ~operation:`Cancel ~seq:1L (Ok ()));
  expect "signal remains pending after mismatched resolution" None
    (Temporal.Future.peek signal_future);
  let signal_failure =
    Temporal_base.Error.defect ~message:"target rejected signal"
  in
  expect "signal resolution"
    (Ok ())
    (Workflow_context_store.resolve_external_workflow context
       ~operation:`Signal ~seq:1L (Error signal_failure));
  expect_public_error "signal failure" "defect" "target rejected signal"
    (Temporal.Future.peek signal_future);
  expect_bridge_error "duplicate signal resolution"
    (Workflow_context_store.resolve_external_workflow context
       ~operation:`Signal ~seq:1L (Ok ()));

  let cancel_future =
    Workflow_context_store.with_context context (fun () ->
        Temporal.Workflow.cancel_external_workflow
          ~workflow_id:"target-workflow" ~run_id:"run-42"
          ~reason:"no longer needed")
  in
  expect "cancellation starts pending" None (Temporal.Future.peek cancel_future);
  expect "cancellation command"
    [ Activation.Request_cancel_external_workflow
        {
          seq = 2L;
          workflow_id = "target-workflow";
          run_id = "run-42";
          reason = "no longer needed";
        } ]
    (Workflow_context_store.take_commands context);
  expect "cancellation resolution"
    (Ok ())
    (Workflow_context_store.resolve_external_workflow context
       ~operation:`Cancel ~seq:2L (Ok ()));
  expect "cancellation completes" (Some (Ok ()))
    (Temporal.Future.peek cancel_future);
  Workflow_context_store.shutdown context

(** Proves that detached calls and invalid target/reason fields fail as typed
    defects without allocating a command sequence or mutating the buffer. *)
let test_external_operation_validation () =
  let signal =
    Temporal.Signal.define ~name:"refresh"
      ~input:Temporal.Codec.string
  in
  let detached =
    Temporal.Workflow.signal_external_workflow
      ~workflow_id:"target-workflow" ~run_id:"run-42" ~signal ~input:"ready"
  in
  expect_public_error "detached signal" "defect"
    "external workflow signal used outside a workflow execution"
    (Temporal.Future.peek detached);

  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let invalid_signal =
    Workflow_context_store.with_context context (fun () ->
        Temporal.Workflow.signal_external_workflow
          ~workflow_id:"" ~run_id:"run-42" ~signal ~input:"ready")
  in
  expect_public_error "empty external workflow ID" "defect"
    "external workflow id must not be empty"
    (Temporal.Future.peek invalid_signal);
  let invalid_cancellation =
    Workflow_context_store.with_context context (fun () ->
        Temporal.Workflow.cancel_external_workflow
          ~workflow_id:"target-workflow" ~run_id:"run-42" ~reason:"")
  in
  expect_public_error "empty cancellation reason" "defect"
    "external cancellation reason must not be empty"
    (Temporal.Future.peek invalid_cancellation);
  expect "invalid operations emit no commands" []
    (Workflow_context_store.take_commands context);
  Workflow_context_store.shutdown context

(** Runs the isolated external-operation lifecycle and validation scenarios. *)
let () =
  test_external_operation_lifecycle ();
  test_external_operation_validation ()
