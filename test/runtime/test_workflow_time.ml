(** Tests for the deterministic workflow clock exposed by [Temporal.Workflow].

    These tests install the same private context that the native activation
    adapter uses, allowing the public API to be checked without a Temporal
    server. They deliberately exercise both the detached error path and the
    exact integer timestamp conversion used during replay. *)

module Protocol = Temporal_protocol.Workflow_protocol
module Scheduler = Temporal_runtime.Scheduler
module Workflow_context_store = Temporal_runtime.Workflow_context_store

(** Fails with a short scenario-specific message when two values differ. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Checks the stable error category and diagnostic returned by [Workflow.now]. *)
let expect_error label expected_kind expected_message = function
  | Ok _ -> failwith (label ^ " unexpectedly succeeded")
  | Error error ->
      expect (label ^ " kind") expected_kind (Temporal.Error.kind error);
      expect (label ^ " message") expected_message (Temporal.Error.message error)

(** Verifies that detached workflow code cannot accidentally read host time. *)
let test_now_outside_workflow () =
  expect_error "detached now" "defect"
    "Temporal.Workflow.now used outside a workflow execution"
    (Temporal.Workflow.now ())

(** Verifies that one activation timestamp round-trips without float rounding. *)
let test_now_round_trip () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let timestamp : Protocol.timestamp = { seconds = -12L; nanoseconds = 345_678_901 } in
  Workflow_context_store.set_activation_timestamp context (Some timestamp);
  let observed = ref None in
  Workflow_context_store.with_context context (fun () ->
      observed := Some (Temporal.Workflow.now ())) ;
  begin match !observed with
  | Some (Ok instant) ->
      expect "seconds" timestamp.seconds (Temporal.Time.seconds instant);
      expect "nanoseconds" timestamp.nanoseconds (Temporal.Time.nanoseconds instant);
      if not (Temporal.Time.equal instant instant) then
        failwith "a timestamp was not equal to itself"
  | Some (Error error) ->
      failwith ("timestamp unexpectedly failed: " ^ Temporal.Error.message error)
  | None -> failwith "workflow clock was not observed"
  end;
  Workflow_context_store.shutdown context

(** Verifies that synthetic activations do not leave an earlier timestamp
    visible to later workflow code. *)
let test_missing_activation_timestamp () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let observed = ref None in
  Workflow_context_store.with_context context (fun () ->
      observed := Some (Temporal.Workflow.now ())) ;
  expect_error "missing timestamp" "defect"
    "Temporal.Workflow.now is unavailable for this activation"
    (Option.get !observed);
  Workflow_context_store.shutdown context

(** Verifies that invalid fractional components are rejected as typed defects. *)
let test_invalid_timestamp_fraction () =
  begin match Temporal.Time.of_unix ~seconds:0L ~nanoseconds:(-1) with
  | Ok _ -> failwith "negative nanoseconds were accepted"
  | Error error ->
      expect "invalid fraction kind" "defect" (Temporal.Error.kind error)
  end;
  begin match Temporal.Time.of_unix ~seconds:0L ~nanoseconds:1_000_000_000 with
  | Ok _ -> failwith "one-second nanoseconds were accepted"
  | Error error ->
      expect "upper-bound fraction kind" "defect" (Temporal.Error.kind error)
  end

(** Runs all deterministic clock scenarios as one dune test executable. *)
let () =
  test_now_outside_workflow ();
  test_now_round_trip ();
  test_missing_activation_timestamp ();
  test_invalid_timestamp_fraction ()
