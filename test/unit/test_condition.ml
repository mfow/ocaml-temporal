(** Public API tests for [Temporal.Condition].  Runtime tests cover the
    package-private store; these scenarios verify public error conversion and
    the two predicate forms available to workflow authors. *)

module Scheduler = Temporal_runtime.Scheduler
module Workflow_context_store = Temporal_runtime.Workflow_context_store

(** Compares values while identifying the failed public scenario. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Checks a public error's category and stable diagnostic. *)
let expect_error label expected_kind expected_message = function
  | Ok () -> failwith (label ^ " unexpectedly succeeded")
  | Error error ->
      expect (label ^ " kind") expected_kind (Temporal.Error.kind error);
      expect (label ^ " message") expected_message (Temporal.Error.message error)

(** Confirms detached calls return typed defects and retain no scheduler
    continuation or condition-store registration. *)
let test_outside_workflow () =
  expect_error "condition outside workflow" "defect"
    "Temporal.Condition.wait_until used outside a workflow execution"
    (Temporal.Condition.wait_until (fun () -> true));
  expect_error "result condition outside workflow" "defect"
    "Temporal.Condition.wait_until used outside a workflow execution"
    (Temporal.Condition.wait_until_result (fun () -> Ok true))

(** Runs an immediate public boolean condition on its owning scheduler. *)
let test_immediate_boolean_condition () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let result = ref None in
  Scheduler.spawn scheduler (fun () ->
      Workflow_context_store.with_context context (fun () ->
          result := Some (Temporal.Condition.wait_until (fun () -> true))));
  expect "public immediate status" "complete" (Scheduler.run_label scheduler);
  expect "public immediate result" (Some (Ok ())) !result;
  Workflow_context_store.shutdown context

(** Confirms that a result-aware predicate can return an application-supplied
    typed error without raising or being rewritten as a scheduler failure. *)
let test_result_predicate_error () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let result = ref None in
  Scheduler.spawn scheduler (fun () ->
      Workflow_context_store.with_context context (fun () ->
          let predicate () =
            Error (Temporal.Error.codec ~message:"state is malformed")
          in
          result := Some (Temporal.Condition.wait_until_result predicate)));
  expect "public predicate error status" "complete"
    (Scheduler.run_label scheduler);
  expect_error "public predicate error" "codec" "state is malformed"
    (Option.get !result);
  Workflow_context_store.shutdown context

(** Confirms that exceptions in the public convenience predicate become typed
    defects and do not escape the effect scheduler. *)
let test_boolean_predicate_exception () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let result = ref None in
  Scheduler.spawn scheduler (fun () ->
      Workflow_context_store.with_context context (fun () ->
          result :=
            Some
              (Temporal.Condition.wait_until (fun () -> raise Exit))));
  expect "public predicate exception status" "complete"
    (Scheduler.run_label scheduler);
  match Option.get !result with
  | Ok () -> failwith "public predicate exception unexpectedly succeeded"
  | Error error ->
      expect "public predicate exception kind" "defect"
        (Temporal.Error.kind error);
  Workflow_context_store.shutdown context

(** Runs all public condition scenarios as one dune test executable. *)
let () =
  test_outside_workflow ();
  test_immediate_boolean_condition ();
  test_result_predicate_error ();
  test_boolean_predicate_exception ()
