(** Focused tests for public workflow-context diagnostics and execution-local
    state. The two synthetic contexts model separate workflow runs and prove
    that a key shared by a workflow definition and its handler does not leak a
    value from one run into another. *)

module Scheduler = Temporal_runtime.Scheduler
module Context_store = Temporal_runtime.Workflow_context_store

(** Fails with a stable message when a test expectation is false. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Verifies that local-state operations fail as typed defects outside a
    workflow execution rather than silently using process-global storage. *)
let test_outside_execution () =
  let expect_outside label expected = function
    | Ok _ -> failwith (label ^ " unexpectedly succeeded")
    | Error error ->
        let view = Temporal.Error.view error in
        expect (label ^ " category") `Defect view.category;
        expect (label ^ " message") expected view.message
  in
  let local = Temporal.Workflow_context.Local.create () in
  expect_outside "outside get"
    "Temporal.Workflow_context.Local.get used outside a workflow execution"
    (Temporal.Workflow_context.Local.get local);
  expect_outside "outside set"
    "Temporal.Workflow_context.Local.set used outside a workflow execution"
    (Temporal.Workflow_context.Local.set local "value")

(** Verifies that one key has an independent value in each execution context,
    while repeated reads and writes within a context retain ordinary mutable
    workflow semantics. *)
let test_execution_isolation () =
  let local = Temporal.Workflow_context.Local.create () in
  let first_scheduler = Scheduler.create () in
  let second_scheduler = Scheduler.create () in
  let first = Context_store.create first_scheduler in
  let second = Context_store.create second_scheduler in
  expect "first starts empty" (Ok None)
    (Context_store.with_context first (fun () ->
         Temporal.Workflow_context.Local.get local));
  expect "first set" (Ok ())
    (Context_store.with_context first (fun () ->
         Temporal.Workflow_context.Local.set local "first"));
  expect "first reads own value" (Ok (Some "first"))
    (Context_store.with_context first (fun () ->
         Temporal.Workflow_context.Local.get local));
  expect "second remains empty" (Ok None)
    (Context_store.with_context second (fun () ->
         Temporal.Workflow_context.Local.get local));
  expect "second set" (Ok ())
    (Context_store.with_context second (fun () ->
         Temporal.Workflow_context.Local.set local "second"));
  expect "second reads own value" (Ok (Some "second"))
    (Context_store.with_context second (fun () ->
         Temporal.Workflow_context.Local.get local));
  expect "first remains isolated" (Ok (Some "first"))
    (Context_store.with_context first (fun () ->
         Temporal.Workflow_context.Local.get local));
  Context_store.shutdown first;
  Context_store.shutdown second

(** Runs the workflow-context contract tests. *)
let () =
  test_outside_execution ();
  test_execution_isolation ()
