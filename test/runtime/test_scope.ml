(** Tests for the public workflow-local cancellation scope.

    The fixtures use the deterministic scheduler directly so they can resolve
    a source future and request scope cancellation in controlled FIFO order.
    No Temporal server or native bridge is involved: this slice proves the
    scope's typed observation and ownership contract before server-side
    cancellation commands are added. *)

module Scheduler = Temporal_runtime.Scheduler
module Workflow_context_store = Temporal_runtime.Workflow_context_store

(** Supplies the structured defect expected when a test future is awaited
    without its owning scheduler. *)
let outside_error () = Temporal.Error.defect ~message:"outside scheduler"

(** Adapts an internal test promise to the public future façade without
    exposing that construction path to installed SDK consumers. *)
let public_future future =
  Temporal_future_kernel.make
    ~await:(fun () -> Temporal_runtime.Future_store.await future)
    ~await_gate:(fun register ->
      Temporal_runtime.Future_store.await_gate future register)
    ~observe:(Temporal_runtime.Future_store.observe future)
    ~is_ready:(fun () -> Temporal_runtime.Future_store.is_ready future)
    ~peek:(fun () -> Temporal_runtime.Future_store.peek future)
    ~owner_id:(Temporal_runtime.Future_store.owner_id future)
    ~outside_error
    ~callbacks_live:(fun () ->
      Temporal_runtime.Future_store.callbacks_live future)
    ~enqueue:(Temporal_runtime.Future_store.enqueue future)

(** Creates a scheduler-owned public promise and retains its internal resolver
    so a test fiber can model a later Temporal activation. *)
let promise scheduler =
  let future, resolve = Scheduler.promise scheduler ~outside_error in
  (public_future future, resolve)

(** Compares values while attaching the scenario name to failures. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Checks a public error's stable category and message. *)
let expect_error label expected_kind expected_message = function
  | Error error ->
      expect (label ^ " kind") expected_kind (Temporal.Error.kind error);
      expect (label ^ " message") expected_message (Temporal.Error.message error)
  | Ok _ -> failwith (label ^ " unexpectedly succeeded")

(** Creates a workflow context and runs [action] with it installed while the
    scheduler is already active. The caller remains responsible for teardown. *)
let with_active_context scheduler context action =
  Scheduler.spawn scheduler (fun () ->
      Workflow_context_store.with_context context action)

(** A scope cannot be created without a current workflow execution. *)
let test_create_outside_workflow () =
  expect_error "create outside workflow" "defect"
    "Temporal.Scope.create used outside a workflow execution"
    (Temporal.Scope.create ())

(** A completed operation wins when the scope is still active, and [with_scope]
    cancels its private signal during cleanup without emitting a command. *)
let test_completed_future_and_cleanup () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let source, resolve = promise scheduler in
  resolve (Ok 42);
  let result = ref None in
  let retained_scope = ref None in
  let status_result = ref None in
  let check_result = ref None in
  with_active_context scheduler context (fun () ->
      result :=
        Some
          (Temporal.Scope.with_scope (fun scope ->
               retained_scope := Some scope;
               Temporal.Scope.await scope source)));
  expect "completed scope run" "complete" (Scheduler.run_label scheduler);
  expect "completed future result" (Some (Ok 42)) !result;
  (* The cleanup callback ran in the first owner turn. Querying the retained
     scope therefore has to be scheduled in a second owner turn instead of
     reading its mutable state from the test Domain. *)
  with_active_context scheduler context (fun () ->
      match !retained_scope with
      | Some scope ->
          status_result := Some (Temporal.Scope.is_cancelled scope);
          check_result := Some (Temporal.Scope.check scope)
      | None -> failwith "scope was not retained");
  expect "scope status query run" "complete" (Scheduler.run_label scheduler);
  let stale_owner_status = ref None in
  with_active_context scheduler context (fun () ->
      match !retained_scope with
      | Some scope ->
          (* Exercise the teardown boundary while the owner ID is still
             published. [callbacks_live] must reject the stale handle even
             though a simple owner-ID comparison would still match. *)
          Workflow_context_store.shutdown context;
          stale_owner_status := Some (Temporal.Scope.is_cancelled scope)
      | None -> failwith "scope was not retained");
  expect "scope shutdown query run" "complete" (Scheduler.run_label scheduler);
  begin match !retained_scope with
  | Some scope ->
      expect "with_scope cancellation status" (Some (Ok true)) !status_result;
      expect_error "cleaned scope check" "cancelled" "workflow scope cancelled"
        (Option.get !check_result);
      expect_error "scope status during shutdown" "defect"
        "Temporal.Scope.is_cancelled used outside its owning workflow scheduler"
        (Option.get !stale_owner_status);
      (* Once the scheduler is closed, even a call made by code that happens
         to retain the old handle must fail as a stale-handle defect. *)
      expect_error "stale scope status" "defect"
        "Temporal.Scope.is_cancelled used outside its owning workflow scheduler"
        (Temporal.Scope.is_cancelled scope);
      expect_error "stale scope check" "defect"
        "Temporal.Scope.check used outside its owning workflow scheduler"
        (Temporal.Scope.check scope);
      expect_error "stale scope cancellation" "defect"
        "Temporal.Scope.cancel used outside its owning workflow scheduler"
        (Temporal.Scope.cancel scope)
  | None -> failwith "scope was not retained"
  end;
  expect "scope emitted no command" []
    (Workflow_context_store.take_commands context)

(** A second workflow fiber can cancel a scope while the first fiber is paused
    in [Scope.await]. The cancellation result is typed, deterministic, and does
    not falsely mark the underlying operation as resolved. *)
let test_cancellation_resumes_waiter () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let source, _resolve_source = promise scheduler in
  let result = ref None in
  let cancel_result = ref None in
  let check_result = ref None in
  let scope_ref = ref None in
  with_active_context scheduler context (fun () ->
      match Temporal.Scope.create () with
      | Error error -> result := Some (Error error)
      | Ok scope ->
          scope_ref := Some scope;
          result := Some (Temporal.Scope.await scope source));
  expect "initial cancellation wait" "blocked"
    (Scheduler.run_label scheduler);
  begin match !scope_ref with
  | None -> failwith "scope was not created before the first run"
  | Some scope ->
      expect_error "off-scheduler cancellation" "defect"
        "Temporal.Scope.cancel used outside its owning workflow scheduler"
        (Temporal.Scope.cancel scope);
      expect_error "off-scheduler status" "defect"
        "Temporal.Scope.is_cancelled used outside its owning workflow scheduler"
        (Temporal.Scope.is_cancelled scope)
  end;
  Scheduler.spawn scheduler (fun () ->
      match !scope_ref with
      | None -> failwith "cancellation fiber ran before scope creation"
      | Some scope ->
          cancel_result := Some (Temporal.Scope.cancel scope);
          check_result := Some (Temporal.Scope.check scope));
  expect "cancellation scheduler status" "blocked"
    (Scheduler.run_label scheduler);
  expect_error "cancelled await" "cancelled" "workflow scope cancelled"
    (Option.get !result);
  expect "cancel operation" (Some (Ok ())) !cancel_result;
  expect_error "cancelled check" "cancelled" "workflow scope cancelled"
    (Option.get !check_result);
  if Temporal.Future.is_ready source then
    failwith "scope cancellation incorrectly resolved the source future";
  expect "scope cancellation emitted no command" []
    (Workflow_context_store.take_commands context);
  Workflow_context_store.shutdown context

(** Repeated cancellation on the owner scheduler is idempotent. The first
    request resolves the private signal, while the retry must not attempt to
    resolve it a second time or emit any Temporal command. *)
let test_cancellation_is_idempotent () =
  let scheduler = Scheduler.create () in
  let context = Workflow_context_store.create scheduler in
  let first = ref None in
  let second = ref None in
  let status = ref None in
  let check = ref None in
  with_active_context scheduler context (fun () ->
      match Temporal.Scope.create () with
      | Error error -> failwith (Temporal.Error.message error)
      | Ok scope ->
          first := Some (Temporal.Scope.cancel scope);
          second := Some (Temporal.Scope.cancel scope);
          status := Some (Temporal.Scope.is_cancelled scope);
          check := Some (Temporal.Scope.check scope));
  expect "idempotent cancellation run" "complete"
    (Scheduler.run_label scheduler);
  expect "first cancellation" (Some (Ok ())) !first;
  expect "second cancellation" (Some (Ok ())) !second;
  expect "idempotent cancellation status" (Some (Ok true)) !status;
  expect_error "idempotent cancellation check" "cancelled"
    "workflow scope cancelled" (Option.get !check);
  expect "scope cancellation emits no command" []
    (Workflow_context_store.take_commands context);
  Workflow_context_store.shutdown context

(** A scope rejects a future owned by another workflow execution as a typed
    defect instead of allowing one scheduler to await another's continuation. *)
let test_cross_execution_future_is_rejected () =
  let first_scheduler = Scheduler.create () in
  let first_context = Workflow_context_store.create first_scheduler in
  let second_scheduler = Scheduler.create () in
  let second_context = Workflow_context_store.create second_scheduler in
  let foreign, _resolve_foreign = promise second_scheduler in
  let result = ref None in
  let scope_ref = ref None in
  with_active_context first_scheduler first_context (fun () ->
      match Temporal.Scope.create () with
      | Error error -> result := Some (Error error)
      | Ok scope ->
          scope_ref := Some scope;
          result := Some (Temporal.Scope.await scope foreign);
          (* The owner must perform cleanup while its scheduler is active;
             otherwise the private cancellation signal would intentionally
             keep this test execution blocked. *)
          ignore (Temporal.Scope.cancel scope));
  expect "cross-owner scope status" "complete"
    (Scheduler.run_label first_scheduler);
  expect_error "cross-owner scope await" "defect"
    "Temporal future combinator received futures from different workflow executions"
    (Option.get !result);
  begin match !scope_ref with
  | Some scope -> ignore (Temporal.Scope.cancel scope)
  | None -> failwith "cross-owner scope was not retained"
  end;
  Workflow_context_store.shutdown first_context;
  Workflow_context_store.shutdown second_context

(** Executes the focused scope contract. *)
let () =
  test_create_outside_workflow ();
  test_completed_future_and_cleanup ();
  test_cancellation_resumes_waiter ();
  test_cancellation_is_idempotent ();
  test_cross_execution_future_is_rejected ()
