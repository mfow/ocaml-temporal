(** Focused lifecycle tests for the real Rust-backed supervisor.

    The generic supervisor tests use a fake backend so they can exercise queue
    admission and failure fan-out quickly. This file keeps the native graph
    coverage separate: it creates the actual Rust runtime, verifies that the
    owner can reject invalid child transitions without a server, and proves
    every child and parent shutdown operation is idempotent. No native pointer
    is returned to this test; all access still goes through typed operations. *)

(** Fails with [label] when two structural values differ. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Exercises native child teardown and parent shutdown without connecting to
    Temporal Server. The invalid worker-start result is important: it proves a
    failed child transition does not publish a partially initialized worker
    that later shutdown would need to guess how to release. *)
let test_native_shutdown_is_idempotent () =
  let module Native = Sdk_supervisor.Native in
  let supervisor =
    match Native.create ~capacity:2 () with
    | Ok supervisor -> supervisor
    | Error _ -> failwith "native supervisor creation failed"
  in
  expect "native compatibility" (Ok ())
    (Native.perform supervisor Native.Check_compatibility);
  let worker_config =
    Result.get_ok
      (Native.worker_config ~namespace:"temporal-sdk-test"
         ~task_queue:"ocaml-temporal-unit" ~build_id:"unit-build"
         ~max_cached_workflows:100 ~max_outstanding_workflow_tasks:100
         ~max_concurrent_workflow_task_polls:5
         ~graceful_shutdown_timeout_ms:1_000L ())
  in
  (match Native.perform supervisor (Native.Start_worker worker_config) with
  | Error
      (Native.Backend
        { Temporal_core_bridge.Native_bridge.status = Invalid_state; _ }) ->
      ()
  | _ -> failwith "native supervisor started a worker without a client");
  expect "first native worker shutdown" (Ok ())
    (Native.perform supervisor Native.Shutdown_worker);
  expect "repeated native worker shutdown" (Ok ())
    (Native.perform supervisor Native.Shutdown_worker);
  expect "first native client disconnect" (Ok ())
    (Native.perform supervisor Native.Disconnect_client);
  expect "repeated native client disconnect" (Ok ())
    (Native.perform supervisor Native.Disconnect_client);
  expect "native shutdown" (Ok ()) (Native.shutdown supervisor);
  expect "native repeated shutdown" (Ok ()) (Native.shutdown supervisor)

(** Runs the focused native lifecycle assertion. *)
let () = test_native_shutdown_is_idempotent ()
