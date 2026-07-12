module Bridge = Temporal_core_bridge.Native_bridge

(** Extracts a successful bridge result so this test fails at the operation
    that violated the native lifecycle contract, rather than continuing with a
    partially initialized runtime. *)
let unwrap = function
  | Ok value -> value
  | Error error -> failwith error.Bridge.message

(** The replay worker is deliberately client-free, which makes it suitable for
    a deterministic native lifetime test. Its readiness wait remains pending
    until the bounded Rust wait expires, so closing the runtime while that wait
    is admitted exercises the C borrow/close gate without requiring a live
    Temporal server. *)
let replay_worker_config () =
  unwrap
    (Bridge.worker_config ~namespace:"temporal-sdk-lifetime"
       ~task_queue:"ocaml-temporal-lifetime" ~build_id:"lifetime-build"
       ~max_cached_workflows:0 ~max_outstanding_workflow_tasks:1
       ~max_concurrent_workflow_task_polls:1
       ~graceful_shutdown_timeout_ms:1_000L)

(** Waits until a spawned Domain has crossed its OCaml-side launch barrier.
    [Domain.cpu_relax] avoids blocking the main Domain while retaining a
    bounded, scheduler-friendly handoff. *)
let await_started started =
  while not (Atomic.get started) do
    Domain.cpu_relax ()
  done

(** Runs one close-versus-blocking-operation race. The waiter owns an OCaml
    root for [runtime], while the C stub releases the OCaml lock during the
    replay readiness wait. [runtime_close] must therefore wait for the native
    operation to return before Rust frees the runtime. *)
let run_close_race () =
  let runtime = unwrap (Bridge.runtime_create ()) in
  let config = replay_worker_config () in
  unwrap (Bridge.replay_worker_start runtime config);
  let started = Atomic.make false in
  let waiter =
    Domain.spawn (fun () ->
        Atomic.set started true;
        Bridge.replay_worker_wait_workflow runtime)
  in
  await_started started;
  (* Give the waiter a scheduling opportunity to enter its blocking C call.
     The operation itself has a bounded native wait, so the close path remains
     finite even when this Domain is scheduled late. *)
  Unix.sleepf 0.01;
  unwrap (Bridge.runtime_close runtime);
  match Domain.join waiter with
  | Ok () -> ()
  | Error { status = Bridge.Not_ready | Bridge.Invalid_state; _ } -> ()
  | Error error ->
      failwith
        ("runtime close race returned an unexpected status: " ^ error.message)

(** Repeats the race enough times to cover both Domain scheduling orders: the
    waiter may be admitted before close, or it may observe the closing gate and
    receive Rust's normal null-runtime status. *)
let () =
  for _iteration = 1 to 8 do
    run_close_race ()
  done
