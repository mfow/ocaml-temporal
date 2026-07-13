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

(** Accepts every documented completion of the close race while preserving
    failure visibility for unrelated bridge errors. The waiter can finish
    before close with [Not_ready] or [Invalid_state], or it can lose the
    admission race after close detaches the native runtime. Rust reports that
    latter, valid close-wins outcome as the narrowly identified
    [Invalid_argument] message below; accepting arbitrary invalid arguments
    here would hide a real ABI regression. *)
let assert_close_race_result (result : (unit, Bridge.error) result) =
  match result with
  | Ok () -> ()
  | Error { status = Bridge.Not_ready | Bridge.Invalid_state; _ } -> ()
  | Error { status = Bridge.Invalid_argument; message }
    when String.equal message "runtime pointer is null" -> ()
  | Error error ->
      failwith ("runtime close race returned an unexpected status: " ^ error.message)

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
  assert_close_race_result (Domain.join waiter)

(** Repeats the race enough times to cover both Domain scheduling orders: the
    waiter may be admitted before close, or it may observe the closing gate and
    receive Rust's normal null-runtime status. *)
let () =
  for _iteration = 1 to 8 do
    run_close_race ()
  done

(** Same close-versus-blocking-operation race as [run_close_race], but with a
    third Domain forcing GC promotion and major-heap compaction throughout the
    waiter's ~100ms bounded native wait ([READINESS_WAIT_TIMEOUT] in
    [rust/core-bridge/src/worker_bridge.rs]). The [runtime] custom block is
    freshly allocated in the minor heap by [Bridge.runtime_create], so a
    stop-the-world minor collection promotes (moves) it, and [Gc.compact]
    subsequently relocates it again on the major heap. Both can happen while
    the waiter Domain holds the OCaml runtime lock released inside its
    blocking C call, and while this Domain's own [Bridge.runtime_close] call
    captures and dereferences the runtime owner across its own blocking
    section. This is a regression test for a use-after-free where the C stubs
    cached an interior pointer into that movable custom block ([Runtime_val])
    before releasing the lock and dereferenced it again afterward: a GC move
    during the release window left the cached pointer stale, corrupting
    whatever now occupied that memory and potentially hanging
    [runtime_close]'s wait forever. Forcing collections here does not
    guarantee any single run hits the exact interleaving that reproduced the
    bug, but across many iterations and an aggressively GC-pressured window it
    reliably did before the fix, and a bounded run here catches a
    reintroduction without risking a flaky hang: [runtime_close] guarantees
    eventual completion once the wait observes zero admitted calls. *)
let run_close_race_under_gc_pressure () =
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
  let keep_pressing = Atomic.make true in
  let presser =
    Domain.spawn (fun () ->
        while Atomic.get keep_pressing do
          Gc.minor ();
          Gc.compact ();
          (* Allocate short-lived garbage so the following [Gc.minor] has real
             promotion work to do rather than an empty nursery. *)
          ignore (Sys.opaque_identity (Bytes.create 4096))
        done)
  in
  Unix.sleepf 0.01;
  unwrap (Bridge.runtime_close runtime);
  Atomic.set keep_pressing false;
  Domain.join presser;
  assert_close_race_result (Domain.join waiter)

(** Repeats the GC-pressured race enough times to make both a promotion-timed
    and a compaction-timed overlap with the waiter's blocking window likely. *)
let () =
  for _iteration = 1 to 8 do
    run_close_race_under_gc_pressure ()
  done
