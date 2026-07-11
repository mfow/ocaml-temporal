(** A manually controlled gate used to synchronize tests without wall-clock
    sleeps. All fields are protected by [mutex]. *)
type gate = {
  mutex : Mutex.t;
  condition : Condition.t;
  mutable open_ : bool;
}

(** Creates a closed test gate. *)
let create_gate () =
  { mutex = Mutex.create (); condition = Condition.create (); open_ = false }

(** Waits until another Domain opens [gate]. Spurious wakes are harmless
    because the predicate is checked in a loop. *)
let await_gate gate =
  Mutex.lock gate.mutex;
  while not gate.open_ do
    Condition.wait gate.condition gate.mutex
  done;
  Mutex.unlock gate.mutex

(** Opens [gate] permanently and wakes every current waiter. *)
let open_gate gate =
  Mutex.lock gate.mutex;
  gate.open_ <- true;
  Condition.broadcast gate.condition;
  Mutex.unlock gate.mutex

(** Spins only until [flag] records a synchronization milestone. This helper
    does not encode a timeout or use wall-clock scheduling. *)
let await_atomic flag =
  while not (Atomic.get flag) do
    Domain.cpu_relax ()
  done

(** Gives a newly spawned producer ample scheduling opportunities without a
    timing sleep. It is used only for a negative assertion that a full mailbox
    has not admitted more work. *)
let yield_to_domains () =
  for _ = 1 to 100_000 do
    Domain.cpu_relax ()
  done

(** Fails with [label] when two structural values differ. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** The typed request language exercises both fire-and-forget and
    request/reply operations without exposing mailbox implementation types. *)
module Request = struct
  type _ t =
    | Add : int -> unit t
    | Read : int list t
    | Echo : 'a -> 'a t
    | Hold : int * gate * gate -> unit t
    | Raise : string -> unit t
end

module Mailbox = Mailbox_processor.Make (Request)

(** Checks that a mailbox operation returned the expected contained handler
    exception without relying on a fragile exception-literal pattern. *)
let expect_handler_failure label expected = function
  | Error (Mailbox.Handler_raised (Failure actual)) -> expect label expected actual
  | _ -> failwith (label ^ " did not return the expected handler failure")

(** Creates a handler whose mutable state is accessed only by the mailbox's
    owner Domain. [Hold] records entry before waiting on a test-controlled
    release gate. *)
let create_handler () =
  let values = ref [] in
  let handle : type result. result Request.t -> result = function
    | Add value -> values := value :: !values
    | Read -> List.rev !values
    | Echo value -> value
    | Hold (value, entered, release) ->
        values := value :: !values;
        open_gate entered;
        await_gate release
    | Raise message -> failwith message
  in
  { Mailbox.handle }

(** Closes and joins [mailbox], failing if orderly shutdown did not succeed. *)
let close_and_expect_clean mailbox =
  Mailbox.close mailbox;
  Mailbox.close mailbox;
  expect "clean join" (Ok ()) (Mailbox.join mailbox);
  expect "repeated clean join" (Ok ()) (Mailbox.join mailbox)

(** Verifies that sequential admissions are handled in FIFO order. *)
let test_fifo () =
  let mailbox = Mailbox.create ~capacity:4 ~handler:(create_handler ()) in
  expect "post one" (Ok ()) (Mailbox.post mailbox (Add 1));
  expect "post two" (Ok ()) (Mailbox.post mailbox (Add 2));
  expect "post three" (Ok ()) (Mailbox.post mailbox (Add 3));
  expect "FIFO values" (Ok [ 1; 2; 3 ]) (Mailbox.call mailbox Read);
  close_and_expect_clean mailbox

(** Verifies that [call] preserves the GADT result type. *)
let test_typed_call () =
  let mailbox = Mailbox.create ~capacity:2 ~handler:(create_handler ()) in
  expect "integer call" (Ok 42) (Mailbox.call mailbox (Echo 42));
  expect "string call" (Ok "typed") (Mailbox.call mailbox (Echo "typed"));
  close_and_expect_clean mailbox

(** Exercises concurrent producers and checks exactly-once processing plus
    each producer's program order. *)
let test_many_producers () =
  let producer_count = 8 in
  let per_producer = 200 in
  let mailbox = Mailbox.create ~capacity:17 ~handler:(create_handler ()) in
  let producers =
    Array.init producer_count (fun producer ->
        Domain.spawn (fun () ->
            for sequence = 0 to per_producer - 1 do
              match
                Mailbox.post mailbox
                  (Add ((producer * per_producer) + sequence))
              with
              | Ok () -> ()
              | Error _ -> failwith "open mailbox rejected a producer"
            done))
  in
  Array.iter Domain.join producers;
  let seen =
    match Mailbox.call mailbox Read with
    | Ok values -> values
    | Error _ -> failwith "read failed"
  in
  let expected = List.init (producer_count * per_producer) Fun.id in
  expect "exactly once" expected (List.sort Int.compare seen);
  for producer = 0 to producer_count - 1 do
    let lower = producer * per_producer in
    let upper = lower + per_producer in
    let sequence =
      List.filter (fun value -> value >= lower && value < upper) seen
    in
    expect "producer program order"
      (List.init per_producer (fun offset -> lower + offset))
      sequence
  done;
  close_and_expect_clean mailbox

(** Proves that a full queue blocks a producer until the owner frees capacity. *)
let test_bounded_backpressure () =
  let entered = create_gate () in
  let release = create_gate () in
  let mailbox = Mailbox.create ~capacity:1 ~handler:(create_handler ()) in
  expect "hold admitted" (Ok ()) (Mailbox.post mailbox (Hold (1, entered, release)));
  await_gate entered;
  expect "queue filled" (Ok ()) (Mailbox.post mailbox (Add 2));
  let started = Atomic.make false in
  let finished = Atomic.make false in
  let blocked =
    Domain.spawn (fun () ->
        Atomic.set started true;
        let result = Mailbox.post mailbox (Add 3) in
        Atomic.set finished true;
        result)
  in
  await_atomic started;
  yield_to_domains ();
  if Atomic.get finished then failwith "full mailbox did not apply backpressure";
  open_gate release;
  expect "blocked post admitted" (Ok ()) (Domain.join blocked);
  expect "backpressure order" (Ok [ 1; 2; 3 ]) (Mailbox.call mailbox Read);
  close_and_expect_clean mailbox

(** Verifies that close rejects new work but drains work admitted beforehand. *)
let test_close_drains_and_rejects () =
  let entered = create_gate () in
  let release = create_gate () in
  let mailbox = Mailbox.create ~capacity:2 ~handler:(create_handler ()) in
  expect "hold admitted" (Ok ()) (Mailbox.post mailbox (Hold (1, entered, release)));
  await_gate entered;
  expect "queued post admitted" (Ok ()) (Mailbox.post mailbox (Add 2));
  Mailbox.close mailbox;
  expect "closed post" (Error Mailbox.Closed) (Mailbox.post mailbox (Add 3));
  open_gate release;
  expect "closed join drains" (Ok ()) (Mailbox.join mailbox)

(** Verifies that close wakes a producer waiting for capacity and rejects its
    not-yet-admitted request. *)
let test_close_releases_blocked_producer () =
  let entered = create_gate () in
  let release = create_gate () in
  let mailbox = Mailbox.create ~capacity:1 ~handler:(create_handler ()) in
  expect "hold admitted" (Ok ()) (Mailbox.post mailbox (Hold (1, entered, release)));
  await_gate entered;
  expect "queue filled" (Ok ()) (Mailbox.post mailbox (Add 2));
  let started = Atomic.make false in
  let producer =
    Domain.spawn (fun () ->
        Atomic.set started true;
        Mailbox.post mailbox (Add 3))
  in
  await_atomic started;
  yield_to_domains ();
  Mailbox.close mailbox;
  expect "blocked producer rejected" (Error Mailbox.Closed)
    (Domain.join producer);
  open_gate release;
  expect "close drains after producer wake" (Ok ()) (Mailbox.join mailbox)

(** Verifies that an unexpected handler exception reaches the active caller,
    causes deterministic terminal shutdown, and is also reported by [join]. *)
let test_handler_failure () =
  let mailbox = Mailbox.create ~capacity:4 ~handler:(create_handler ()) in
  let call_result = Mailbox.call mailbox (Raise "handler defect") in
  expect_handler_failure "active caller" "handler defect" call_result;
  expect_handler_failure "new call after failure" "handler defect"
    (Mailbox.call mailbox (Echo 1));
  expect_handler_failure "failed join" "handler defect" (Mailbox.join mailbox)

(** Verifies that callers queued behind a failing request are all released
    with the same terminal failure rather than stranded. *)
let test_handler_failure_releases_waiters () =
  let entered = create_gate () in
  let release = create_gate () in
  let mailbox = Mailbox.create ~capacity:8 ~handler:(create_handler ()) in
  expect "hold admitted" (Ok ()) (Mailbox.post mailbox (Hold (1, entered, release)));
  await_gate entered;
  expect "failing post admitted" (Ok ())
    (Mailbox.post mailbox (Raise "queued defect"));
  let waiter_started = Array.init 4 (fun _ -> Atomic.make false) in
  let waiters =
    Array.mapi
      (fun index started ->
        Domain.spawn (fun () ->
            Atomic.set started true;
            Mailbox.call mailbox (Echo index)))
      waiter_started
  in
  Array.iter await_atomic waiter_started;
  yield_to_domains ();
  open_gate release;
  Array.iter
    (fun waiter ->
      expect_handler_failure "queued waiter" "queued defect"
        (Domain.join waiter))
    waiters;
  expect_handler_failure "queued failure join" "queued defect"
    (Mailbox.join mailbox)

(** Verifies that terminal handler failure also wakes a producer blocked on a
    full queue, even if it races with the owner's dequeue notification. *)
let test_handler_failure_releases_blocked_producer () =
  let entered = create_gate () in
  let release = create_gate () in
  let mailbox = Mailbox.create ~capacity:1 ~handler:(create_handler ()) in
  expect "hold admitted" (Ok ()) (Mailbox.post mailbox (Hold (1, entered, release)));
  await_gate entered;
  expect "failing post admitted" (Ok ())
    (Mailbox.post mailbox (Raise "capacity defect"));
  let started = Atomic.make false in
  let producer =
    Domain.spawn (fun () ->
        Atomic.set started true;
        Mailbox.call mailbox (Echo 3))
  in
  await_atomic started;
  yield_to_domains ();
  open_gate release;
  expect_handler_failure "capacity waiter" "capacity defect"
    (Domain.join producer);
  expect_handler_failure "capacity failure join" "capacity defect"
    (Mailbox.join mailbox)

(** Rejects a zero capacity because no enqueue could ever make progress. *)
let test_capacity_validation () =
  match Mailbox.create ~capacity:0 ~handler:(create_handler ()) with
  | exception Invalid_argument _ -> ()
  | mailbox ->
      Mailbox.close mailbox;
      ignore (Mailbox.join mailbox);
      failwith "zero capacity was accepted"

let () =
  test_fifo ();
  test_typed_call ();
  test_many_producers ();
  test_bounded_backpressure ();
  test_close_drains_and_rejects ();
  test_close_releases_blocked_producer ();
  test_handler_failure ();
  test_handler_failure_releases_waiters ();
  test_handler_failure_releases_blocked_producer ();
  test_capacity_validation ()
