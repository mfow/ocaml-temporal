module Scheduler = Temporal_runtime.Scheduler

let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

let test_fifo_resume () =
  let scheduler = Scheduler.create () in
  let first, resolve_first =
    Scheduler.promise scheduler ~outside_error:(fun () -> "outside scheduler")
  in
  let second, resolve_second =
    Scheduler.promise scheduler ~outside_error:(fun () -> "outside scheduler")
  in
  let seen = ref [] in
  Scheduler.spawn scheduler (fun () ->
      match Temporal.Future.await first with
      | Ok value -> seen := value :: !seen
      | Error error -> failwith error);
  Scheduler.spawn scheduler (fun () ->
      match Temporal.Future.await second with
      | Ok value -> seen := value :: !seen
      | Error error -> failwith error);
  expect "blocked" "blocked" (Scheduler.run_label scheduler);
  resolve_second (Ok "second");
  resolve_first (Ok "first");
  expect "complete" "complete" (Scheduler.run_label scheduler);
  expect "resolution-order resume" [ "second"; "first" ] (List.rev !seen);
  expect "FIFO trace" [ 0; 1; 2; 3 ] (Scheduler.trace scheduler)

let test_double_resolution () =
  let scheduler = Scheduler.create () in
  let _, resolve =
    Scheduler.promise scheduler ~outside_error:(fun () -> "outside scheduler")
  in
  resolve (Ok 1);
  match resolve (Ok 2) with
  | exception Invalid_argument _ -> ()
  | _ -> failwith "future resolved twice"

let test_combinators () =
  let scheduler = Scheduler.create () in
  let left, resolve_left =
    Scheduler.promise scheduler ~outside_error:(fun () -> "outside scheduler")
  in
  let right, resolve_right =
    Scheduler.promise scheduler ~outside_error:(fun () -> "outside scheduler")
  in
  let both = Temporal.Future.both (Temporal.Future.map String.length left) right in
  let result = ref None in
  Scheduler.spawn scheduler (fun () -> result := Some (Temporal.Future.await both));
  expect "blocked combinator" "blocked" (Scheduler.run_label scheduler);
  resolve_right (Ok 42);
  resolve_left (Ok "agent");
  expect "complete combinator" "complete" (Scheduler.run_label scheduler);
  expect "both" (Some (Ok (5, 42))) !result

let test_outside_scheduler () =
  let scheduler = Scheduler.create () in
  let future, _ =
    Scheduler.promise scheduler ~outside_error:(fun () -> "outside scheduler")
  in
  expect "outside await" (Error "outside scheduler") (Temporal.Future.await future)

let test_immediate_and_multiple_waiters () =
  let scheduler = Scheduler.create () in
  let future, resolve =
    Scheduler.promise scheduler ~outside_error:(fun () -> "outside scheduler")
  in
  resolve (Ok 7);
  let seen = ref [] in
  Scheduler.spawn scheduler (fun () ->
      seen := Temporal.Future.await future :: !seen);
  Scheduler.spawn scheduler (fun () ->
      seen := Temporal.Future.await future :: !seen);
  expect "immediate complete" "complete" (Scheduler.run_label scheduler);
  expect "immediate values" [ Ok 7; Ok 7 ] !seen;
  expect "spawn trace" [ 0; 1 ] (Scheduler.trace scheduler)

let test_map_error_and_owner_check () =
  let first_scheduler = Scheduler.create () in
  let second_scheduler = Scheduler.create () in
  let first, resolve_first =
    Scheduler.promise first_scheduler ~outside_error:(fun () -> "outside")
  in
  let second, _ =
    Scheduler.promise second_scheduler ~outside_error:(fun () -> "outside")
  in
  let mapped = Temporal.Future.map_error String.uppercase_ascii first in
  resolve_first (Error "failure");
  expect "mapped error processing" "complete" (Scheduler.run_label first_scheduler);
  expect "mapped error" (Some (Error "FAILURE")) (Temporal.Future.peek mapped);
  (match Temporal.Future.both first second with
  | exception Invalid_argument _ -> ()
  | _ -> failwith "cross-scheduler futures were combined")

let test_mapper_defect_is_contained () =
  let scheduler = Scheduler.create () in
  let source, resolve =
    Scheduler.promise scheduler ~outside_error:(fun () -> "outside")
  in
  let _mapped = Temporal.Future.map (fun _ -> failwith "mapper defect") source in
  resolve (Ok 1);
  match Scheduler.run scheduler with
  | Scheduler.Failed (Failure message) when message = "mapper defect" -> ()
  | _ -> failwith "mapper exception escaped or was not recorded"

let test_both_observes_sibling_after_error () =
  let scheduler = Scheduler.create () in
  let left, resolve_left =
    Scheduler.promise scheduler ~outside_error:(fun () -> "outside")
  in
  let right, resolve_right =
    Scheduler.promise scheduler ~outside_error:(fun () -> "outside")
  in
  let combined = Temporal.Future.both left right in
  resolve_left (Error "left failed");
  expect "sibling still pending" "blocked" (Scheduler.run_label scheduler);
  assert (not (Temporal.Future.is_ready combined));
  resolve_right (Ok 9);
  expect "both settled" "complete" (Scheduler.run_label scheduler);
  expect "left error retained" (Some (Error "left failed"))
    (Temporal.Future.peek combined)

let test_shutdown_closes_pending_continuations () =
  let scheduler = Scheduler.create () in
  let future, resolve =
    Scheduler.promise scheduler ~outside_error:(fun () -> "outside")
  in
  Scheduler.spawn scheduler (fun () -> ignore (Temporal.Future.await future));
  expect "shutdown setup" "blocked" (Scheduler.run_label scheduler);
  Scheduler.shutdown scheduler;
  match resolve (Ok 1) with
  | exception Invalid_argument _ -> ()
  | _ -> failwith "shutdown future remained resolvable"

let () =
  test_fifo_resume ();
  test_double_resolution ();
  test_combinators ();
  test_outside_scheduler ();
  test_immediate_and_multiple_waiters ();
  test_map_error_and_owner_check ();
  test_mapper_defect_is_contained ();
  test_both_observes_sibling_after_error ();
  test_shutdown_closes_pending_continuations ()
