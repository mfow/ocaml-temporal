(** Exercises the public priority constructor and the exact binary32 boundary
    used when an OCaml workflow command crosses into Temporal Core. *)

let expect_invalid label result =
  match result with
  | Error error when Temporal.Error.kind error = "defect" -> ()
  | Error _ -> failwith (label ^ " returned the wrong error category")
  | Ok _ -> failwith (label ^ " accepted invalid priority")

(** Builds the smallest valid priority and checks that its immutable accessors
    retain the caller's values while the wire weight is deterministic. *)
let test_valid () =
  let priority =
    match
      Temporal.Priority.make ~priority_key:7 ~fairness_key:"llm"
        ~fairness_weight:1.0
    with
    | Ok value -> value
    | Error error ->
        failwith ("valid priority was rejected: " ^ Temporal.Error.message error)
  in
  assert (Temporal.Priority.priority_key priority = 7);
  assert (Temporal.Priority.fairness_key priority = "llm");
  assert (Temporal.Priority.fairness_weight priority = 1.0);
  assert (Temporal.Priority.fairness_weight_bits priority = 0x3f800000L)

(** Invalid values fail before they can be attached to a workflow command. *)
let test_invalid () =
  expect_invalid "negative key"
    (Temporal.Priority.make ~priority_key:(-1) ~fairness_key:"" ~fairness_weight:1.0);
  expect_invalid "oversized key"
    (Temporal.Priority.make ~priority_key:(Int32.to_int Int32.max_int + 1)
       ~fairness_key:"" ~fairness_weight:1.0);
  expect_invalid "oversized fairness key"
    (Temporal.Priority.make ~priority_key:0 ~fairness_key:(String.make 65 'x')
       ~fairness_weight:1.0);
  expect_invalid "NUL fairness key"
    (Temporal.Priority.make ~priority_key:0 ~fairness_key:"bad\000key"
       ~fairness_weight:1.0);
  expect_invalid "zero fairness weight"
    (Temporal.Priority.make ~priority_key:0 ~fairness_key:"" ~fairness_weight:0.0);
  expect_invalid "NaN fairness weight"
    (Temporal.Priority.make ~priority_key:0 ~fairness_key:"" ~fairness_weight:nan)

let () =
  test_valid ();
  test_invalid ()
