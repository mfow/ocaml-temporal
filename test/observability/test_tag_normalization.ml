module Observability = Temporal_base.Observability

(** Extracts a required tag value from the normalization fixture. *)
let require definition tags =
  match Logs.Tag.find definition tags with
  | Some value -> value
  | None -> failwith "expected observability tag"

(** Numeric metadata exposed to reporters is always finite and non-negative,
    even if a future internal caller supplies an invalid value. *)
let test_invalid_numeric_metadata_becomes_zero () =
  let tags =
    Observability.tags ~operation:"invalid_numeric_metadata"
      ~duration_ms:(-1.) ~job_count:(-2) ~command_count:(-3) ()
  in
  assert (require Observability.Tag.duration_ms tags = 0.);
  assert (require Observability.Tag.job_count tags = 0);
  assert (require Observability.Tag.command_count tags = 0);
  List.iter
    (fun duration_ms ->
      let tags =
        Observability.tags ~operation:"non_finite_duration" ~duration_ms ()
      in
      assert (require Observability.Tag.duration_ms tags = 0.))
    [ Float.nan; Float.infinity; Float.neg_infinity ]

(** Valid values retain their precision and magnitude. *)
let test_valid_numeric_metadata_is_unchanged () =
  let tags =
    Observability.tags ~operation:"valid_numeric_metadata" ~duration_ms:1.25
      ~job_count:2 ~command_count:3 ()
  in
  assert (require Observability.Tag.duration_ms tags = 1.25);
  assert (require Observability.Tag.job_count tags = 2);
  assert (require Observability.Tag.command_count tags = 3)

let () =
  test_invalid_numeric_metadata_becomes_zero ();
  test_valid_numeric_metadata_is_unchanged ()
