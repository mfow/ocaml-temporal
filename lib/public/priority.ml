(** Validated scheduling priority shared by activity and child-workflow APIs. *)
type t = {
  priority_key : int;
  fairness_key : string;
  fairness_weight : float;
}

(** Rounds a positive finite OCaml double to IEEE-754 binary32 bits. All
    accepted weights are normal binary32 values, so the compact conversion can
    use the double's 53-bit significand and round-to-nearest-even. *)
let float32_bits value =
  let bits = Int64.bits_of_float value in
  let exponent = Int64.to_int (Int64.logand (Int64.shift_right_logical bits 52) 0x7ffL) - 1023 in
  let fraction = Int64.logand bits 0x000f_ffff_ffff_ffffL in
  let significand = Int64.logor 0x0010_0000_0000_0000L fraction in
  let exponent32 = exponent + 127 in
  let retained = Int64.shift_right_logical significand 29 in
  let discarded = Int64.logand significand 0x1fff_ffffL in
  let rounded =
    if discarded > 0x1000_0000L
       || (discarded = 0x1000_0000L && Int64.logand retained 1L = 1L)
    then Int64.add retained 1L
    else retained
  in
  let rounded, exponent32 =
    if rounded = 0x0100_0000L then (0x0080_0000L, exponent32 + 1)
    else (rounded, exponent32)
  in
  Int64.logor (Int64.shift_left (Int64.of_int exponent32) 23)
    (Int64.logand rounded 0x007f_ffffL)

(** Converts a finite OCaml float to the exact IEEE-754 bits retained by the
    JSON bridge. The bridge later serializes this as an unsigned integer, so
    replay never depends on a platform-specific float printer. *)
let validate ~priority_key ~fairness_key ~fairness_weight =
  let invalid message = Error (Error.defect ~message) in
  if priority_key < 0 || priority_key > Int32.to_int Int32.max_int then
    invalid "priority_key must be between 0 and Int32.max_int"
  else if String.length fairness_key > 64 then
    invalid "fairness_key must be at most 64 UTF-8 bytes"
  else if String.contains fairness_key '\000' then
    invalid "fairness_key must not contain NUL"
  else if not (Temporal_base.Codec.valid_utf_8 fairness_key) then
    invalid "fairness_key must be valid UTF-8"
  else
    match classify_float fairness_weight with
    | FP_nan | FP_infinite -> invalid "fairness_weight must be finite"
    | FP_zero | FP_subnormal | FP_normal
      when fairness_weight < 0.001 || fairness_weight > 1000.0 ->
        invalid "fairness_weight must be between 0.001 and 1000.0"
    | FP_zero | FP_subnormal | FP_normal -> Ok ()

(** Validates and snapshots caller-owned text before retaining it in a command. *)
let make ~priority_key ~fairness_key ~fairness_weight =
  match validate ~priority_key ~fairness_key ~fairness_weight with
  | Error _ as error -> error
  | Ok () ->
      let fairness_key = String.sub fairness_key 0 (String.length fairness_key) in
      (match validate ~priority_key ~fairness_key ~fairness_weight with
      | Error _ as error -> error
      | Ok () -> Ok { priority_key; fairness_key; fairness_weight })

(** Returns the Core priority key. *)
let priority_key value = value.priority_key

(** Returns a detached fairness key so foreign aliases cannot mutate command
    data through an accessor. *)
let fairness_key value = String.sub value.fairness_key 0 (String.length value.fairness_key)

(** Returns the exact validated fairness weight. *)
let fairness_weight value = value.fairness_weight

(** Returns the single-precision representation used by the Core protobuf. *)
let fairness_weight_bits value = float32_bits value.fairness_weight
