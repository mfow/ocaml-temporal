(** Stores the exact timestamp representation used by Temporal's activation
    protocol. Keeping this representation integer-only makes workflow replay
    independent of local timezone and floating-point behavior. *)
type t = {
  seconds : int64;
  nanoseconds : int;
}

(** Rejects a fractional component that cannot be represented by the protocol.
    Temporal normalizes fractions to a single non-negative second, so negative
    values and one-billion nanoseconds are both invalid. *)
let valid_nanoseconds nanoseconds =
  nanoseconds >= 0 && nanoseconds < 1_000_000_000

(** Constructs a validated timestamp for workflow code. The error is typed so
    callers can handle malformed application-provided values without relying
    on exceptions for normal control flow. *)
let of_unix ~seconds ~nanoseconds =
  if valid_nanoseconds nanoseconds then Ok { seconds; nanoseconds }
  else
    Error
      (Error.defect
         ~message:
           "Temporal.Time nanoseconds must be between 0 and 999999999")

(** Returns the signed whole-second component. *)
let seconds instant = instant.seconds

(** Returns the normalized fractional component. *)
let nanoseconds instant = instant.nanoseconds

(** Compares timestamps lexicographically without converting to a lossy float. *)
let compare left right =
  let seconds_order = Int64.compare left.seconds right.seconds in
  if seconds_order <> 0 then seconds_order
  else Int.compare left.nanoseconds right.nanoseconds

(** Tests exact equality using both timestamp components. *)
let equal left right =
  Int64.equal left.seconds right.seconds
  && Int.equal left.nanoseconds right.nanoseconds
