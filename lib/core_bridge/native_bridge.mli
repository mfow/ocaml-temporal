(** Reason a call to the linked Rust library failed. [Unknown code] retains a
    numeric status introduced by a newer Rust bridge instead of losing it. *)
type status =
  | Invalid_argument
  | Abi_mismatch
  | Panic
  | Internal
  | Unknown of int

(** Error copied into the OCaml heap. Once returned, it contains no pointer to
    Rust memory. *)
type error = {
  status : status;
  message : string;
}

(** Private owner of the Temporal Core runtime and Tokio executor for one SDK
    instance. It is intentionally abstract and never enters the public API. *)
type runtime

(** Version of the C-compatible interface expected by this OCaml code. *)
val abi_version : int32

(** Checks whether the linked Rust library implements [requested_version]. *)
val check_abi_version : int32 -> (unit, error) result

(** Sends bytes through the Rust allocation boundary and copies them back. This
    exists to test memory ownership; workflow code does not use it. *)
val echo : bytes -> (bytes, error) result

(** Waits in Rust for at most 1,000 milliseconds while allowing other OCaml
    Domains to run. This tests the blocking-call design used by future worker
    polling. Values outside 0 through 1,000 return an error. *)
val conformance_wait_ms : int -> (unit, error) result

(** Creates a native runtime after checking that the statically linked bridge
    implements the compatibility contract expected by this OCaml build. *)
val runtime_create : unit -> (runtime, error) result

(** Destroys the native runtime. Repeating this call on the same value is safe.
    The SDK supervisor must close all future child handles first. *)
val runtime_close : runtime -> (unit, error) result
