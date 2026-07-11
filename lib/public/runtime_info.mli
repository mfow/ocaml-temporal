(** Reports information about the Rust bridge linked into the OCaml executable.
    This module is mainly useful for installation checks and diagnostics. *)

(** Asks the linked Rust library for its bridge version and verifies that it
    matches the version expected by this OCaml package. *)
val native_bridge_abi_version : unit -> (int32, Error.t) result
