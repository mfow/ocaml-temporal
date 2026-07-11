(** Information about the native Temporal Core implementation linked into the
    current OCaml executable. *)

val native_bridge_abi_version : unit -> (int32, Error.t) result
(** Returns the negotiated native bridge ABI version. *)
