(** Negotiates the ABI at call time and converts the bridge-owned error into the
    stable OCaml error channel. Rust allocation cleanup happens inside the
    private bridge binding before this function receives the result. *)
let native_bridge_abi_version () =
  let module Bridge = Temporal_core_bridge.Native_bridge in
  match Bridge.check_abi_version Bridge.abi_version with
  | Ok () -> Ok Bridge.abi_version
  | Error error ->
      Error
        (Error.make ~non_retryable:true ~category:`Bridge
           ~message:error.message ())
