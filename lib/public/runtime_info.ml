let native_bridge_abi_version () =
  let module Bridge = Temporal_core_bridge.Native_bridge in
  match Bridge.check_abi_version Bridge.abi_version with
  | Ok () -> Ok Bridge.abi_version
  | Error error ->
      Error
        (Temporal_base.Error.make ~non_retryable:true ~category:`Bridge
           ~message:error.message ())
