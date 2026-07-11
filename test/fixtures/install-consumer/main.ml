let () =
  match Temporal.Runtime_info.native_bridge_abi_version () with
  | Ok 1l -> ()
  | Ok version ->
      failwith (Printf.sprintf "unexpected native ABI version %ld" version)
  | Error error -> failwith (Temporal.Error.message error)
