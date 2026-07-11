module Bridge = Temporal_core_bridge.Native_bridge

(** Extracts a successful bridge result or reports the native status and
    message as a test failure. *)
let unwrap = function
  | Ok value -> value
  | Error error -> failwith error.Bridge.message

let () =
  assert
    (Temporal.Runtime_info.native_bridge_abi_version ()
    = Ok Bridge.abi_version);
  unwrap (Bridge.check_abi_version Bridge.abi_version);
  (match Bridge.check_abi_version 2l with
  | Error { status = Abi_mismatch; message } -> assert (String.length message > 0)
  | _ -> failwith "ABI mismatch was not returned as a typed error");
  let input = Bytes.init 256 Char.chr in
  assert (unwrap (Bridge.echo input) = input);
  let progressed = Atomic.make false in
  let waiter = Domain.spawn (fun () -> unwrap (Bridge.conformance_wait_ms 100)) in
  let worker =
    Domain.spawn (fun () ->
        let deadline = Unix.gettimeofday () +. 0.05 in
        while Unix.gettimeofday () < deadline do
          Domain.cpu_relax ()
        done;
        Atomic.set progressed true)
  in
  Domain.join worker;
  Domain.join waiter;
  assert (Atomic.get progressed);
  let runtime = unwrap (Bridge.runtime_create ()) in
  unwrap (Bridge.runtime_close runtime);
  unwrap (Bridge.runtime_close runtime)
