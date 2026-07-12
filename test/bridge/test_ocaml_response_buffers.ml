module Bridge = Temporal_core_bridge.Native_bridge

(** The native result ABI represents an empty Rust allocation as [{ NULL, 0 }].
    This test exercises the OCaml copy path for that exact representation,
    rather than only checking the Rust-side buffer helper. *)
let () =
  match Bridge.echo Bytes.empty with
  | Ok value ->
      assert (Bytes.length value = 0);
      assert (Bytes.equal value Bytes.empty)
  | Error error ->
      failwith
        (Printf.sprintf "empty native response was rejected: %s" error.message)
