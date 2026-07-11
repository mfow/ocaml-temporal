(** Reads a whole repository file in binary mode and always closes it. *)
let read path =
  let channel = open_in_bin path in
  Fun.protect
    ~finally:(fun () -> close_in channel)
    (fun () -> really_input_string channel (in_channel_length channel))

(** Reports whether [needle] occurs in [haystack] without adding a test-only
    string dependency. *)
let contains ~needle haystack =
  let needle_length = String.length needle in
  let haystack_length = String.length haystack in
  let rec search offset =
    if offset + needle_length > haystack_length then false
    else if String.sub haystack offset needle_length = needle then true
    else search (offset + 1)
  in
  needle_length = 0 || search 0

(** Fails with the path and missing text when repository metadata drifts. *)
let require_text ~path ~needle =
  let contents = read path in
  if not (contains ~needle contents) then
    failwith (Printf.sprintf "%s does not contain %S" path needle)

(** Verifies that the project license file is the expected Apache license. *)
let test_license () =
  let source_root = Sys.getenv "TEMPORAL_SOURCE_ROOT" in
  let license = Filename.concat source_root "LICENSE" in
  if not (String.starts_with ~prefix:"Apache License" (read license)) then
    failwith "LICENSE does not begin with the Apache License marker"

(** Checks package identity, maintainership, experimental status, and exact
    dependency declarations in both generated build ecosystems. *)
let test_package_metadata () =
  let source_root = Sys.getenv "TEMPORAL_SOURCE_ROOT" in
  let source path = Filename.concat source_root path in
  let opam = source "temporal-sdk.opam" in
  let locked = source "temporal-sdk.opam.locked" in
  let dune_project = source "dune-project" in
  if Sys.file_exists (source "temporal.opam") then
    failwith "the retired temporal.opam package manifest still exists";
  require_text ~path:opam ~needle:"maintainer: \"Michael Fowlie\"";
  require_text ~path:opam ~needle:"authors: \"Michael Fowlie\"";
  require_text ~path:opam ~needle:"x-maintenance-intent: [ \"(latest)\" ]";
  require_text ~path:opam ~needle:"\"experimental\"";
  require_text ~path:opam ~needle:"\"logs\" {>= \"0.10\"}";
  require_text ~path:opam ~needle:"\"yojson\" {>= \"3.0\"}";
  require_text ~path:locked ~needle:"name: \"temporal-sdk\"";
  require_text ~path:locked ~needle:"maintainer: \"Michael Fowlie\"";
  require_text ~path:locked ~needle:"authors: \"Michael Fowlie\"";
  require_text ~path:locked ~needle:"x-maintenance-intent: [ \"(latest)\" ]";
  require_text ~path:locked ~needle:"\"experimental\"";
  require_text ~path:locked ~needle:"\"logs\" {= \"0.10.0\"}";
  require_text ~path:locked ~needle:"\"yojson\" {= \"3.0.0\"}";
  require_text ~path:dune_project ~needle:"(authors \"Michael Fowlie\")";
  require_text ~path:dune_project ~needle:"(maintainers \"Michael Fowlie\")";
  require_text ~path:dune_project ~needle:"(maintenance_intent \"(latest)\")";
  require_text ~path:dune_project ~needle:"(name temporal-sdk)";
  require_text ~path:dune_project ~needle:"(logs (>= 0.10))";
  require_text ~path:dune_project ~needle:"(yojson (>= 3.0))";
  if contains ~needle:"alcotest" (read (source "test/bridge/dune")) then
    failwith "bridge protocol tests must not add an Alcotest dependency"

(** Ensures Dune never attempts to turn the statically linked Rust bridge into a
    dynamically loadable OCaml stub library. Rust reports GNU-style native
    linker flags on Windows, but FlexDLL cannot consume those flags while
    constructing a DLL. The SDK intentionally ships an OCaml-owned executable
    with the Rust bridge linked into it, so static foreign archives are the
    portable and architecturally correct mode. *)
let test_static_foreign_archives () =
  let source_root = Sys.getenv "TEMPORAL_SOURCE_ROOT" in
  let workspace = Filename.concat source_root "dune-workspace" in
  let bridge = Filename.concat source_root "lib/core_bridge/dune" in
  require_text ~path:workspace
    ~needle:"(disable_dynamically_linked_foreign_archives true)";
  require_text ~path:bridge ~needle:"(foreign_library";
  require_text ~path:bridge ~needle:"(archive_name temporal_native_stubs)";
  require_text ~path:bridge ~needle:"(no_dynlink)";
  require_text
    ~path:(Filename.concat source_root "scripts/build-rust-bridge.sh")
    ~needle:"scripts/render-rust-link-flags.sh";
  if contains ~needle:"(foreign_stubs" (read bridge) then
    failwith "lib/core_bridge/dune must not build a temporary native-stubs DLL"

(** Ensures the reusable mailbox remains an internal build unit rather than an
    installed sublibrary visible to [temporal-sdk] consumers. *)
let test_mailbox_is_private () =
  let source_root = Sys.getenv "TEMPORAL_SOURCE_ROOT" in
  let dune = Filename.concat source_root "lib/mailbox_processor/dune" in
  let interface =
    Filename.concat source_root "lib/mailbox_processor/mailbox_processor.mli"
  in
  require_text ~path:dune ~needle:"(name temporal_mailbox_processor)";
  require_text
    ~path:interface
    ~needle:"must not call [post], [call], or [join] on that same processor";
  require_text ~path:interface ~needle:"Calling [close] from the handler is safe";
  if contains ~needle:"public_name" (read dune) then
    failwith "the mailbox processor must remain a Dune-private library"

(** Ensures native graph ownership remains an internal implementation unit and
    that its interface cannot return the owner-confined backend state. *)
let test_sdk_supervisor_is_private () =
  let source_root = Sys.getenv "TEMPORAL_SOURCE_ROOT" in
  let dune = Filename.concat source_root "lib/sdk_supervisor/dune" in
  let interface =
    Filename.concat source_root "lib/sdk_supervisor/sdk_supervisor.mli"
  in
  require_text ~path:dune ~needle:"(name temporal_sdk_supervisor)";
  require_text
    ~path:interface
    ~needle:"No backend state or\n    native handle can be obtained";
  require_text
    ~path:interface
    ~needle:"must be offloaded by any cooperative scheduler adapter";
  if contains ~needle:"public_name" (read dune) then
    failwith "the SDK supervisor must remain a Dune-private library"

let () =
  test_license ();
  test_package_metadata ();
  test_static_foreign_archives ();
  test_mailbox_is_private ();
  test_sdk_supervisor_is_private ()
