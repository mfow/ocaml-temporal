let read path =
  let channel = open_in_bin path in
  Fun.protect
    ~finally:(fun () -> close_in channel)
    (fun () -> really_input_string channel (in_channel_length channel))

let contains ~needle haystack =
  let needle_length = String.length needle in
  let haystack_length = String.length haystack in
  let rec search offset =
    if offset + needle_length > haystack_length then false
    else if String.sub haystack offset needle_length = needle then true
    else search (offset + 1)
  in
  needle_length = 0 || search 0

let require_text ~path ~needle =
  let contents = read path in
  if not (contains ~needle contents) then
    failwith (Printf.sprintf "%s does not contain %S" path needle)

let test_license () =
  let source_root = Sys.getenv "TEMPORAL_SOURCE_ROOT" in
  let license = Filename.concat source_root "LICENSE" in
  if not (String.starts_with ~prefix:"Apache License" (read license)) then
    failwith "LICENSE does not begin with the Apache License marker"

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
  require_text ~path:locked ~needle:"name: \"temporal-sdk\"";
  require_text ~path:locked ~needle:"maintainer: \"Michael Fowlie\"";
  require_text ~path:locked ~needle:"authors: \"Michael Fowlie\"";
  require_text ~path:locked ~needle:"x-maintenance-intent: [ \"(latest)\" ]";
  require_text ~path:locked ~needle:"\"experimental\"";
  require_text ~path:dune_project ~needle:"(authors \"Michael Fowlie\")";
  require_text ~path:dune_project ~needle:"(maintainers \"Michael Fowlie\")";
  require_text ~path:dune_project ~needle:"(maintenance_intent \"(latest)\")";
  require_text ~path:dune_project ~needle:"(name temporal-sdk)"

let () =
  test_license ();
  test_package_metadata ()
