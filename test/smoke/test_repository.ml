let read path =
  let channel = open_in_bin path in
  Fun.protect
    ~finally:(fun () -> close_in channel)
    (fun () -> really_input_string channel (in_channel_length channel))

let test_license () =
  let source_root = Sys.getenv "TEMPORAL_SOURCE_ROOT" in
  let license = Filename.concat source_root "LICENSE" in
  Alcotest.(check bool)
    "Apache marker" true
    (String.starts_with ~prefix:"Apache License" (read license))

let () =
  Alcotest.run "repository"
    [ ("metadata", [ Alcotest.test_case "license" `Quick test_license ]) ]
