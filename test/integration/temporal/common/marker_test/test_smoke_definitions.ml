(** Focused tests for the filesystem handshake used by the live acceptance
    fixture. These tests stay independent of Docker and Temporal Server: the
    worker activity's marker protocol must be correct before either external
    service is started. *)

(** Raises a useful failure when one marker contract assertion is false. *)
let require condition message = if not condition then failwith message

(** Reads the complete marker file after the writer has atomically published
    it. The helper is intentionally strict so the test catches an extra line
    or an omitted terminating newline. *)
let read_file path =
  let channel = open_in_bin path in
  Fun.protect
    ~finally:(fun () -> close_in_noerr channel)
    (fun () -> really_input_string channel (in_channel_length channel))

(** Removes a path when present, preserving the original assertion failure if
    cleanup itself is unable to find an already-removed file. *)
let remove_if_present path =
  try if Sys.file_exists path then Sys.remove path with _ -> ()

(** Lists staging files created beside the final marker. A successful atomic
    publication must leave no temporary file behind. *)
let staging_files path =
  let directory = Filename.dirname path in
  let prefix = Filename.basename path ^ ".tmp." in
  Sys.readdir directory
  |> Array.to_list
  |> List.filter (fun name -> String.starts_with ~prefix name)

(** Verifies that one publication uses the marker directory, writes the exact
    token, and cleans its unique staging file after the rename. *)
let test_atomic_publication () =
  let directory = Filename.get_temp_dir_name () in
  let path =
    Filename.concat directory
      (Printf.sprintf "ocaml-temporal-marker-%d" (Unix.getpid ()))
  in
  let token = "cancellation-test-token" in
  remove_if_present path;
  Fun.protect
    ~finally:(fun () -> remove_if_present path)
    (fun () ->
      require (staging_files path = []) "staging file existed before test";
      (match Smoke_definitions.publish_cancellation_ready path token with
      | Ok () -> ()
      | Error error ->
          failwith
            (Printf.sprintf "marker publication failed: %s"
               (Temporal.Error.message error)));
      require (Sys.file_exists path) "marker was not published";
      require (String.equal (read_file path) (token ^ "\n"))
        "marker contents were not exact";
      require (staging_files path = []) "staging file remained after rename")

(** Verifies the success path and reports a stable test name in Dune output. *)
let () =
  test_atomic_publication ();
  print_endline "smoke definitions marker test passed"
