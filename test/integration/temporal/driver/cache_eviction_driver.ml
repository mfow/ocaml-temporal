(** Independent OCaml assertion client for the live sticky-cache eviction test.

    It starts a target timer workflow, waits until the shell controller has
    observed the durable timer boundary, then starts a second timer workflow to
    create one-entry-cache pressure. The controller, rather than the workflow,
    owns the filesystem handshake; workflow code remains fully replay-safe. *)

module Client = Temporal.Client
module Error = Temporal.Error
module Definitions = Smoke_definitions

(** Reads a required non-empty environment value as a typed process setup
    error, avoiding an unstructured exception before the client is closed. *)
let required_env name =
  match Sys.getenv_opt name with
  | Some value when value <> "" -> Ok value
  | _ -> Error (Error.defect ~message:(name ^ " must not be empty"))

(** Reads a path used by the controller handshake and ensures that client-side
    cleanup cannot escape the mounted test directory through a relative or
    NUL-containing value. *)
let required_absolute_path name =
  let open Temporal.Result_syntax in
  let* path = required_env name in
  if String.contains path '\000' || Filename.is_relative path then
    Error
      (Error.defect
         ~message:(name ^ " must be an absolute path without NUL"))
  else Ok path

(** Replaces a marker atomically so the host-side controller never parses a
    partial workflow/run identity from the driver process. Marker content is
    bounded metadata only and deliberately excludes workflow payload values. *)
let publish_marker path contents =
  let temporary = ref None in
  try
    let generated =
      Filename.temp_file ~temp_dir:(Filename.dirname path)
        (Filename.basename path ^ ".tmp.") ""
    in
    temporary := Some generated;
    let channel = open_out_bin generated in
    Fun.protect
      ~finally:(fun () -> close_out_noerr channel)
      (fun () ->
        output_string channel contents;
        flush channel);
    Sys.rename generated path;
    temporary := None;
    Ok ()
  with exception_ ->
    Option.iter
      (fun generated -> try Sys.remove generated with _ -> ())
      !temporary;
    Error
      (Error.defect
         ~message:
           (Printf.sprintf "cannot publish cache-eviction driver marker %s: %s"
              path (Printexc.to_string exception_)))

(** Reads a small complete marker if it exists. A strict length bound prevents
    a misconfigured mounted path from being treated as unbounded driver input;
    a missing file is represented separately because it is normal while the
    controller is still inspecting Temporal history. *)
let read_optional_marker path =
  if not (Sys.file_exists path) then Ok None
  else
    try
      let channel = open_in_bin path in
      Fun.protect
        ~finally:(fun () -> close_in_noerr channel)
        (fun () ->
          let length = in_channel_length channel in
          if length < 0 || length > 128 then
            Error
              (Error.defect
                 ~message:"cache-eviction release marker is too large")
          else Ok (Some (really_input_string channel length)))
    with exception_ ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "cannot read cache-eviction release marker: %s"
                (Printexc.to_string exception_)))

(** Waits for the exact controller release token with a finite three-minute
    budget. This is ordinary client-process coordination, never workflow code;
    a timeout becomes a typed failure rather than an indefinitely parked CI
    container. *)
let wait_for_release path =
  let rec loop attempts_remaining =
    if attempts_remaining = 0 then
      Error
        (Error.defect
           ~message:
             "cache-eviction controller did not release the pressure workflow")
    else
      match read_optional_marker path with
      | Error error -> Error error
      | Ok None ->
          Unix.sleepf 0.1;
          loop (attempts_remaining - 1)
      | Ok (Some "release\n") -> Ok ()
      | Ok (Some _) ->
          Error
            (Error.defect
               ~message:"cache-eviction release marker has an unexpected value")
  in
  (* The controller may need to start two short-lived admin-tools containers
     and retry a history query while the target's durable timer remains
     pending. Keep this budget below the driver's five-minute outer timeout,
     but above the controller's two-minute bounded history loop so a healthy
     but slow CI host cannot turn coordination latency into a false failure. *)
  loop 1_800

(** Checks the one public target result expected after a fresh replay. The
    pressure run is deliberately not awaited: it only exists to evict the
    target from the one-entry cache, and the fixture tears down its isolated
    Temporal database immediately after the target result is proven. *)
let require_target_completed = function
  | Client.Completed value when value = "SMOKE:CACHE:EVICTION:TARGET" -> Ok ()
  | Client.Completed value ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "cache-eviction target returned %S" value))
  | Client.Failed error
  | Client.Cancelled error
  | Client.Terminated error
  | Client.Timed_out error ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "cache-eviction target ended with %s: %s"
                (Error.kind error) (Error.message error)))
  | Client.Continued_as_new execution ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "cache-eviction target continued as new at run %s"
                execution.run_id))

(** Starts the exact target run, pauses only in this non-workflow client for a
    server-history-confirmed release, starts the pressure run, and waits on the
    original run. [finish] always shuts down the client so the acceptance
    controller does not confuse a successful assertion with a leaked client
    graph. *)
let run () =
  match Sys.getenv_opt "TEMPORAL_TWO_BINARY_LIVE" with
  | Some "1" ->
      let open Temporal.Result_syntax in
      let* target_url = required_env "TEMPORAL_ADDRESS" in
      let* namespace = required_env "TEMPORAL_NAMESPACE" in
      let* workflow_id = required_env "SMOKE_CACHE_EVICTION_WORKFLOW_ID" in
      let* accepted_file =
        required_absolute_path "SMOKE_CACHE_EVICTION_ACCEPTED_FILE"
      in
      let* release_file =
        required_absolute_path "SMOKE_CACHE_EVICTION_RELEASE_FILE"
      in
      let* pressure_file =
        required_absolute_path "SMOKE_CACHE_EVICTION_PRESSURE_FILE"
      in
      let* result_file =
        required_absolute_path "SMOKE_CACHE_EVICTION_RESULT_FILE"
      in
      let* client =
        Client.create ~target_url ~namespace
          ~identity:"ocaml-temporal-cache-eviction-driver" ()
      in
      let finish result =
        match Client.shutdown client with
        | Ok () -> result
        | Error error -> Error error
      in
      let result =
        let* target =
          Client.start client ~workflow:Definitions.sticky_cache_eviction
            ~task_queue:Definitions.task_queue ~id:workflow_id ~input:"target" ()
        in
        let* () =
          publish_marker accepted_file
            (Printf.sprintf "workflow_id=%s\nrun_id=%s\n"
               (Client.workflow_id target) (Client.run_id target))
        in
        let* () = wait_for_release release_file in
        let pressure_workflow_id = workflow_id ^ "-pressure" in
        let* pressure =
          Client.start client ~workflow:Definitions.sticky_cache_eviction
            ~task_queue:Definitions.task_queue ~id:pressure_workflow_id
            ~input:"pressure" ()
        in
        let* () =
          publish_marker pressure_file
            (Printf.sprintf "workflow_id=%s\nrun_id=%s\n"
               (Client.workflow_id pressure) (Client.run_id pressure))
        in
        let* outcome = Client.wait target in
        let* () = require_target_completed outcome in
        publish_marker result_file "completed\n"
      in
      finish result
  | _ ->
      Error
        (Error.defect
           ~message:
             "cache-eviction acceptance is not enabled; set TEMPORAL_TWO_BINARY_LIVE=1")

(** Converts the typed test-client result into the one-shot container's exit
    status. Error details stay in its log; shared markers contain only the
    small protocol values checked by the controller. *)
let () =
  match run () with
  | Ok () -> Printf.printf "cache-eviction driver completed\n%!"
  | Error error ->
      Printf.eprintf "cache-eviction driver failed (%s): %s\n%!" (Error.kind error)
        (Error.message error);
      exit 1
