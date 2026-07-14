(** Dedicated worker for the live sticky-cache eviction acceptance test.

    This process deliberately registers only the timer workflow used by the
    fixture and configures a one-entry sticky cache. It is separate from the
    broad smoke worker so cache pressure cannot be masked by unrelated cached
    executions, and so its test-only diagnostics have one unambiguous owner. *)

module Worker = Temporal.Worker
module Error = Temporal.Error
module Definitions = Smoke_definitions

(** Emits a payload-free worker phase for CI diagnostics. The message records
    process lifecycle only; workflow inputs, outputs, and bridge documents are
    intentionally never written to the container log. *)
let phase operation status =
  Printf.eprintf "cache-eviction worker phase=%s status=%s\n%!" operation status

(** Reads one required non-empty environment value as a typed startup error.
    The worker uses this for server settings and its test fixture identifiers
    before allocating a native runtime. *)
let required_env name =
  match Sys.getenv_opt name with
  | Some value when value <> "" -> Ok value
  | _ -> Error (Error.defect ~message:(name ^ " must not be empty"))

(** Reads an absolute marker/diagnostic path and rejects a NUL-containing or
    relative value before this test process creates, removes, or opens a file.
    The paths live in either the container's [/tmp] or the repository bind
    mount; accepting a relative path would make cleanup depend on Dune's
    current directory. *)
let required_absolute_path name =
  let open Temporal.Result_syntax in
  let* path = required_env name in
  if String.contains path '\000' || Filename.is_relative path then
    Error
      (Error.defect
         ~message:(name ^ " must be an absolute path without NUL"))
  else Ok path

(** Atomically publishes a small control marker in the same directory as its
    destination. The controller therefore observes either a complete marker or
    no marker, never a partial write from the independent worker container. *)
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
           (Printf.sprintf "cannot publish cache-eviction marker %s: %s" path
              (Printexc.to_string exception_)))

(** Removes a stale marker before startup and fails closed if it remains. A
    Compose container may be recreated while the bind mount still has evidence
    from an interrupted run; letting that marker satisfy a health check would
    invalidate the acceptance result. *)
let clear_marker_before_start path =
  try
    if Sys.file_exists path then Sys.remove path;
    if Sys.file_exists path then
      Error
        (Error.defect
           ~message:(Printf.sprintf "cannot remove stale marker %s" path))
    else Ok ()
  with exception_ ->
    Error
      (Error.defect
         ~message:
           (Printf.sprintf "cannot remove stale marker %s: %s" path
              (Printexc.to_string exception_)))

(** Best-effort readiness cleanup used after construction has succeeded. The
    original worker result remains authoritative if a container filesystem
    cleanup race makes deletion fail during shutdown. *)
let clear_marker path =
  try if Sys.file_exists path then Sys.remove path with _ -> ()

(** Enables this executable only inside the Compose acceptance topology. The
    explicit guard prevents an accidental local invocation from connecting to a
    developer endpoint merely because an address environment variable exists. *)
let require_live_gate () =
  match Sys.getenv_opt "TEMPORAL_TWO_BINARY_LIVE" with
  | Some "1" -> Ok ()
  | _ ->
      Error
        (Error.defect
           ~message:
             "cache-eviction acceptance is not enabled; set TEMPORAL_TWO_BINARY_LIVE=1 only in its Compose job")

(** Runs the public worker loop while a small control Domain converts process
    termination into [Worker.shutdown]. Signal handlers mutate only an atomic
    flag; all SDK calls remain outside signal context so a signal cannot
    interrupt the OCaml/Rust ownership boundary halfway through an operation. *)
let run_with_signal_shutdown worker =
  let stop_requested = Atomic.make false in
  let watcher_finished = Atomic.make false in
  let request_shutdown _signal = Atomic.set stop_requested true in
  let previous_term = Sys.signal Sys.sigterm (Sys.Signal_handle request_shutdown) in
  let previous_int = Sys.signal Sys.sigint (Sys.Signal_handle request_shutdown) in
  let watcher =
    Domain.spawn (fun () ->
        while not (Atomic.get watcher_finished) do
          if Atomic.get stop_requested then begin
            (* [Worker.shutdown] has bounded native waits and is idempotent.
               Retry a typed drain failure rather than ending the helper while
               [Worker.run] is still live and Compose is waiting for a clean
               stop marker. *)
            match Worker.shutdown worker with
            | Ok () -> Atomic.set watcher_finished true
            | Error _ -> Unix.sleepf 0.05
          end
          else Unix.sleepf 0.05
        done)
  in
  let run_result =
    try
      phase "worker_run" "begin";
      Fun.protect
        ~finally:(fun () -> Atomic.set watcher_finished true)
        (fun () -> Worker.run worker)
    with exception_ ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "cache-eviction worker run raised: %s"
                (Printexc.to_string exception_)))
  in
  Domain.join watcher;
  Sys.set_signal Sys.sigterm previous_term;
  Sys.set_signal Sys.sigint previous_int;
  phase "worker_shutdown" "begin";
  let shutdown_result = Worker.shutdown worker in
  phase "worker_run" (match run_result with Ok () -> "stopped" | Error _ -> "error");
  phase "worker_shutdown"
    (match shutdown_result with Ok () -> "ok" | Error _ -> "error");
  let open Temporal.Result_syntax in
  let* () = run_result in
  shutdown_result

(** Creates the one-cache worker through the public OCaml API, publishes its
    readiness only after construction, and preserves the diagnostic file until
    the controller has validated it. No workflow code performs filesystem or
    process operations; all test-only coordination remains in this outer
    executable. *)
let run () =
  match require_live_gate () with
  | Error error -> Error error
  | Ok () ->
      let open Temporal.Result_syntax in
      let* ready_file = required_absolute_path "SMOKE_CACHE_EVICTION_READY_FILE" in
      let* stopped_file =
        required_absolute_path "SMOKE_CACHE_EVICTION_WORKER_STOPPED_FILE"
      in
      let* diagnostics_file =
        required_absolute_path "SMOKE_WORKER_CACHE_EVICTION_DIAGNOSTICS_FILE"
      in
      let* _target_workflow_id =
        required_env "SMOKE_CACHE_EVICTION_WORKFLOW_ID"
      in
      let* target_url = required_env "TEMPORAL_ADDRESS" in
      let* namespace = required_env "TEMPORAL_NAMESPACE" in
      let* () = clear_marker_before_start ready_file in
      let* () = clear_marker_before_start stopped_file in
      let* () = clear_marker_before_start diagnostics_file in
      let* options =
        Worker.Options.make ~max_cached_workflows:1
          ~max_concurrent_workflow_task_polls:2 ()
      in
      let worker_result =
        Worker.create ~target_url ~namespace
          ~identity:"ocaml-temporal-cache-eviction-worker"
          ~options ~task_queue:Definitions.task_queue
          ~workflows:[ Worker.workflow Definitions.sticky_cache_eviction ]
          ~activities:[] ()
      in
      let* worker =
        match worker_result with
        | Ok worker ->
            phase "worker_create" "ok";
            Ok worker
        | Error error ->
            phase "worker_create" ("error:" ^ Error.kind error);
            Error error
      in
      Fun.protect
        ~finally:(fun () -> clear_marker ready_file)
        (fun () ->
          phase "worker_ready" "begin";
          let* () = publish_marker ready_file "cache-eviction-worker-ready\n" in
          phase "worker_ready" "published";
          let* () = run_with_signal_shutdown worker in
          (* Reuse the ordinary worker-stop marker protocol. The filename
             distinguishes this dedicated worker, while sharing the exact
             content lets the same checked shutdown assertion guard both
             workers against a container that merely exited. *)
          let* () = publish_marker stopped_file "worker-stopped\n" in
          phase "worker_stopped" "published";
          Ok ())

(** Converts the typed worker outcome into the process status used by the
    Compose health controller, while keeping bridge details out of the marker
    files themselves. *)
let () =
  match run () with
  | Ok () -> Printf.printf "cache-eviction worker stopped cleanly\n%!"
  | Error error ->
      phase "worker" ("error:" ^ Error.kind error);
      Printf.eprintf "cache-eviction worker failed (%s): %s\n%!" (Error.kind error)
        (Error.message error);
      exit 1
