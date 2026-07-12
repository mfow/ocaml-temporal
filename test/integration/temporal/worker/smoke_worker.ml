(** Worker process for the two-OCaml-binary live acceptance test.

    The worker registers the same shared workflow definitions as the driver and
    keeps the public native worker loop alive until graceful shutdown. The
    executable remains guarded by [TEMPORAL_TWO_BINARY_LIVE] so a local run
    cannot accidentally connect to a developer's Temporal endpoint; the
    Compose job is the only place that enables the live gate. *)

module Worker = Temporal.Worker
module Error = Temporal.Error
module Definitions = Smoke_definitions

(** Records a worker-process phase using metadata only. This executable never
    logs workflow/activity payloads; the markers exist to show whether a live
    acceptance stall occurred during construction, readiness publication, or
    the long-running worker loop. *)
let phase operation status =
  Printf.eprintf "two-binary worker phase=%s status=%s\n%!" operation status

(** Reads a required environment setting as a typed configuration error. *)
let required_env name =
  match Sys.getenv_opt name with
  | Some value when not (String.equal value "") -> Ok value
  | _ -> Error (Error.defect ~message:(name ^ " must not be empty"))

(** Publishes a small readiness marker only after [Worker.create] has completed
    successfully. The temporary file and rename make the health check observe
    either the complete marker or no marker, never a partially written file. *)
let publish_ready path =
  let temporary = Printf.sprintf "%s.tmp.%d" path (Unix.getpid ()) in
  try
    let channel = open_out_bin temporary in
    Fun.protect
      ~finally:(fun () -> close_out_noerr channel)
      (fun () -> output_string channel "worker-ready\n");
    Sys.rename temporary path;
    Ok ()
  with exception_ ->
    (try Sys.remove temporary with _ -> ());
    Error
      (Error.defect
         ~message:
           (Printf.sprintf "cannot publish worker readiness marker %s: %s" path
              (Printexc.to_string exception_)))

(** Removes the readiness marker during every normal result path. Compose also
    removes the container on failure, but explicit cleanup prevents a local
    rerun from inheriting stale readiness state. *)
let clear_ready path =
  try if Sys.file_exists path then Sys.remove path with _ -> ()

(** Runs the blocking worker loop while a small control Domain translates
    Compose's SIGTERM/SIGINT into the public shutdown operation. Signal handlers
    only flip atomics; all SDK and native calls stay outside the signal context,
    so teardown cannot interrupt a mutex or a JSON/FFI conversion halfway
    through. *)
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
            (* A bounded native wait lets this call join the worker loop rather
               than terminating the process while Core still owns a lease. *)
            (try ignore (Worker.shutdown worker) with _ -> ());
            Atomic.set watcher_finished true
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
             (Printf.sprintf "worker run raised: %s"
                (Printexc.to_string exception_)))
  in
  Domain.join watcher;
  Sys.set_signal Sys.sigterm previous_term;
  Sys.set_signal Sys.sigint previous_int;
  phase "worker_shutdown" "begin";
  let shutdown_result = Worker.shutdown worker in
  phase "worker_run"
    (match run_result with Ok () -> "stopped" | Error _ -> "error");
  phase "worker_shutdown"
    (match shutdown_result with Ok () -> "ok" | Error _ -> "error");
  let open Temporal.Result_syntax in
  let* () = run_result in
  shutdown_result

(** Prevents a normal local invocation from silently running the mock backend
    and being reported as a real Temporal worker. *)
let require_live_gate () =
  match Sys.getenv_opt "TEMPORAL_TWO_BINARY_LIVE" with
  | Some "1" -> Ok ()
  | _ ->
      Error
        (Error.defect
           ~message:
             "two-binary live acceptance is not enabled; set \
              TEMPORAL_TWO_BINARY_LIVE=1 only in its Compose job")

(** Reports a structured SDK error without exposing bridge JSON or payload bytes
    in process logs. *)
let fail operation error =
  phase operation ("error:" ^ Error.kind error);
  Printf.eprintf "%s failed (%s): %s\n" operation (Error.kind error)
    (Error.message error);
  1

(** Creates, runs, and shuts down the worker using the public API only. *)
let run () =
  match require_live_gate () with
  | Error error -> Error error
  | Ok () ->
      let open Temporal.Result_syntax in
      let* target_url = required_env "TEMPORAL_ADDRESS" in
      let* namespace = required_env "TEMPORAL_NAMESPACE" in
      let* ready_file = required_env "SMOKE_WORKER_READY_FILE" in
      let worker_result =
        Worker.create ~target_url ~namespace
          ~identity:"ocaml-temporal-two-binary-worker"
          ~task_queue:Definitions.task_queue
          ~workflows:
            [
              Worker.workflow Definitions.fan_out;
              Worker.workflow Definitions.timer_then_activity;
              Worker.workflow Definitions.child_after_timer;
              Worker.workflow Definitions.parent_awaits_child;
            ]
          ~activities:[ Worker.activity Definitions.mock_transform ]
          ()
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
        ~finally:(fun () -> clear_ready ready_file)
        (fun () ->
          phase "worker_ready" "begin";
          match publish_ready ready_file with
          | Error error ->
              phase "worker_ready" ("error:" ^ Error.kind error);
              Error error
          | Ok () ->
              phase "worker_ready" "published";
              run_with_signal_shutdown worker)

(** Reports the final typed result and converts it to a process exit code. *)
let () =
  match run () with
  | Ok () -> Printf.printf "two-binary worker stopped cleanly\n%!"
  | Error error -> exit (fail "two-binary worker" error)
