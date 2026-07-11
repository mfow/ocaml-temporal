(** Worker process for the first two-OCaml-binary live acceptance test.

    The worker registers the same shared workflow definitions as the driver and
    keeps the public worker loop alive until graceful shutdown. The process is
    intentionally not added as a Compose service yet because the current public
    [Worker.create] surface routes only to the deterministic in-memory backend;
    accepting an HTTP endpoint here would produce a false green smoke test. *)

module Worker = Temporal.Worker
module Error = Temporal.Error
module Definitions = Smoke_definitions

(** Reads a required environment setting as a typed configuration error. *)
let required_env name =
  match Sys.getenv_opt name with
  | Some value when not (String.equal value "") -> Ok value
  | _ -> Error (Error.defect ~message:(name ^ " must not be empty"))

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
      let* worker =
        Worker.create ~target_url ~namespace
          ~identity:"ocaml-temporal-two-binary-worker"
          ~task_queue:Definitions.task_queue
          ~workflows:
            [
              Worker.workflow Definitions.fan_out;
              Worker.workflow Definitions.timer_then_activity;
            ]
          ~activities:[ Worker.activity Definitions.mock_transform ]
          ()
      in
      let run_result = Worker.run worker in
      let shutdown_result = Worker.shutdown worker in
      let* () = run_result in
      shutdown_result

(** Reports the final typed result and converts it to a process exit code. *)
let () =
  match run () with
  | Ok () -> Printf.printf "two-binary worker stopped cleanly\n%!"
  | Error error -> exit (fail "two-binary worker" error)
