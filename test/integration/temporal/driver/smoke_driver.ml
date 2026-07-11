(** Driver process for the first two-OCaml-binary live acceptance test.

    This executable is a deliberately small, typed harness. It starts both
    scenarios before waiting for either one, then checks the exact terminal
    payloads returned by the public client API. It is not wired into Compose
    yet: the native client adapter and production scheduling loop are still
    private implementation work. Without [TEMPORAL_TWO_BINARY_LIVE=1] the
    process exits with a distinct status instead of accidentally exercising the
    in-memory [mock://] backend and giving a false live-smoke signal. *)

module Client = Temporal.Client
module Error = Temporal.Error
module Definitions = Smoke_definitions

(** The process-level error path reports one expected [result] failure and
    returns a nonzero exit status; ordinary Temporal failures are never raised
    as uncaught OCaml exceptions. *)
let fail operation error =
  Printf.eprintf "%s failed (%s): %s\n" operation (Error.kind error)
    (Error.message error);
  1

(** Reads a required environment setting as a typed configuration error. *)
let required_env name =
  match Sys.getenv_opt name with
  | Some value when not (String.equal value "") -> Ok value
  | _ -> Error (Error.defect ~message:(name ^ " must not be empty"))

(** Prevents this compile-time scaffold from being mistaken for a working live
    client. The guard is removed only when the public native adapter can perform
    a real StartWorkflowExecution and exact-run wait. *)
let require_live_gate () =
  match Sys.getenv_opt "TEMPORAL_TWO_BINARY_LIVE" with
  | Some "1" -> Ok ()
  | _ ->
      Error
        (Error.defect
           ~message:
             "two-binary live acceptance is not enabled; set \
              TEMPORAL_TWO_BINARY_LIVE=1 only in its Compose job")

(** Requires a completed terminal outcome with the expected typed payload. *)
let require_completed operation expected = function
  | Ok (Client.Completed actual) when String.equal actual expected -> Ok ()
  | Ok (Client.Completed actual) ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "%s returned unexpected result %S" operation actual))
  | Ok (Client.Failed error)
  | Ok (Client.Cancelled error)
  | Ok (Client.Terminated error)
  | Ok (Client.Timed_out error) ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "%s completed with Temporal failure: %s" operation
                (Error.message error)))
  | Ok (Client.Continued_as_new { run_id; _ }) ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf
                "%s continued as new at run %s; exact-run waiting must not \
                 follow it"
                operation run_id))
  | Error error -> Error error

(** Runs the two-workflow scenario through only the public client surface. *)
let run () =
  match require_live_gate () with
  | Error error -> Error error
  | Ok () ->
      let open Temporal.Result_syntax in
      let* target_url = required_env "TEMPORAL_ADDRESS" in
      let* namespace = required_env "TEMPORAL_NAMESPACE" in
      let* client =
        Client.create ~target_url ~namespace
          ~identity:"ocaml-temporal-two-binary-driver" ()
      in
      let finish result =
        match Client.shutdown client with
        | Ok () -> result
        | Error shutdown_error -> Error shutdown_error
      in
      let result =
        let* fan_handle =
          Client.start client ~workflow:Definitions.fan_out
            ~task_queue:Definitions.task_queue ~id:"two-binary-fan-out"
            ~input:"smoke"
        in
        let* timer_handle =
          Client.start client ~workflow:Definitions.timer_then_activity
            ~task_queue:Definitions.task_queue
            ~id:"two-binary-timer-then-activity" ~input:"smoke"
        in
        (* Both starts intentionally happen before the first wait.  This is
           the assertion that the client can retain independent exact-run
           handles while the worker services both executions. *)
        let* fan_result = Client.wait fan_handle in
        let* () =
          require_completed "smoke.fan_out" "SMOKE:LEFT|SMOKE:RIGHT"
            (Ok fan_result)
        in
        let* timer_result = Client.wait timer_handle in
        require_completed "smoke.timer_then_activity" "SMOKE:TIMER"
          (Ok timer_result)
      in
      finish result

(** Reports the final typed result and converts it to a process exit code. *)
let () =
  match run () with
  | Ok () -> Printf.printf "two-binary driver assertions passed\n%!"
  | Error error -> exit (fail "two-binary driver" error)
