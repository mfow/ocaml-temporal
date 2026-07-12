(** Driver process for the two-OCaml-binary live acceptance test.

    This executable is a deliberately small, typed harness. It starts the
    fan-out, timer, retry, parent/child, typed-failure, and long-running
    cancellation scenarios before waiting for any of them, then checks the
    exact terminal outcomes returned by the public client API. Without
    [TEMPORAL_TWO_BINARY_LIVE=1] the process exits with a distinct status
    instead of accidentally exercising the in-memory [mock://] backend and
    giving a false live-smoke signal. *)

module Client = Temporal.Client
module Error = Temporal.Error
module Definitions = Smoke_definitions

(** Wall-clock process timestamp used only for operator diagnostics.
    Workflow payloads and workflow code never call this helper; the driver is
    an ordinary client process, so a clock adjustment can affect only a
    displayed latency and never Temporal determinism. *)
let elapsed_ms started =
  Float.max 0. ((Unix.gettimeofday () -. started) *. 1_000.)

(** Emits one bounded phase record. The fields are operation metadata only:
    workflow/run identifiers and error kinds identify a server interaction,
    while payload bytes and application results are deliberately omitted. *)
let phase ~operation ?status ?workflow_id ?run_id ?duration_ms () =
  let field name value =
    match value with None -> "" | Some value -> " " ^ name ^ "=" ^ value
  in
  let status = field "status" status in
  let workflow_id = field "workflow_id" workflow_id in
  let run_id = field "run_id" run_id in
  let duration_ms =
    match duration_ms with
    | None -> ""
    | Some value -> Printf.sprintf " duration_ms=%.1f" value
  in
  Printf.eprintf "two-binary phase=%s%s%s%s%s\n%!" operation status workflow_id
    run_id duration_ms

(** Measures one client operation and records both its boundary and typed
    result. Keeping this wrapper at the driver boundary makes a future stalled
    native call visible without changing the public result semantics. *)
let measured operation action =
  let started = Unix.gettimeofday () in
  phase ~operation ~status:"begin" ();
  match action () with
  | Ok _ as result ->
      phase ~operation ~status:"ok" ~duration_ms:(elapsed_ms started) ();
      result
  | Error error as result ->
      phase ~operation ~status:("error:" ^ Error.kind error)
        ~duration_ms:(elapsed_ms started) ();
      result

(** The process-level error path reports one expected [result] failure and
    returns a nonzero exit status; ordinary Temporal failures are never raised
    as uncaught OCaml exceptions. *)
let fail operation error =
  phase ~operation ~status:("error:" ^ Error.kind error) ();
  Printf.eprintf "%s failed (%s): %s\n" operation (Error.kind error)
    (Error.message error);
  1

(** Reads a required environment setting as a typed configuration error. *)
let required_env name =
  match Sys.getenv_opt name with
  | Some value when not (String.equal value "") -> Ok value
  | _ -> Error (Error.defect ~message:(name ^ " must not be empty"))

(** Prevents a normal local invocation from connecting to an unintended
    endpoint. The dedicated Compose service sets this gate only for the live
    acceptance run. *)
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

(** Gives terminal outcomes a safe label for phase logs without inspecting the
    typed result value. In particular, completed payload bytes never enter a
    process log. *)
let terminal_kind = function
  | Client.Completed _ -> "completed"
  | Client.Failed _ -> "failed"
  | Client.Cancelled _ -> "cancelled"
  | Client.Terminated _ -> "terminated"
  | Client.Timed_out _ -> "timed_out"
  | Client.Continued_as_new _ -> "continued_as_new"

(** Requires the live workflow to fail as a non-retryable workflow application
    error. [Error.view] exposes the stable category and retry policy while the
    stable message prefix identifies this fixture's intentional failure without
    depending on extra source or Core failure-info text. *)
let require_non_retryable_failure operation expected_message = function
  | Ok (Client.Failed error) ->
      let view = Error.view error in
      if
        view.category = `Workflow
        && view.non_retryable
        && String.starts_with ~prefix:expected_message view.message
      then Ok ()
      else
        Error
          (Error.defect
             ~message:
               (Printf.sprintf
                  "%s returned an unexpected workflow failure (kind=%s, non_retryable=%b)"
                  operation (Error.kind error) view.non_retryable))
  | Ok outcome ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "%s returned terminal outcome %s" operation
                (terminal_kind outcome)))
  | Error error -> Error error

(** Requires the exact run to report Temporal's cancellation category after a
    successful cancellation acknowledgement. The assertion deliberately checks
    public metadata rather than Core's full failure-info text: category,
    retryability, and the stable message are the contract callers can safely
    branch on. *)
let require_cancelled operation = function
  | Ok (Client.Cancelled error) ->
      let view = Error.view error in
      if
        view.category = `Cancelled
        && not view.non_retryable
        && String.equal view.message "workflow execution was cancelled"
      then Ok ()
      else
        Error
          (Error.defect
             ~message:
               (Printf.sprintf
                  "%s returned unexpected cancellation metadata (kind=%s, non_retryable=%b)"
                  operation (Error.kind error) view.non_retryable))
  | Ok outcome ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "%s returned terminal outcome %s" operation
                (terminal_kind outcome)))
  | Error error -> Error error

(** Sends an exact-run cancellation request and records only its acknowledgement
    metadata. A successful result means Temporal accepted the control request;
    the caller must still wait on the same handle to observe the terminal
    cancellation. *)
let cancel_workflow handle =
  let operation = "cancel:" ^ Client.workflow_id handle in
  let started = Unix.gettimeofday () in
  phase ~operation ~status:"begin" ~workflow_id:(Client.workflow_id handle)
    ~run_id:(Client.run_id handle) ();
  match
    Client.cancel ~request_id:"two-binary-cancel-long-running-1"
      ~reason:"live acceptance requested cancellation" handle
  with
  | Ok () ->
      phase ~operation ~status:"acknowledged"
        ~workflow_id:(Client.workflow_id handle) ~run_id:(Client.run_id handle)
        ~duration_ms:(elapsed_ms started) ();
      Ok ()
  | Error error ->
      phase ~operation ~status:("error:" ^ Error.kind error)
        ~workflow_id:(Client.workflow_id handle) ~run_id:(Client.run_id handle)
        ~duration_ms:(elapsed_ms started) ();
      Error error

(** Starts one workflow and records the server-issued run identity only after
    the typed client has accepted it. *)
let start_workflow client ~workflow ~task_queue ~id ~input =
  let operation = "start:" ^ id in
  let started = Unix.gettimeofday () in
  phase ~operation ~status:"begin" ~workflow_id:id ();
  match
    Client.start client ~workflow ~task_queue ~id ~input ()
  with
  | Ok handle as result ->
      phase ~operation ~status:"accepted" ~workflow_id:(Client.workflow_id handle)
        ~run_id:(Client.run_id handle) ~duration_ms:(elapsed_ms started) ();
      result
  | Error error as result ->
      phase ~operation ~status:("error:" ^ Error.kind error) ~workflow_id:id
        ~duration_ms:(elapsed_ms started) ();
      result

(** Waits for one exact run and logs only its terminal class. The returned
    outcome remains untouched so callers still perform the same exact typed
    assertions as the acceptance test did before instrumentation. *)
let wait_workflow handle =
  let operation = "wait:" ^ Client.workflow_id handle in
  let started = Unix.gettimeofday () in
  phase ~operation ~status:"begin" ~workflow_id:(Client.workflow_id handle)
    ~run_id:(Client.run_id handle) ();
  match Client.wait handle with
  | Ok outcome as result ->
      phase ~operation ~status:(terminal_kind outcome)
        ~workflow_id:(Client.workflow_id handle) ~run_id:(Client.run_id handle)
        ~duration_ms:(elapsed_ms started) ();
      result
  | Error error as result ->
      phase ~operation ~status:("error:" ^ Error.kind error)
        ~workflow_id:(Client.workflow_id handle) ~run_id:(Client.run_id handle)
        ~duration_ms:(elapsed_ms started) ();
      result

(** Runs every live smoke scenario through only the public client surface. *)
let run () =
  match require_live_gate () with
  | Error error -> Error error
  | Ok () ->
      let open Temporal.Result_syntax in
      let* target_url = required_env "TEMPORAL_ADDRESS" in
      let* namespace = required_env "TEMPORAL_NAMESPACE" in
      let* client =
        measured "client_create" (fun () ->
            Client.create ~target_url ~namespace
              ~identity:"ocaml-temporal-two-binary-driver" ())
      in
      let finish result =
        phase ~operation:"client_shutdown" ~status:"begin" ();
        let started = Unix.gettimeofday () in
        match Client.shutdown client with
        | Ok () ->
            phase ~operation:"client_shutdown" ~status:"ok"
              ~duration_ms:(elapsed_ms started) ();
            result
        | Error shutdown_error ->
            phase ~operation:"client_shutdown"
              ~status:("error:" ^ Error.kind shutdown_error)
              ~duration_ms:(elapsed_ms started) ();
            Error shutdown_error
      in
      let result =
        let* fan_handle =
          start_workflow client ~workflow:Definitions.fan_out
            ~task_queue:Definitions.task_queue ~id:"two-binary-fan-out" ~input:"smoke"
        in
        let* timer_handle =
          start_workflow client ~workflow:Definitions.timer_then_activity
            ~task_queue:Definitions.task_queue
            ~id:"two-binary-timer-then-activity" ~input:"smoke"
        in
        let* retry_handle =
          start_workflow client ~workflow:Definitions.activity_retry
            ~task_queue:Definitions.task_queue ~id:"two-binary-activity-retry"
            ~input:"smoke"
        in
        let* parent_handle =
          start_workflow client ~workflow:Definitions.parent_awaits_child
            ~task_queue:Definitions.task_queue
            ~id:"two-binary-parent-awaits-child" ~input:"smoke"
        in
        let* failure_handle =
          start_workflow client ~workflow:Definitions.non_retryable_failure
            ~task_queue:Definitions.task_queue
            ~id:"two-binary-non-retryable-failure" ~input:"smoke"
        in
        let* cancellation_handle =
          start_workflow client ~workflow:Definitions.long_running_cancellation
            ~task_queue:Definitions.task_queue
            ~id:"two-binary-long-running-cancellation" ~input:"smoke"
        in
        (* All six starts intentionally happen before the first wait. The
           cancellation request is sent immediately, while the long timer keeps
           that exact execution outstanding; this proves that acknowledgement
           and terminal cancellation are separate client operations. *)
        let* () = cancel_workflow cancellation_handle in
        let* fan_result = wait_workflow fan_handle in
        let* () =
          require_completed "smoke.fan_out" "SMOKE:LEFT|SMOKE:RIGHT"
            (Ok fan_result)
        in
        let* timer_result = wait_workflow timer_handle in
        let* () =
          require_completed "smoke.timer_then_activity" "SMOKE:TIMER"
            (Ok timer_result)
        in
        let* retry_result = wait_workflow retry_handle in
        let* () =
          require_completed "smoke.activity_retry" "SMOKE:ATTEMPT:2"
            (Ok retry_result)
        in
        let* parent_result = wait_workflow parent_handle in
        let* () =
          require_completed "smoke.parent_awaits_child" "SMOKE:CHILD"
            (Ok parent_result)
        in
        let* failure_result = wait_workflow failure_handle in
        let* () =
          require_non_retryable_failure "smoke.non_retryable_failure"
            "intentional terminal workflow failure" (Ok failure_result)
        in
        let* cancellation_result = wait_workflow cancellation_handle in
        require_cancelled "smoke.long_running_cancellation"
          (Ok cancellation_result)
      in
      finish result

(** Reports the final typed result and converts it to a process exit code. *)
let () =
  match run () with
  | Ok () -> Printf.printf "two-binary driver assertions passed\n%!"
  | Error error -> exit (fail "two-binary driver" error)
