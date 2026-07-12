(** Driver process for the first two-OCaml-binary live acceptance test.

    This executable is a deliberately small, typed harness. It starts both
    scenarios before waiting for either one, then checks the exact terminal
    payloads returned by the public client API. Without
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

(** Runs the two-workflow scenario through only the public client surface. *)
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
        (* Both starts intentionally happen before the first wait.  This is
           the assertion that the client can retain independent exact-run
           handles while the worker services both executions. *)
        let* fan_result = wait_workflow fan_handle in
        let* () =
          require_completed "smoke.fan_out" "SMOKE:LEFT|SMOKE:RIGHT"
            (Ok fan_result)
        in
        let* timer_result = wait_workflow timer_handle in
        require_completed "smoke.timer_then_activity" "SMOKE:TIMER"
          (Ok timer_result)
      in
      finish result

(** Reports the final typed result and converts it to a process exit code. *)
let () =
  match run () with
  | Ok () -> Printf.printf "two-binary driver assertions passed\n%!"
  | Error error -> exit (fail "two-binary driver" error)
