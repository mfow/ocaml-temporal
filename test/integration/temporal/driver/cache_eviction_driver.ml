(** Driver for the live sticky-cache eviction acceptance.

    The client starts two exact runs while the worker is configured with one
    Core cache slot. Each run schedules a long deterministic timer, so the
    first task reaches a durable pending boundary. Before admitting B, the
    driver makes a read-only query of A and requires its typed response. That
    gives the native stream a completed follow-up activation boundary, rather
    than racing the marker published immediately after the initial completion
    with subsequent Core processing. It then requires the worker's
    payload-free eviction marker. The second run's normal-completion marker is
    retained only as timeout diagnostics: pinned Core ordering buffers B until
    A's [RemoveFromCache(CacheFull)] activation has been acknowledged, so B is
    never evidence that eviction has happened. It cancels both exact runs and
    requires each to reach Temporal's typed cancellation outcome. The client
    never registers or executes workflow code. *)

module Client = Temporal.Client
module Error = Temporal.Error
module Definitions = Smoke_definitions

(** Records one client-driver phase without exposing payloads or native
    connection details. The phase markers distinguish setup, start, marker,
    cancellation, and wait failures when a live RPC returns a generic bridge
    status. *)
let phase operation status =
  Printf.eprintf "cache eviction phase=%s status=%s\n%!" operation status

(** Reads a required non-empty environment value as a typed configuration
    result rather than raising while the executable is initializing. *)
let required_env name =
  match Sys.getenv_opt name with
  | Some value when value <> "" -> Ok value
  | _ -> Error (Error.defect ~message:(name ^ " must not be empty"))

(** Converts the bounded driver timeout into a positive floating-point wait
    budget used only for the file-backed test coordination marker. *)
let timeout_seconds () =
  match Sys.getenv_opt "SMOKE_DRIVER_TIMEOUT_SECONDS" with
  | None -> Ok 300.
  | Some value -> (
      try
        let seconds = float_of_string value in
        if seconds > 0. then Ok seconds
        else Error (Error.defect ~message:"SMOKE_DRIVER_TIMEOUT_SECONDS must be positive")
      with _ ->
        Error
          (Error.defect
             ~message:"SMOKE_DRIVER_TIMEOUT_SECONDS must be a number"))

(** Removes a prior coordination marker and verifies that the shared bind
    mount no longer exposes it. Failing closed here prevents an interrupted
    acceptance run from being mistaken for the current workflow's readiness or
    eviction event. *)
let clear_marker path =
  try
    if Sys.file_exists path then Sys.remove path;
    if Sys.file_exists path then
      Error (Error.defect ~message:("cannot remove stale marker " ^ path))
    else Ok ()
  with exception_ ->
    Error
      (Error.defect
         ~message:
           (Printf.sprintf "cannot remove stale marker %s: %s" path
              (Printexc.to_string exception_)))

(** Returns whether a shared marker is non-empty and, when requested, exactly
    matches its expected payload. Atomic publication makes a complete read
    possible, while the payload check prevents a previous run's readiness token
    from satisfying the current run. *)
let marker_matches path expected =
  if not (Sys.file_exists path) || (Unix.stat path).Unix.st_size = 0 then false
  else
    match expected with
    | None -> true
    | Some expected -> (
        try
          let channel = open_in_bin path in
          Fun.protect
            ~finally:(fun () -> close_in_noerr channel)
            (fun () ->
              let contents =
                really_input_string channel (in_channel_length channel)
              in
              String.equal contents expected)
        with _ -> false)

(** Waits for A's cache-full marker while retaining B's completion marker as a
    diagnostic only. Core buffers B when the one-slot cache is full and only
    releases it after A's cache-removal activation is acknowledged; therefore
    B cannot satisfy this acceptance condition. One deadline covers both
    observations so a late B marker cannot silently grant a second timeout
    budget before the required eviction arrives. *)
let wait_for_eviction_with_second_diagnostic ~eviction ~second_ready ~timeout =
  let deadline = Unix.gettimeofday () +. timeout in
  let rec loop saw_second_acknowledgement =
    if marker_matches eviction None then Ok ()
    else if Unix.gettimeofday () >= deadline then
      let message =
        if saw_second_acknowledgement then
          "second workflow was acknowledged but A cache-full eviction marker was not published"
        else
          "neither A cache-full eviction marker nor second workflow acknowledgement was published"
      in
      Error (Error.defect ~message)
    else begin
      let saw_second_acknowledgement =
        saw_second_acknowledgement
        || marker_matches second_ready (Some "initial-completion\n")
      in
      Unix.sleepf 0.1;
      loop saw_second_acknowledgement
    end
  in
  loop false

(** Requires an exact run to report Temporal's typed cancellation category. *)
let require_cancelled label = function
  | Client.Cancelled error ->
      let view = Error.view error in
      if view.category = `Cancelled && not view.non_retryable then Ok ()
      else
        Error
          (Error.defect
             ~message:
               (Printf.sprintf "%s returned unexpected cancellation metadata"
                  label))
  | Client.Completed _ ->
      Error (Error.defect ~message:(label ^ " completed instead of cancelling"))
  | Client.Failed error
  | Client.Terminated error
  | Client.Timed_out error ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "%s ended as %s: %s" label (Error.kind error)
                (Error.message error)))
  | Client.Continued_as_new execution ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "%s continued as new at run %s" label
                execution.run_id))

(** Cancels one exact run with a distinct idempotency key. *)
let cancel handle ~request_id =
  Client.cancel ~request_id ~reason:"cache eviction acceptance requested" handle

(** Validates the fixed response from A's read-only cache-settling query.
    A mismatched value is a typed driver defect, rather than an accidental
    signal that any successful query response is sufficient evidence. *)
let require_resident = function
  | "resident" -> Ok ()
  | value ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "cache-settling query returned unexpected value %S"
                value))

(** Identifies the bounded set of Temporal RPC failures that can be transient
    while a sticky-cache worker is draining. The query is a control-plane
    observation, not the eviction assertion itself, so a short-lived
    [failed_precondition] or transport deadline must not turn an otherwise
    healthy cache transition into a false negative. Codec, protocol, and
    workflow-handler errors remain terminal and are never retried. *)
let transient_query_error error =
  let view = Error.view error in
  if view.non_retryable || view.category <> `Bridge then false
  else
    let prefix = "Temporal client RPC failed: " in
    let message = view.message in
    if String.starts_with ~prefix message then
      let code =
        String.sub message (String.length prefix)
          (String.length message - String.length prefix)
      in
      List.mem code
        [ "aborted"; "deadline_exceeded"; "failed_precondition"; "internal";
          "resource_exhausted"; "unavailable" ]
    else false

(** Waits for the worker to answer the cache-settling query, retaining the
    last typed error if the bounded observation window expires. Temporal may
    briefly report that no poller was seen immediately after A's first task;
    retrying only the known transient RPC codes preserves strict failure for
    malformed responses and handler defects while allowing the worker to
    publish its next activation boundary. *)
let query_residency handle ~timeout =
  let deadline = Unix.gettimeofday () +. min timeout 60. in
  let rec loop () =
    match Client.query handle ~query:Definitions.cache_eviction_residency_query with
    | Ok value -> require_resident value
    | Error error when transient_query_error error ->
        if Unix.gettimeofday () >= deadline then Error error
        else begin
          phase "cache_settling" "query_retry";
          Unix.sleepf 0.5;
          loop ()
        end
    | Error error -> Error error
  in
  loop ()

(** Starts two exact executions against the one-slot worker cache. The
    cache-only workflow has no durable command before it parks, so each initial
    workflow task can complete as an empty non-terminal activation while Core
    retains the execution in its sticky cache. Admitting the second execution
    then forces a real cache-full eviction; the driver observes that marker and
    proves cancellation still reaches each exact run. *)
let run () =
  match Sys.getenv_opt "TEMPORAL_TWO_BINARY_LIVE" with
  | Some "1" ->
      let open Temporal.Result_syntax in
      let* target_url = required_env "TEMPORAL_ADDRESS" in
      let* namespace = required_env "TEMPORAL_NAMESPACE" in
      let* marker = required_env "SMOKE_CACHE_EVICTION_FILE" in
      let* second_ready =
        required_env "SMOKE_CACHE_EVICTION_SECOND_READY_FILE"
      in
      let* timeout = timeout_seconds () in
      phase "client_create" "begin";
      let* client =
        Client.create ~target_url ~namespace
          ~identity:"ocaml-temporal-cache-eviction-driver" ()
      in
      phase "client_create" "ok";
      let finish result =
        match Client.shutdown client with
        | Ok () ->
            phase "client_shutdown" "ok";
            result
        | Error error -> Error error
      in
      let result =
        let* () = clear_marker marker in
        let* () = clear_marker second_ready in
        phase "start_a" "begin";
        let* first =
          Client.start client ~workflow:Definitions.cache_eviction
            ~task_queue:Definitions.task_queue
            ~id:"two-binary-cache-eviction-a" ~input:"first" ()
        in
        phase "start_a" "ok";
        (* The typed query is the synchronization barrier. It cannot return
           until the worker has accepted and executed a workflow activation,
           so it is stronger than a best-effort filesystem callback that may
           be delayed while Core is processing the same completion. *)
        phase "cache_settling" "begin";
        let* () = query_residency first ~timeout in
        phase "cache_settling" "observed";
        phase "start_b" "begin";
        let* second =
          Client.start client ~workflow:Definitions.cache_eviction
            ~task_queue:Definitions.task_queue
            ~id:"two-binary-cache-eviction-b" ~input:"second" ()
        in
        phase "start_b" "ok";
        phase "eviction_marker" "begin";
        let* () =
          wait_for_eviction_with_second_diagnostic ~eviction:marker
            ~second_ready ~timeout
        in
        phase "eviction_marker" "observed";
        phase "cancel_b" "begin";
        let* () = cancel second ~request_id:"two-binary-cache-eviction-cancel-b" in
        phase "cancel_b" "ok";
        phase "wait_b" "begin";
        let* second_outcome = Client.wait second in
        phase "wait_b" "ok";
        let* () = require_cancelled "cache eviction run B" second_outcome in
        phase "cancel_a" "begin";
        let* () = cancel first ~request_id:"two-binary-cache-eviction-cancel-a" in
        phase "cancel_a" "ok";
        phase "wait_a" "begin";
        let* first_outcome = Client.wait first in
        phase "wait_a" "ok";
        require_cancelled "cache eviction run A" first_outcome
      in
      finish result
  | _ ->
      Error
        (Error.defect
           ~message:
             "cache eviction acceptance is not enabled; set TEMPORAL_TWO_BINARY_LIVE=1")

(** Converts the typed process result into a useful command status without
    exposing workflow payloads or native protocol details. *)
let () =
  match run () with
  | Ok () -> Printf.printf "cache eviction driver assertions passed\n%!"
  | Error error ->
      Printf.eprintf "cache eviction driver failed (%s): %s\n%!"
        (Error.kind error) (Error.message error);
      exit 1
