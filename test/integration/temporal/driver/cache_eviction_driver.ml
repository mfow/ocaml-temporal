(** Driver for the live sticky-cache eviction acceptance.

    The client starts two exact runs while the worker is configured with one
    Core cache slot. It waits for the worker's payload-free eviction marker,
    cancels both runs by their returned handles, and requires both exact runs
    to reach Temporal's typed cancellation outcome. The client never registers
    or executes workflow code. *)

module Client = Temporal.Client
module Error = Temporal.Error
module Definitions = Smoke_definitions

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

(** Waits for an atomically published worker marker. A bounded wait keeps a
    missing eviction activation a normal driver failure instead of letting CI
    hang until its global job timeout. *)
let wait_for_marker ?expected path ~timeout =
  let deadline = Unix.gettimeofday () +. timeout in
  let rec loop () =
    if marker_matches path expected then Ok ()
    else if Unix.gettimeofday () >= deadline then
      Error (Error.defect ~message:"cache eviction marker was not published")
    else begin
      Unix.sleepf 0.1;
      loop ()
    end
  in
  loop ()

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
      let* timeout = timeout_seconds () in
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
        let* () = clear_marker marker in
        let* first =
          Client.start client ~workflow:Definitions.cache_eviction
            ~task_queue:Definitions.task_queue
            ~id:"two-binary-cache-eviction-a" ~input:"first" ()
        in
        let* second =
          Client.start client ~workflow:Definitions.cache_eviction
            ~task_queue:Definitions.task_queue
            ~id:"two-binary-cache-eviction-b" ~input:"second" ()
        in
        let* () = wait_for_marker marker ~timeout in
        let* () = cancel first ~request_id:"two-binary-cache-eviction-cancel-a" in
        let* () = cancel second ~request_id:"two-binary-cache-eviction-cancel-b" in
        let* first_outcome = Client.wait first in
        let* second_outcome = Client.wait second in
        let* () = require_cancelled "cache eviction run A" first_outcome in
        require_cancelled "cache eviction run B" second_outcome
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
