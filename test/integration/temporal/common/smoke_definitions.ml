(** Definitions shared by the live two-process Temporal acceptance test.

    Keeping the workflow and activity values in one private test library makes
    the driver and worker compile against exactly the same names and codecs. The
    workflow bodies deliberately contain no process, filesystem, network, or
    clock access: those operations would make them non-replayable. The one
    process-local counter below belongs only to the non-deterministic activity
    implementation and is never read by workflow code. *)

(** The task queue used only by this fixture. It is intentionally distinct from
    every production queue so an accidentally reused local namespace cannot
    dispatch test work to an application worker. *)
let task_queue = "ocaml-temporal-two-binary-smoke"

(** The mock activity uppercases its input. The implementation is deterministic
    so the driver can assert the exact result without depending on external
    services or wall-clock state. *)
let mock_transform =
  Temporal.Activity.define ~name:"smoke.mock_transform"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun input ->
      Ok (String.uppercase_ascii input))

(** Counts attempts for the intentionally transient activity used by the live
    retry scenario. This state belongs to the activity worker process rather
    than workflow state: activities are allowed to perform non-deterministic
    work, and a fresh worker process is started for every isolated Compose
    run. The atomic counter also keeps the fixture correct if the worker later
    dispatches activity tasks from more than one Domain. *)
let retry_once_attempts = Atomic.make 0

(** Fails exactly the first activity attempt and reports the attempt number on
    the succeeding call. The retryable [Activity] error leaves Temporal free
    to apply the workflow's explicit retry policy; the attempt suffix gives
    the driver an observable proof that the final result came from attempt 2. *)
let retry_once_activity =
  Temporal.Activity.define ~name:"smoke.retry_once"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun input ->
      let attempt = Atomic.fetch_and_add retry_once_attempts 1 + 1 in
      if attempt = 1 then
        Error
          (Temporal.Error.make ~category:`Activity
             ~message:"intentional transient failure for retry acceptance" ())
      else
        Ok
          (Printf.sprintf "%s:ATTEMPT:%d" (String.uppercase_ascii input)
             attempt))

(** The environment variable used by the cancellation handshake activity. The
    marker lives in the repository bind mount so the independent driver can
    observe completion without adding another Temporal workflow signal. *)
let cancellation_ready_file_env = "SMOKE_CANCELLATION_READY_FILE"

(** Checks the marker path before any activity attempts to write it. A live
    acceptance run must provide an absolute, non-empty path without NUL bytes;
    rejecting anything else keeps an accidental host-path or malformed
    environment setting from turning into an uncontrolled filesystem write. *)
let validate_cancellation_ready_file path =
  if String.equal path "" then
    Error
      (Temporal.Error.defect
         ~message:(cancellation_ready_file_env ^ " must not be empty"))
  else if String.contains path '\000' then
    Error
      (Temporal.Error.defect
         ~message:(cancellation_ready_file_env ^ " must not contain NUL"))
  else if Filename.is_relative path then
    Error
      (Temporal.Error.defect
         ~message:(cancellation_ready_file_env ^ " must be an absolute path"))
  else Ok path

(** Reads and validates the shared marker path from the process environment.
    This is called by both the worker startup path and the activity itself so
    configuration is checked before the worker becomes ready and again at the
    side-effect boundary. *)
let cancellation_ready_file () =
  match Sys.getenv_opt cancellation_ready_file_env with
  | None ->
      Error
        (Temporal.Error.defect
           ~message:(cancellation_ready_file_env ^ " must be set"))
  | Some path -> validate_cancellation_ready_file path

(** Removes a prior marker as best-effort test cleanup. The marker is not
    workflow state; deleting it between runs prevents a driver from mistaking
    a previous activity completion for the current execution's handshake. *)
let clear_cancellation_ready_file path =
  try if Sys.file_exists path then Sys.remove path with _ -> ()

(** Publishes the per-run token with a temporary file followed by [Sys.rename].
    The rename is atomic on the shared Linux bind mount, so the driver observes
    either the complete marker or no marker, never a partially written file.
    Any temporary file is removed on failure before a typed activity error is
    returned. *)
let publish_cancellation_ready path token =
  let temporary = Printf.sprintf "%s.tmp.%d" path (Unix.getpid ()) in
  let cleanup () =
    try if Sys.file_exists temporary then Sys.remove temporary with _ -> ()
  in
  try
    let channel = open_out_bin temporary in
    Fun.protect
      ~finally:(fun () -> close_out_noerr channel)
      (fun () ->
        output_string channel token;
        output_char channel '\n';
        flush channel);
    Sys.rename temporary path;
    Ok ()
  with exception_ ->
    cleanup ();
    Error
      (Temporal.Error.make ~category:`Activity
         ~message:
           (Printf.sprintf "cannot publish cancellation readiness marker: %s"
              (Printexc.to_string exception_))
         ())

(** A test-only activity that publishes the cancellation handshake token. Its
    filesystem side effect is intentionally isolated to the activity process;
    the workflow itself remains deterministic and only schedules this activity
    alongside its durable timer. *)
let cancellation_ready_activity =
  Temporal.Activity.define ~name:"smoke.cancellation_ready"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.unit (fun token ->
      match cancellation_ready_file () with
      | Error error -> Error error
      | Ok path -> publish_cancellation_ready path token)

(** Builds the short, bounded policy used by [activity_retry]. Keeping this as
    a result lets the workflow return a typed configuration defect if the
    public constructor's validation ever changes, instead of hiding a
    construction exception in a module initializer. *)
let retry_policy =
  Temporal.Activity.Retry_policy.make
    ~initial_interval:(Temporal.Duration.of_ms 100L)
    ~backoff_coefficient:1.0
    ~maximum_interval:(Temporal.Duration.of_ms 100L) ~maximum_attempts:2 ()

(** The heartbeat acceptance activity and workflow use a deliberately short
    timeout. It is long enough for the Core heartbeat manager to flush one
    request over the local Compose network, while still proving that the
    timeout delivered in the next activity attempt is the value selected by
    the workflow command. *)
let heartbeat_timeout = Temporal.Duration.of_ms 500L

(** The first heartbeat detail is a stable, human-readable marker. It is
    written by the first activity attempt and must be returned by Temporal in
    the retrying attempt's [Context.details] list; the driver never relies on
    worker-local mutable state for this assertion. *)
let heartbeat_progress_detail = "SMOKE:HEARTBEAT:PROGRESS:1"

(** Verifies the timeout that Temporal attached to this activity attempt. A
    missing or changed timeout is a protocol/configuration defect rather than
    an ordinary retryable activity failure, so the activity reports it as a
    non-retryable typed error. *)
let require_heartbeat_timeout context =
  match Temporal.Activity.Context.heartbeat_timeout context with
  | Some timeout
    when Int64.equal
           (Temporal.Duration.to_ms timeout)
           (Temporal.Duration.to_ms heartbeat_timeout) ->
      Ok ()
  | Some timeout ->
      Error
        (Temporal.Error.defect
           ~message:
             (Printf.sprintf
                "heartbeat timeout was %Ldms, expected %Ldms"
                (Temporal.Duration.to_ms timeout)
                (Temporal.Duration.to_ms heartbeat_timeout)))
  | None ->
      Error
        (Temporal.Error.defect
           ~message:"heartbeat timeout was absent from the activity attempt")

(** Sends one heartbeat, waits briefly for Core's asynchronous heartbeat
    manager to flush it, then returns a retryable application failure. The
    deliberate delay is activity-side code and cannot affect workflow replay;
    it prevents the immediate failure completion from racing the heartbeat
    request on a busy local Temporal Server. *)
let heartbeat_retry_activity =
  Temporal.Activity.define_with_context ~name:"smoke.heartbeat_retry"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string
    (fun context input ->
      let open Temporal.Result_syntax in
      let* () = require_heartbeat_timeout context in
      match Temporal.Activity.Context.details context with
      | [] ->
          let* () =
            Temporal.Activity.Context.heartbeat context Temporal.Codec.string
              heartbeat_progress_detail
          in
          Unix.sleepf 0.1;
          Error
            (Temporal.Error.make ~category:`Activity
               ~message:
                 "intentional retry after recording an activity heartbeat" ())
      | [ detail ] ->
          let* progress = Temporal.Codec.decode Temporal.Codec.string detail in
          if String.equal progress heartbeat_progress_detail then
            Ok ("SMOKE:HEARTBEAT:RETRIED:" ^ String.uppercase_ascii input)
          else
            Error
              (Temporal.Error.defect
                 ~message:
                   (Printf.sprintf
                      "unexpected heartbeat detail %S on retry" progress))
      | details ->
          Error
            (Temporal.Error.defect
               ~message:
                 (Printf.sprintf
                    "expected one heartbeat detail on retry, received %d"
                    (List.length details))))

(** Starts two independent activity commands before awaiting either result.
    [Future.all] preserves input order, making this scenario test both fan-out
    and deterministic aggregation when the live worker path exercises the
    complete activity command fields. *)
let fan_out =
  Temporal.Workflow.define ~name:"smoke.fan_out" ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string (fun seed ->
      let left = Temporal.Activity.start mock_transform (seed ^ ":left") in
      let right = Temporal.Activity.start mock_transform (seed ^ ":right") in
      match Temporal.Future.await (Temporal.Future.all [ left; right ]) with
      | Error error -> Error error
      | Ok values -> Ok (String.concat "|" values))

(** Waits for a short durable timer before scheduling one activity. The timer is
    an SDK command rather than an OCaml sleep, so replay can resolve it from
    Temporal history when this definition is run by Core. *)
let timer_then_activity =
  Temporal.Workflow.define ~name:"smoke.timer_then_activity"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      match Temporal.Workflow.sleep (Temporal.Duration.of_ms 10L) with
      | Error error -> Error error
      | Ok () -> Temporal.Activity.execute mock_transform (seed ^ ":timer"))

(** Schedules the transient activity with an explicit two-attempt retry policy.
    The activity itself fails once and then succeeds, so a live terminal result
    ending in [ATTEMPT:2] proves that Temporal Server and Core delivered a
    second activity task rather than the worker merely returning a local
    success. *)
let activity_retry =
  Temporal.Workflow.define ~name:"smoke.activity_retry"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      match retry_policy with
      | Error error -> Error error
      | Ok policy ->
          Temporal.Activity.execute ~retry_policy:policy retry_once_activity
            seed)

(** Schedules [heartbeat_retry_activity] with both an explicit heartbeat
    timeout and the same bounded two-attempt retry policy used elsewhere in the
    fixture. The first attempt records progress and fails; the second attempt
    can finish only when Temporal has returned that progress detail and the
    configured timeout through its activity-task context. *)
let activity_heartbeat_retry =
  Temporal.Workflow.define ~name:"smoke.activity_heartbeat_retry"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      match retry_policy with
      | Error error -> Error error
      | Ok policy ->
          Temporal.Activity.execute ~heartbeat_timeout
            ~retry_policy:policy heartbeat_retry_activity seed)

(** A child workflow that waits on a short durable timer before deriving its
    result entirely from its input. The timer is deliberately inside the child
    rather than the parent so the live scenario exercises child start and
    terminal resolution as well as a child-owned timer activation. *)
let child_after_timer =
  Temporal.Workflow.define ~name:"smoke.child_after_timer"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      match Temporal.Workflow.sleep (Temporal.Duration.of_ms 10L) with
      | Error error -> Error error
      | Ok () -> Ok (String.uppercase_ascii (seed ^ ":child")))

(** Starts [child_after_timer] with an identity derived only from the parent
    input, then waits through the public direct-style child helper. The fixed
    prefix keeps this fixture's child execution distinct from the driver's
    top-level workflow IDs, while deriving the suffix deterministically keeps
    the child command stable when Temporal replays the parent. *)
let parent_awaits_child =
  Temporal.Workflow.define ~name:"smoke.parent_awaits_child"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      Temporal.Child_workflow.execute
        ~id:("two-binary-parent-child-" ^ seed)
        child_after_timer seed)

(** Deliberately fails the workflow with a typed, non-retryable application
    error. The failure is deterministic and does not inspect its input, so a
    replay observes the same terminal command. The live driver checks the
    category, retry policy, and stable message prefix rather than matching
    Core's full diagnostic text, which may include source and failure-info
    context. *)
let non_retryable_failure =
  Temporal.Workflow.define ~name:"smoke.non_retryable_failure"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun _seed ->
      Error
        (Temporal.Error.make ~non_retryable:true ~category:`Workflow
           ~message:"intentional terminal workflow failure" ()))

(** Keeps a workflow execution open on a durable timer until the driver asks
    Temporal to cancel its exact run. The long interval is intentional: the
    workflow must still be outstanding when [Temporal.Client.cancel] returns,
    while the body remains replay-safe because it uses no wall clock, random
    value, I/O, or process-global state. Cancellation is converted by the
    native worker into a terminal [Cancel_workflow_execution] command before
    the timer fires. *)
let long_running_cancellation =
  Temporal.Workflow.define ~name:"smoke.long_running_cancellation"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun token ->
      let timer = Temporal.Workflow.start_sleep (Temporal.Duration.of_ms 30_000L) in
      let marker =
        Temporal.Activity.start ~do_not_eagerly_execute:true
          cancellation_ready_activity token
      in
      match Temporal.Future.await (Temporal.Future.both timer marker) with
      | Error error -> Error error
      | Ok ((), ()) -> Ok (String.uppercase_ascii (token ^ ":finished")))
