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

(** Builds the short, bounded policy used by [activity_retry]. Keeping this as
    a result lets the workflow return a typed configuration defect if the
    public constructor's validation ever changes, instead of hiding a
    construction exception in a module initializer. *)
let retry_policy =
  Temporal.Activity.Retry_policy.make
    ~initial_interval:(Temporal.Duration.of_ms 100L)
    ~backoff_coefficient:1.0
    ~maximum_interval:(Temporal.Duration.of_ms 100L) ~maximum_attempts:2 ()

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
