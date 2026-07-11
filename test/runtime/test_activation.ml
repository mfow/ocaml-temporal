module Activation = Temporal_runtime.Activation
module Execution = Temporal_runtime.Execution

(** Builds a string payload through the public codec for activation fixtures. *)
let payload value =
  match Temporal.Codec.encode Temporal.Codec.string value with
  | Ok payload -> payload
  | Error error -> failwith (Temporal.Error.message error)

(** Remote activity called by the greeting workflow fixture. *)
let greeting =
  Temporal.Activity.remote ~name:"greeting" ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

(** Workflow fixture that schedules an activity, waits, sleeps, and completes. *)
let workflow input =
  let open Temporal.Result_syntax in
  assert (Temporal.Workflow_context.is_active ());
  let pending = Temporal.Activity.start greeting input in
  let* greeting = Temporal.Future.await pending in
  let* () = Temporal.Workflow.sleep (Temporal.Duration.of_ms 10L) in
  Ok (greeting ^ "!")

(** Typed definition for the main activation scenario. *)
let greeting_workflow =
  Temporal.Workflow.define ~name:"greeting_workflow"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string workflow

(** Compares expected activation values with a labelled failure message. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Exercises the full synthetic start/activity/timer/completion sequence. *)
let test_commands_and_completion () =
  let execution = Execution.start greeting_workflow "Ada" in
  expect "activity command"
    [
      Activation.Schedule_activity
        { seq = 1L; name = "greeting"; input = payload "Ada" };
    ]
    (Execution.activate execution [ Activation.Start_workflow ]);
  expect "timer command"
    [ Activation.Start_timer { seq = 2L; milliseconds = 10L } ]
    (Execution.activate execution
       [
         Activation.Resolve_activity
           { seq = 1L; result = Ok (payload "Hello Ada") };
       ]);
  expect "workflow complete"
    [ Activation.Complete_workflow (payload "Hello Ada!") ]
    (Execution.activate execution [ Activation.Fire_timer { seq = 2L } ]);
  expect "completion emitted once" [] (Execution.activate execution [])

(** Runs the greeting fixture and returns all emitted command batches for replay
    comparison. *)
let run_greeting () =
  let execution = Execution.start greeting_workflow "Ada" in
  [
    Execution.activate execution [ Activation.Start_workflow ];
    Execution.activate execution
      [
        Activation.Resolve_activity
          { seq = 1L; result = Ok (payload "Hello Ada") };
      ];
    Execution.activate execution [ Activation.Fire_timer { seq = 2L } ];
  ]

(** Confirms identical inputs and activation jobs produce identical commands. *)
let test_replay_is_stable () =
  expect "replay command bytes" (run_greeting ()) (run_greeting ())

(** Creates one named activity used to observe result-delivery order. *)
let ordering_activity name =
  Temporal.Activity.remote ~name ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

(** Workflow fixture whose output records the order in which two results resume
    their fibers. *)
let ordered_workflow =
  Temporal.Workflow.define ~name:"ordered" ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.string (fun () ->
      let observed = ref [] in
      let first = Temporal.Activity.start (ordering_activity "first") "first" in
      let second = Temporal.Activity.start (ordering_activity "second") "second" in
      let first =
        Temporal.Future.map
          (fun value ->
            observed := value :: !observed;
            value)
          first
      in
      let second =
        Temporal.Future.map
          (fun value ->
            observed := value :: !observed;
            value)
          second
      in
      match Temporal.Future.await (Temporal.Future.both first second) with
      | Error error -> Error error
      | Ok _ -> Ok (String.concat "," (List.rev !observed)))

(** Applies a chosen result-job order and checks the resulting workflow output. *)
let resolve_order jobs expected =
  let execution = Execution.start ordered_workflow () in
  ignore (Execution.activate execution [ Activation.Start_workflow ]);
  expect "explicit resolution order"
    [ Activation.Complete_workflow (payload expected) ]
    (Execution.activate execution jobs)

(** Confirms activation job order determines runnable-fiber order. *)
let test_resolution_job_order () =
  let first =
    Activation.Resolve_activity { seq = 1L; result = Ok (payload "first") }
  in
  let second =
    Activation.Resolve_activity { seq = 2L; result = Ok (payload "second") }
  in
  resolve_order [ second; first ] "second,first";
  resolve_order [ first; second ] "first,second"

(** Confirms duplicate starts and unknown activity/timer sequences fail as
    non-retryable bridge defects. *)
let test_bridge_defects () =
  let execution = Execution.start greeting_workflow "Ada" in
  ignore (Execution.activate execution [ Activation.Start_workflow ]);
  (match
     Execution.activate execution
       [ Activation.Fire_timer { seq = 999L } ]
   with
  | [ Activation.Fail_workflow error ] ->
      expect "bridge category" "bridge" (Temporal.Error.kind error)
  | _ -> failwith "unknown timer did not fail the workflow");
  let duplicate = Execution.start greeting_workflow "Ada" in
  ignore (Execution.activate duplicate [ Activation.Start_workflow ]);
  ignore
    (Execution.activate duplicate
       [
         Activation.Resolve_activity
           { seq = 1L; result = Ok (payload "Hello Ada") };
       ]);
  match
    Execution.activate duplicate
      [
        Activation.Resolve_activity
          { seq = 1L; result = Ok (payload "Hello again") };
      ]
  with
  | [ Activation.Fail_workflow error ] ->
      expect "duplicate category" "bridge" (Temporal.Error.kind error)
  | _ -> failwith "duplicate activity resolution was accepted"

(** Workflow fixture proving that a zero-duration sleep emits no timer. *)
let zero_sleep_workflow =
  Temporal.Workflow.define ~name:"zero_sleep" ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.unit (fun () ->
      match Temporal.Workflow.sleep (Temporal.Duration.of_ms 0L) with
      | Ok () -> Ok ()
      | Error error -> Error error)

(** Workflow fixture that proves timers can be scheduled independently before
    the workflow chooses which one to await. *)
let concurrent_sleep_workflow =
  Temporal.Workflow.define ~name:"concurrent_sleep" ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.string (fun () ->
      let first =
        Temporal.Workflow.start_sleep (Temporal.Duration.of_ms 5L)
      in
      let _second =
        Temporal.Workflow.start_sleep (Temporal.Duration.of_ms 10L)
      in
      match Temporal.Future.await first with
      | Ok () -> Ok "first fired"
      | Error error -> Error error)

(** Workflow fixture that inspects the zero-duration future without waiting,
    proving it is immediately ready and history-neutral. *)
let zero_start_sleep_workflow =
  Temporal.Workflow.define ~name:"zero_start_sleep" ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.unit (fun () ->
      let timer =
        Temporal.Workflow.start_sleep (Temporal.Duration.of_ms 0L)
      in
      if Temporal.Future.is_ready timer then Temporal.Future.await timer
      else Error (Temporal.Error.defect ~message:"zero timer was pending"))

(** Covers zero sleep and rejection of negative durations. *)
let test_zero_sleep_and_duration_validation () =
  let execution = Execution.start zero_sleep_workflow () in
  expect "zero sleep has no timer"
    [
      Activation.Complete_workflow
        (match Temporal.Codec.encode Temporal.Codec.unit () with
        | Ok payload -> payload
        | Error error -> failwith (Temporal.Error.message error));
    ]
    (Execution.activate execution [ Activation.Start_workflow ]);
  match Temporal.Duration.of_ms (-1L) with
  | exception Invalid_argument _ -> ()
  | _ -> failwith "negative duration accepted"

(** Covers non-blocking timer creation, command order, selected waiting, and a
    ready zero-duration timer that emits no history command. *)
let test_start_sleep () =
  let concurrent = Execution.start concurrent_sleep_workflow () in
  expect "two timers start before waiting"
    [
      Activation.Start_timer { seq = 1L; milliseconds = 5L };
      Activation.Start_timer { seq = 2L; milliseconds = 10L };
    ]
    (Execution.activate concurrent [ Activation.Start_workflow ]);
  expect "awaited timer completes workflow"
    [ Activation.Complete_workflow (payload "first fired") ]
    (Execution.activate concurrent [ Activation.Fire_timer { seq = 1L } ]);
  let zero = Execution.start zero_start_sleep_workflow () in
  expect "zero start sleep has no command"
    [
      Activation.Complete_workflow
        (match Temporal.Codec.encode Temporal.Codec.unit () with
        | Ok payload -> payload
        | Error error -> failwith (Temporal.Error.message error));
    ]
    (Execution.activate zero [ Activation.Start_workflow ]);
  match
    Temporal.Workflow.start_sleep (Temporal.Duration.of_ms 1L)
    |> Temporal.Future.peek
  with
  | Some (Error error) ->
      expect "detached start sleep" "defect" (Temporal.Error.kind error)
  | Some (Ok ()) | None -> failwith "detached start sleep did not fail immediately"

(** Verifies cancellation emits once and cache removal releases blocked state
    without producing a workflow command. *)
let test_cancel_and_evict () =
  let cancelled = Execution.start greeting_workflow "Ada" in
  ignore (Execution.activate cancelled [ Activation.Start_workflow ]);
  expect "cancellation command" [ Activation.Cancel_workflow_execution ]
    (Execution.activate cancelled [ Activation.Cancel_workflow ]);
  let evicted = Execution.start greeting_workflow "Ada" in
  ignore (Execution.activate evicted [ Activation.Start_workflow ]);
  expect "eviction has no command" []
    (Execution.activate evicted [ Activation.Remove_from_cache ]);
  expect "evicted execution stays inert" []
    (Execution.activate evicted [ Activation.Start_workflow ])

let () =
  test_commands_and_completion ();
  test_replay_is_stable ();
  test_resolution_job_order ();
  test_bridge_defects ();
  test_zero_sleep_and_duration_validation ();
  test_start_sleep ();
  test_cancel_and_evict ()
