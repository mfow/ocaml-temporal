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

(** Remote child definition used to verify that a parent command carries the
    explicit durable child ID, workflow type, encoded input, and typed output
    codec without exposing runtime sequence numbers to application code. *)
let greeting_child =
  Temporal.Workflow.remote ~name:"greeting_child" ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

(** Parent fixture that exercises the direct-style child convenience function.
    The child call suspends this workflow until the matching resolution job is
    delivered by the synthetic Core boundary. *)
let child_parent_workflow =
  Temporal.Workflow.define ~name:"child_parent" ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string (fun input ->
      Temporal.Child_workflow.execute ~id:"greeting/Ada" greeting_child input)

(** Compares expected activation values with a labelled failure message. *)
let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

(** Exercises the full synthetic start/activity/timer/completion sequence. *)
let test_commands_and_completion () =
  let execution = Execution.start greeting_workflow "Ada" in
  expect "activity command"
    [
      Activation.Schedule_activity
        {
          seq = 1L;
          activity_id = "ocaml-activity-1";
          activity_type = "greeting";
          task_queue = "default";
          arguments = [ payload "Ada" ];
          schedule_to_close_timeout = None;
          schedule_to_start_timeout = None;
          start_to_close_timeout = Some 60_000L;
          heartbeat_timeout = None;
          cancellation_type = Activation.Try_cancel;
          do_not_eagerly_execute = false;
        };
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

(** Explicit activity labels survive command construction, while an execution's
    configured worker queue supplies the default for ordinary [Activity.start]
    calls. *)
let test_activity_options_and_queue () =
  let explicit_workflow =
    Temporal.Workflow.define ~name:"explicit_activity_options"
      ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun input ->
        Temporal.Activity.start ~activity_id:"lookup-42" ~task_queue:"fast-lane"
          ~schedule_to_close_timeout:(Temporal.Duration.of_ms 5_000L)
          ~heartbeat_timeout:(Temporal.Duration.of_ms 1_000L)
          ~cancellation_type:Temporal.Activity.Wait_cancellation_completed
          ~do_not_eagerly_execute:true greeting input
        |> Temporal.Future.await)
  in
  let explicit = Execution.start ~task_queue:"worker-default" explicit_workflow "Ada" in
  begin match Execution.activate explicit [ Activation.Start_workflow ] with
  | [
      Activation.Schedule_activity
        {
          seq = 1L;
          activity_id = "lookup-42";
          activity_type = "greeting";
          task_queue = "fast-lane";
          arguments = [ argument ];
          schedule_to_close_timeout = Some 5_000L;
          schedule_to_start_timeout = None;
          start_to_close_timeout = None;
          heartbeat_timeout = Some 1_000L;
          cancellation_type = Activation.Wait_cancellation_completed;
          do_not_eagerly_execute = true;
        };
    ] when argument = payload "Ada" -> ()
  | _ -> failwith "explicit activity options were not preserved"
  end;
  let defaulted = Execution.start ~task_queue:"worker-default" greeting_workflow "Ada" in
  begin match Execution.activate defaulted [ Activation.Start_workflow ] with
  | [ Activation.Schedule_activity { task_queue = "worker-default"; _ } ] -> ()
  | _ -> failwith "execution task queue was not used as activity default"
  end;
  let start_only_workflow =
    Temporal.Workflow.define ~name:"start_only_activity_options"
      ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun input ->
        Temporal.Activity.start
          ~schedule_to_start_timeout:(Temporal.Duration.of_ms 2_000L)
          greeting input
        |> Temporal.Future.await)
  in
  let start_only = Execution.start start_only_workflow "Ada" in
  begin match Execution.activate start_only [ Activation.Start_workflow ] with
  | [
      Activation.Schedule_activity
        {
          schedule_to_start_timeout = Some 2_000L;
          start_to_close_timeout = Some 60_000L;
          _;
        };
    ] -> ()
  | _ -> failwith "activity default timeout was not applied with schedule-to-start"
  end

(** Invalid optional activity identity is returned through the workflow's
    typed failure path and does not emit a schedule command. *)
let test_invalid_activity_options_do_not_schedule () =
  let invalid_workflow =
    Temporal.Workflow.define ~name:"invalid_activity_options"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Temporal.Activity.start ~task_queue:"" greeting "ignored"
        |> Temporal.Future.await
        |> Result.map (fun _ -> ()))
  in
  let execution = Execution.start invalid_workflow () in
  match Execution.activate execution [ Activation.Start_workflow ] with
  | [ Activation.Fail_workflow error ] when Temporal.Error.kind error = "defect" ->
      ()
  | [ Activation.Schedule_activity _ ] ->
      failwith "invalid activity queue emitted a schedule command"
  | _ -> failwith "invalid activity options did not fail through the workflow"

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

(** Workflow fixture combining the identity value [all []] with a normal timer.
    The empty aggregate must inherit this execution's scheduler ownership so it
    composes without a false cross-execution defect. *)
let empty_all_with_timer_workflow =
  Temporal.Workflow.define ~name:"empty_all_with_timer"
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
      let empty = Temporal.Future.all [] in
      let timer =
        Temporal.Workflow.start_sleep (Temporal.Duration.of_ms 1L)
      in
      match Temporal.Future.await (Temporal.Future.both empty timer) with
      | Ok ([], ()) -> Ok ()
      | Ok (_ :: _, ()) ->
          Error (Temporal.Error.defect ~message:"empty all returned a value")
      | Error error -> Error error)

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

(** Proves an empty aggregate created inside a workflow is owned by that
    workflow and can be paired with an ordinary pending operation. *)
let test_empty_all_uses_current_workflow_owner () =
  let execution = Execution.start empty_all_with_timer_workflow () in
  expect "empty all composes with timer"
    [ Activation.Start_timer { seq = 1L; milliseconds = 1L } ]
    (Execution.activate execution [ Activation.Start_workflow ]);
  expect "empty all and timer complete"
    [
      Activation.Complete_workflow
        (match Temporal.Codec.encode Temporal.Codec.unit () with
        | Ok payload -> payload
        | Error error -> failwith (Temporal.Error.message error));
    ]
    (Execution.activate execution [ Activation.Fire_timer { seq = 1L } ])

(** Covers the child schedule/resolution path and proves the application-owned
    ID is emitted unchanged as durable command data. *)
let test_child_workflow_completion () =
  let execution = Execution.start child_parent_workflow "Ada" in
  expect "child schedule command"
    [
      Activation.Start_child_workflow
        {
          seq = 1L;
          id = "greeting/Ada";
          name = "greeting_child";
          input = payload "Ada";
        };
    ]
    (Execution.activate execution [ Activation.Start_workflow ]);
  expect "child result completes parent"
    [ Activation.Complete_workflow (payload "Hello Ada") ]
    (Execution.activate execution
       [
         Activation.Resolve_child_workflow
           { seq = 1L; result = Ok (payload "Hello Ada") };
       ])

(** Parent fixture that starts a child and activity before awaiting either.
    Their shared sequence space and source order must be replay-stable. *)
let concurrent_child_parent =
  Temporal.Workflow.define ~name:"concurrent_child_parent"
    ~input:Temporal.Codec.unit ~output:Temporal.Codec.string (fun () ->
      let child =
        Temporal.Child_workflow.start ~id:"child-1" greeting_child "Ada"
      in
      let activity = Temporal.Activity.start greeting "Grace" in
      match Temporal.Future.await (Temporal.Future.both child activity) with
      | Ok (child, activity) -> Ok (child ^ "," ^ activity)
      | Error error -> Error error)

(** Verifies children and activities can be started concurrently and that
    child output decoding happens before the typed future is resolved. *)
let test_child_workflow_concurrency_and_decoding () =
  let execution = Execution.start concurrent_child_parent () in
  expect "child and activity command order"
    [
      Activation.Start_child_workflow
        {
          seq = 1L;
          id = "child-1";
          name = "greeting_child";
          input = payload "Ada";
        };
      Activation.Schedule_activity
        {
          seq = 2L;
          activity_id = "ocaml-activity-2";
          activity_type = "greeting";
          task_queue = "default";
          arguments = [ payload "Grace" ];
          schedule_to_close_timeout = None;
          schedule_to_start_timeout = None;
          start_to_close_timeout = Some 60_000L;
          heartbeat_timeout = None;
          cancellation_type = Activation.Try_cancel;
          do_not_eagerly_execute = false;
        };
    ]
    (Execution.activate execution [ Activation.Start_workflow ]);
  expect "one concurrent result leaves parent pending" []
    (Execution.activate execution
       [
         Activation.Resolve_child_workflow
           { seq = 1L; result = Ok (payload "Hello Ada") };
       ]);
  expect "both child and activity results complete parent"
    [ Activation.Complete_workflow (payload "Hello Ada,Hello Grace") ]
    (Execution.activate execution
       [
         Activation.Resolve_activity
           { seq = 2L; result = Ok (payload "Hello Grace") };
       ]);
  let invalid = Execution.start child_parent_workflow "Ada" in
  ignore (Execution.activate invalid [ Activation.Start_workflow ]);
  match
    Execution.activate invalid
      [
        Activation.Resolve_child_workflow
          {
            seq = 1L;
            result =
              Ok
                {
                  Temporal.Payload.metadata = [ ("encoding", "binary/plain") ];
                  data = Bytes.of_string "wrong codec";
                };
          };
      ]
  with
  | [ Activation.Fail_workflow error ] ->
      expect "child output codec failure" "codec" (Temporal.Error.kind error)
  | _ -> failwith "invalid child output did not fail the parent"

(** Codec fixture that rejects every input, used to prove encoding happens
    before command sequence allocation or mutation of workflow history. *)
let rejecting_child_input =
  Temporal.Codec.make ~encoding:"test/reject"
    ~encode:(fun _ -> Error (Temporal.Error.codec ~message:"input rejected"))
    ~decode:(fun _ -> Ok ())

(** Checks that validation returned a ready defect with the expected message,
    allowing the fixture to continue and prove a later valid child receives
    sequence one. *)
let require_ready_child_id_error expected future =
  match Temporal.Future.peek future with
  | Some (Error error) when String.equal (Temporal.Error.message error) expected ->
      Ok ()
  | Some (Error error) ->
      Error
        (Temporal.Error.defect
           ~message:
             (Printf.sprintf "unexpected child ID error: %s"
                (Temporal.Error.message error)))
  | Some (Ok _) ->
      Error (Temporal.Error.defect ~message:"invalid child ID succeeded")
  | None ->
      Error (Temporal.Error.defect ~message:"invalid child ID remained pending")

(** Rejects IDs that cannot cross the strict JSON boundary, then starts one
    valid child. This makes sequence allocation observable after both failures. *)
let child_id_boundary_workflow =
  Temporal.Workflow.define ~name:"child_id_boundary" ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.string (fun () ->
      let open Temporal.Result_syntax in
      let oversized = String.make 65_537 'x' in
      let* () =
        Temporal.Child_workflow.start ~id:oversized greeting_child "oversized"
        |> require_ready_child_id_error
             "child workflow id exceeds 65536 UTF-8 bytes"
      in
      let invalid_utf_8 = String.make 1 (Char.chr 0xff) in
      let* () =
        Temporal.Child_workflow.start ~id:invalid_utf_8 greeting_child "invalid"
        |> require_ready_child_id_error
             "child workflow id must be valid UTF-8"
      in
      Temporal.Child_workflow.execute ~id:"after-invalid" greeting_child "Ada")

(** Invalid IDs and codec inputs are expected failures. They produce no child
    command, and detached calls return a ready typed defect rather than raising
    or performing a private suspension effect. *)
let test_child_workflow_validation () =
  let empty_id =
    Temporal.Workflow.define ~name:"empty_child_id" ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit (fun () ->
        let target =
          Temporal.Workflow.remote ~name:"child" ~input:Temporal.Codec.unit
            ~output:Temporal.Codec.unit
        in
        Temporal.Child_workflow.execute ~id:"" target ())
  in
  (match Execution.activate (Execution.start empty_id ()) [ Activation.Start_workflow ] with
  | [ Activation.Fail_workflow error ] ->
      expect "empty child ID kind" "defect" (Temporal.Error.kind error);
      expect "empty child ID message" "child workflow id must not be empty"
        (Temporal.Error.message error)
  | _ -> failwith "empty child ID emitted a command or succeeded");
  let invalid_input =
    Temporal.Workflow.define ~name:"invalid_child_input"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        let target =
          Temporal.Workflow.remote ~name:"rejecting_child"
            ~input:rejecting_child_input ~output:Temporal.Codec.unit
        in
        Temporal.Child_workflow.execute ~id:"valid-id" target ())
  in
  (match
     Execution.activate (Execution.start invalid_input ()) [ Activation.Start_workflow ]
   with
  | [ Activation.Fail_workflow error ] ->
      expect "child input codec failure" "input rejected"
        (Temporal.Error.message error)
  | _ -> failwith "invalid child input emitted a command or succeeded");
  match
    Temporal.Child_workflow.start ~id:"detached" greeting_child "Ada"
    |> Temporal.Future.peek
  with
  | Some (Error error) ->
      expect "detached child kind" "defect" (Temporal.Error.kind error)
  | Some (Ok _) | None -> failwith "detached child did not fail immediately"

(** Proves ID validation occurs before input encoding, sequence allocation, and
    command emission. The first valid child still receives sequence one. *)
let test_child_workflow_id_boundary () =
  let execution = Execution.start child_id_boundary_workflow () in
  expect "invalid child IDs consume no sequence"
    [
      Activation.Start_child_workflow
        {
          seq = 1L;
          id = "after-invalid";
          name = "greeting_child";
          input = payload "Ada";
        };
    ]
    (Execution.activate execution [ Activation.Start_workflow ]);
  expect "valid child after ID failures completes"
    [ Activation.Complete_workflow (payload "Hello Ada") ]
    (Execution.activate execution
       [
         Activation.Resolve_child_workflow
           { seq = 1L; result = Ok (payload "Hello Ada") };
       ])

(** Remote child failures retain their typed error, while unknown or repeated
    sequence numbers become bridge defects and never resolve a future twice. *)
let test_child_workflow_failures_and_sequence_ownership () =
  let remote_failure = Execution.start child_parent_workflow "Ada" in
  ignore (Execution.activate remote_failure [ Activation.Start_workflow ]);
  let child_error = Temporal.Error.defect ~message:"child failed" in
  (match
     Execution.activate remote_failure
       [
         Activation.Resolve_child_workflow
           { seq = 1L; result = Error child_error };
       ]
   with
  | [ Activation.Fail_workflow error ] ->
      expect "remote child error" "child failed" (Temporal.Error.message error)
  | _ -> failwith "remote child failure was not propagated");
  let unknown = Execution.start child_parent_workflow "Ada" in
  ignore (Execution.activate unknown [ Activation.Start_workflow ]);
  (match
     Execution.activate unknown
       [
         Activation.Resolve_child_workflow
           { seq = 999L; result = Ok (payload "unknown") };
       ]
   with
  | [ Activation.Fail_workflow error ] ->
      expect "unknown child sequence" "bridge" (Temporal.Error.kind error)
  | _ -> failwith "unknown child sequence was accepted");
  let duplicate = Execution.start concurrent_child_parent () in
  ignore (Execution.activate duplicate [ Activation.Start_workflow ]);
  ignore
    (Execution.activate duplicate
       [
         Activation.Resolve_child_workflow
           { seq = 1L; result = Ok (payload "Hello Ada") };
       ]);
  match
    Execution.activate duplicate
      [
        Activation.Resolve_child_workflow
          { seq = 1L; result = Ok (payload "Hello again") };
      ]
  with
  | [ Activation.Fail_workflow error ] ->
      expect "duplicate child sequence" "bridge" (Temporal.Error.kind error)
  | _ -> failwith "duplicate child resolution was accepted"

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
  test_activity_options_and_queue ();
  test_invalid_activity_options_do_not_schedule ();
  test_replay_is_stable ();
  test_resolution_job_order ();
  test_bridge_defects ();
  test_zero_sleep_and_duration_validation ();
  test_start_sleep ();
  test_empty_all_uses_current_workflow_owner ();
  test_child_workflow_completion ();
  test_child_workflow_concurrency_and_decoding ();
  test_child_workflow_validation ();
  test_child_workflow_id_boundary ();
  test_child_workflow_failures_and_sequence_ownership ();
  test_cancel_and_evict ()
