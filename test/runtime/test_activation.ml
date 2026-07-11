module Activation = Temporal_runtime.Activation
module Execution = Temporal_runtime.Execution

let payload value =
  match Temporal.Codec.encode Temporal.Codec.string value with
  | Ok payload -> payload
  | Error error -> failwith (Temporal.Error.message error)

let greeting =
  Temporal.Activity.remote ~name:"greeting" ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

let workflow input =
  let open Temporal.Result_syntax in
  assert (Temporal.Workflow_context.is_active ());
  let pending = Temporal.Activity.start greeting input in
  let* greeting = Temporal.Future.await pending in
  let* () = Temporal.Workflow.sleep (Temporal.Duration.of_ms 10L) in
  Ok (greeting ^ "!")

let greeting_workflow =
  Temporal.Workflow.define ~name:"greeting_workflow"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string workflow

let expect label expected actual =
  if expected <> actual then failwith (label ^ " did not match")

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

let test_replay_is_stable () =
  expect "replay command bytes" (run_greeting ()) (run_greeting ())

let ordering_activity name =
  Temporal.Activity.remote ~name ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

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

let resolve_order jobs expected =
  let execution = Execution.start ordered_workflow () in
  ignore (Execution.activate execution [ Activation.Start_workflow ]);
  expect "explicit resolution order"
    [ Activation.Complete_workflow (payload expected) ]
    (Execution.activate execution jobs)

let test_resolution_job_order () =
  let first =
    Activation.Resolve_activity { seq = 1L; result = Ok (payload "first") }
  in
  let second =
    Activation.Resolve_activity { seq = 2L; result = Ok (payload "second") }
  in
  resolve_order [ second; first ] "second,first";
  resolve_order [ first; second ] "first,second"

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

let zero_sleep_workflow =
  Temporal.Workflow.define ~name:"zero_sleep" ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.unit (fun () ->
      match Temporal.Workflow.sleep (Temporal.Duration.of_ms 0L) with
      | Ok () -> Ok ()
      | Error error -> Error error)

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
  test_cancel_and_evict ()
