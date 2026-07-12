(** Remote activity used to compile-check partially applied starter helpers. *)
let summarize =
  Temporal.Activity.remote ~name:"summarize" ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

(** Retains the opaque definition while [test_ordinary_helper_composition]
    creates a partially applied starter with the same name. *)
let summarize_definition = summarize

(** Remote child workflow with the same result type, suitable for homogeneous
    aggregation alongside [summarize]. *)
let review =
  Temporal.Workflow.remote ~name:"review" ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

(** Remote child workflow with a different result type, proving [race] keeps
    heterogeneous result types distinguishable through [Left] and [Right]. *)
let render =
  Temporal.Workflow.remote ~name:"render" ~input:Temporal.Codec.string
    ~output:Temporal.Codec.bytes

(** Tracks whether a user codec was invoked while an activity request was
    already known to be invalid. The counter makes validation ordering visible
    without requiring a running workflow scheduler or native backend. *)
let tracked_activity_encode_calls = ref 0

(** A codec with observable encoding lets the regression test distinguish
    option validation from conversion side effects. *)
let tracked_activity_codec =
  Temporal.Codec.make ~encoding:"test/tracked-activity"
    ~encode:(fun value ->
      incr tracked_activity_encode_calls;
      Ok (Bytes.of_string value))
    ~decode:(fun bytes -> Ok (Bytes.to_string bytes))

(** A remote activity using the tracked codec is sufficient because the
    invalid request must be rejected before a workflow context is consulted. *)
let tracked_activity =
  Temporal.Activity.remote ~name:"tracked_activity"
    ~input:tracked_activity_codec ~output:Temporal.Codec.string

(** Starts each ordinary function before aggregating its result. The helper is
    independent of Temporal definitions and can wrap activities, child
    workflows, or additional application helpers with the same future type. *)
let fan_out starters input =
  List.map (fun start -> start input) starters |> Temporal.Future.all

(** Races two ordinary starter functions without exposing effects or scheduler
    internals. The returned variant preserves the distinct output types. *)
let fastest left right input =
  Temporal.Future.race (left input) (right input)

(** Checks a future is already a typed defect. Public operations invoked by
    this unit test are intentionally detached from a workflow execution. *)
let expect_detached label future =
  match Temporal.Future.peek future with
  | Some (Error error) ->
      if not (String.equal (Temporal.Error.kind error) "defect") then
        failwith (label ^ " returned the wrong error category")
  | Some (Ok _) -> failwith (label ^ " unexpectedly succeeded")
  | None -> failwith (label ^ " unexpectedly remained pending")

(** Invalid activity options are rejected before a caller's input codec runs.
    This preserves deterministic workflow authoring: malformed command fields
    cannot trigger user conversion work or side effects before the typed
    defect is returned. *)
let test_activity_option_validation_precedes_encoding () =
  tracked_activity_encode_calls := 0;
  expect_detached "invalid activity queue"
    (Temporal.Activity.start ~task_queue:"" tracked_activity "document");
  if !tracked_activity_encode_calls <> 0 then
    failwith "invalid activity option invoked the input codec";
  let handle =
    Temporal.Activity.start_handle ~task_queue:"" tracked_activity "document"
  in
  expect_detached "invalid activity handle" (Temporal.Activity.future handle);
  match Temporal.Activity.cancel handle with
  | Error error when String.equal (Temporal.Error.kind error) "defect" -> ()
  | Error _ -> failwith "invalid activity handle returned the wrong error"
  | Ok () -> failwith "invalid activity handle cancellation unexpectedly succeeded"

(** Compile-checks that partial application and higher-order wrappers retain
    normal OCaml function composition for all and heterogeneous race. *)
let test_ordinary_helper_composition () =
  let summarize = Temporal.Activity.start summarize in
  let review = Temporal.Child_workflow.start ~id:"review-1" review in
  expect_detached "fan out" (fan_out [ summarize; review ] "document");
  let render = Temporal.Child_workflow.start ~id:"render-1" render in
  expect_detached "heterogeneous race" (fastest summarize render "document");
  let handle =
    Temporal.Activity.start_handle summarize_definition "document"
  in
  expect_detached "detached activity handle" (Temporal.Activity.future handle);
  match Temporal.Activity.cancel handle with
  | Error error when String.equal (Temporal.Error.kind error) "defect" -> ()
  | Error _ -> failwith "detached activity handle returned the wrong error"
  | Ok () -> failwith "detached activity handle cancellation unexpectedly succeeded"

(** Executes the standalone authoring and validation checks. *)
let () =
  test_ordinary_helper_composition ();
  test_activity_option_validation_precedes_encoding ()
