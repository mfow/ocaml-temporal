(** Remote activity used to compile-check partially applied starter helpers. *)
let summarize =
  Temporal.Activity.remote ~name:"summarize" ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

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

(** Compile-checks that partial application and higher-order wrappers retain
    normal OCaml function composition for all and heterogeneous race. *)
let test_ordinary_helper_composition () =
  let summarize = Temporal.Activity.start summarize in
  let review = Temporal.Child_workflow.start ~id:"review-1" review in
  expect_detached "fan out" (fan_out [ summarize; review ] "document");
  let render = Temporal.Child_workflow.start ~id:"render-1" render in
  expect_detached "heterogeneous race" (fastest summarize render "document")

(** Executes the standalone compile-and-behavior check. *)
let () = test_ordinary_helper_composition ()
