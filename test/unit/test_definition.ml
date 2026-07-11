let greeting =
  Temporal.Activity.remote ~name:"greeting" ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

let workflow input = Ok (input ^ "!")

let greeting_workflow =
  Temporal.Workflow.define ~name:"greeting_workflow"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string workflow

let expect_invalid_name make =
  match make () with
  | exception Invalid_argument _ -> ()
  | _ -> failwith "invalid definition name accepted"

let () =
  assert (Temporal.Activity.name greeting = "greeting");
  assert (Temporal.Workflow.name greeting_workflow = "greeting_workflow");
  expect_invalid_name (fun () ->
      Temporal.Activity.remote ~name:"" ~input:Temporal.Codec.unit
        ~output:Temporal.Codec.unit);
  expect_invalid_name (fun () ->
      Temporal.Workflow.remote ~name:"bad\000name" ~input:Temporal.Codec.unit
        ~output:Temporal.Codec.unit);
  assert (not (Temporal.Workflow_context.is_active ()));
  (match Temporal.Future.await (Temporal.Activity.start greeting "Ada") with
  | Error error -> assert (Temporal.Error.kind error = "defect")
  | Ok _ -> failwith "activity started outside a workflow");
  match Temporal.Workflow.sleep (Temporal.Duration.of_ms 1L) with
  | Error error -> assert (Temporal.Error.kind error = "defect")
  | Ok () -> failwith "workflow slept outside an execution"
