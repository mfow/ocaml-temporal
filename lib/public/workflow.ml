type ('input, 'output) implementation =
  'input -> ('output, Error.t) result

type ('input, 'output) t =
  ( 'input,
    'output,
    ('input, 'output) implementation )
  Temporal_base.Definition.t

let define ~name ~input ~output implementation =
  Temporal_base.Definition.make ~name ~input ~output
    ~implementation:(Some implementation)

let remote ~name ~input ~output =
  Temporal_base.Definition.make ~name ~input ~output ~implementation:None

let name = Temporal_base.Definition.name

let sleep duration =
  match Temporal_runtime.Workflow_context_store.current () with
  | None ->
      Error
        (Error.defect ~message:"workflow sleep used outside a workflow execution")
  | Some context ->
      let milliseconds = Duration.to_ms duration in
      if milliseconds = 0L then Ok ()
      else
        Temporal_runtime.Workflow_context_store.start_timer context milliseconds
        |> Future.await
