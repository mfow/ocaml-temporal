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

let outside_error () =
  Error.defect ~message:"activity operation used outside a workflow"

let resolved result =
  match Temporal_runtime.Workflow_context_store.current () with
  | Some context -> Temporal_runtime.Workflow_context_store.resolved context result
  | None -> Temporal_runtime.Future_store.resolved ~outside_error result

let start definition input =
  match Codec.encode (Temporal_base.Definition.input definition) input with
  | Error error -> resolved (Error error)
  | Ok input -> (
      match Temporal_runtime.Workflow_context_store.current () with
      | None ->
          Temporal_runtime.Workflow_context_store.detached_error
            ~message:"activity operation used outside a workflow"
      | Some context ->
          Temporal_runtime.Workflow_context_store.schedule_activity context
            ~name:(name definition) ~input
            ~decode:(Codec.decode (Temporal_base.Definition.output definition)))

let execute definition input = Future.await (start definition input)
