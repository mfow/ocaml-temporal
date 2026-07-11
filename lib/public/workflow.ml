(** Workflow definitions share the private typed representation with activity
    definitions while retaining a distinct public type constructor. *)
type ('input, 'output) implementation =
  'input -> ('output, Error.t) result

type ('input, 'output) t =
  ( 'input,
    'output,
    ('input, 'output) implementation )
  Temporal_base.Definition.t

(** Registers executable workflow code for the local worker. *)
let define ~name ~input ~output implementation =
  Temporal_base.Definition.make ~name ~input ~output
    ~implementation:(Some implementation)

(** Declares a typed workflow target without local implementation code. *)
let remote ~name ~input ~output =
  Temporal_base.Definition.make ~name ~input ~output ~implementation:None

let name = Temporal_base.Definition.name

(** Implements durable sleep through the active deterministic execution. The
    zero case avoids adding a meaningless timer command to history. *)
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
