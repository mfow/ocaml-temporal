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

(** Starts a durable timer without waiting for it. The active workflow context
    owns both the future and its history command; zero duration is represented
    by a context-owned ready future without adding a meaningless command. *)
let start_sleep duration =
  match Temporal_runtime.Workflow_context_store.current () with
  | None ->
      Temporal_runtime.Workflow_context_store.detached_error
        ~message:"workflow sleep used outside a workflow execution"
  | Some context ->
      let milliseconds = Duration.to_ms duration in
      if milliseconds = 0L then
        Temporal_runtime.Workflow_context_store.resolved context (Ok ())
      else
        Temporal_runtime.Workflow_context_store.start_timer context milliseconds

(** Implements direct-style durable sleep as timer creation followed by a
    future wait, so helpers may choose either blocking or start-now behavior. *)
let sleep duration = Future.await (start_sleep duration)
