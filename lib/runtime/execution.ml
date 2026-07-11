(** The OCaml function stored in a local workflow definition. *)
type ('input, 'output) implementation =
  'input -> ('output, Temporal_base.Error.t) result

(** In-memory state for one workflow. Only the activation loop changes it.
    [terminal] prevents a second completion command, and [evicted] prevents use
    after Core asks the worker to remove the execution from its cache. *)
type ('input, 'output) t = {
  definition :
    ('input, 'output, ('input, 'output) implementation) Temporal_base.Definition.t;
  input : 'input;
  scheduler : Scheduler.t;
  context : Workflow_context_store.t;
  mutable started : bool;
  mutable terminal : bool;
  mutable evicted : bool;
}

(** Creates execution state without calling user workflow code. The code starts
    only after Temporal delivers a start job, matching replay behavior. *)
let start definition input =
  let scheduler = Scheduler.create () in
  {
    definition;
    input;
    scheduler;
    context = Workflow_context_store.create scheduler;
    started = false;
    terminal = false;
    evicted = false;
  }

(** Emits the workflow's completion, failure, or cancellation command once and
    immediately releases every paused fiber. *)
let emit_terminal execution command =
  if not execution.terminal then (
    execution.terminal <- true;
    Workflow_context_store.emit execution.context command;
    Workflow_context_store.shutdown execution.context)

(** Fails the workflow through the same one-terminal-command check. *)
let fail execution error =
  emit_terminal execution (Activation.Fail_workflow error)

(** Creates a non-retryable error when Core and the OCaml runtime disagree about
    activation state. This is an SDK/bridge failure, not an application error. *)
let bridge_error message =
  Temporal_base.Error.make ~non_retryable:true ~category:`Bridge ~message ()

(** Starts the workflow function exactly once. Its output is encoded inside the
    scheduled fiber so an exception or codec failure follows the normal
    workflow-failure path. *)
let start_workflow execution =
  if execution.started then
    fail execution (bridge_error "workflow received duplicate start job")
  else if not execution.terminal then begin
    execution.started <- true;
    Scheduler.spawn execution.scheduler (fun () ->
        match Temporal_base.Definition.implementation execution.definition with
        | None -> fail execution (bridge_error "remote workflow has no implementation")
        | Some implementation ->
            begin match implementation execution.input with
            | Error error -> fail execution error
            | Ok output ->
                begin match
                  Temporal_base.Codec.encode
                    (Temporal_base.Definition.output execution.definition)
                    output
                with
                | Error error -> fail execution error
                | Ok payload ->
                    emit_terminal execution (Activation.Complete_workflow payload)
                end
            end)
  end

(** Applies one activation job. Activity and timer sequence numbers locate the
    future created by the earlier command. Unknown or repeated numbers fail the
    workflow because ignoring them would make replay disagree with Core. *)
let process_job execution = function
  | Activation.Start_workflow -> start_workflow execution
  | Resolve_activity { seq; result } -> (
      match Workflow_context_store.resolve_activity execution.context ~seq result with
      | Ok () -> ()
      | Error error -> fail execution error)
  | Fire_timer { seq } -> (
      match Workflow_context_store.fire_timer execution.context ~seq with
      | Ok () -> ()
      | Error error -> fail execution error)
  | Cancel_workflow ->
      emit_terminal execution Activation.Cancel_workflow_execution
  | Remove_from_cache ->
      execution.evicted <- true;
      Workflow_context_store.shutdown execution.context

(** Runs queued fibers with this workflow installed as the current context. An
    uncaught OCaml exception becomes a non-retryable defect instead of escaping
    the worker loop. *)
let run_scheduler execution =
  match
    Workflow_context_store.with_context execution.context (fun () ->
        Scheduler.run execution.scheduler)
  with
  | Scheduler.Failed exception_ ->
      fail execution
        (Temporal_base.Error.defect ~message:(Printexc.to_string exception_))
  | Scheduler.Complete | Scheduler.Blocked -> ()

(** Processes jobs in order, runs fibers, and returns new commands. Cache
    removal stops processing immediately and returns no commands for the old
    in-memory execution. *)
let activate execution jobs =
  if execution.evicted then []
  else (
    List.iter
      (fun job -> if not execution.evicted then process_job execution job)
      jobs;
    if execution.evicted then []
    else (
      if not execution.terminal then run_scheduler execution;
      Workflow_context_store.take_commands execution.context))
