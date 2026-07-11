(** The OCaml function stored in a local workflow definition. *)
type ('input, 'output) implementation =
  'input -> ('output, Temporal_base.Error.t) result

(** Shared logging vocabulary. It deliberately contains reporter exceptions so
    workflow command and error semantics never depend on application logging. *)
module Observability = Temporal_base.Observability

(** Reports with workflow context masked so an application reporter cannot
    re-enter workflow APIs and mutate the deterministic command buffer. *)
let report ~src level ~tags message =
  Workflow_context_store.without_context (fun () ->
      Observability.report ~src level ~tags message)

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
  let execution =
    {
      definition;
      input;
      scheduler;
      context = Workflow_context_store.create scheduler;
      started = false;
      terminal = false;
      evicted = false;
    }
  in
  let tags =
    Observability.tags ~operation:"execution_created"
      ~workflow_type:(Temporal_base.Definition.name definition) ()
  in
  report ~src:Observability.Source.workflow Logs.Debug ~tags
    "workflow execution state created";
  execution

(** Returns the registered type used for bounded workflow log metadata. *)
let workflow_type execution =
  Temporal_base.Definition.name execution.definition

(** Emits the workflow's completion, failure, or cancellation command once and
    immediately releases every paused fiber. *)
let emit_terminal execution command =
  if not execution.terminal then (
    execution.terminal <- true;
    Workflow_context_store.emit execution.context command;
    Workflow_context_store.shutdown execution.context;
    match command with
    | Activation.Complete_workflow _ ->
        let tags =
          Observability.tags ~operation:"workflow_completed"
            ~workflow_type:(workflow_type execution) ()
        in
        report ~src:Observability.Source.workflow Logs.Info ~tags
          "workflow completed"
    | Fail_workflow _ | Cancel_workflow_execution
    | Schedule_activity _ | Request_cancel_activity _ | Start_timer _
    | Cancel_timer _ -> ())

(** Fails the workflow through the same one-terminal-command check. *)
let fail execution error =
  if not execution.terminal then (
    let tags =
      Observability.tags ~operation:"workflow_failed"
        ~workflow_type:(workflow_type execution)
        ~error_kind:(Temporal_base.Error.kind error) ()
    in
    report ~src:Observability.Source.workflow Logs.Error ~tags
      "workflow failed");
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
    let tags =
      Observability.tags ~operation:"workflow_started"
        ~workflow_type:(workflow_type execution) ()
    in
    report ~src:Observability.Source.workflow Logs.Info ~tags
      "workflow started";
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
      if not execution.terminal then (
        let tags =
          Observability.tags ~operation:"workflow_cancelled"
            ~workflow_type:(workflow_type execution) ()
        in
        report ~src:Observability.Source.workflow Logs.Info ~tags
          "workflow cancellation requested");
      emit_terminal execution Activation.Cancel_workflow_execution
  | Remove_from_cache ->
      execution.evicted <- true;
      let tags =
        Observability.tags ~operation:"execution_evicted"
          ~workflow_type:(workflow_type execution) ()
      in
      report ~src:Observability.Source.workflow Logs.Debug ~tags
        "workflow execution evicted";
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
  let job_count = List.length jobs in
  let commands, duration_ms =
    Observability.measure_ms (fun () ->
        if execution.evicted then (
          let tags =
            Observability.tags ~operation:"activation_ignored"
              ~workflow_type:(workflow_type execution) ~job_count ()
          in
          report ~src:Observability.Source.workflow Logs.Warning
            ~tags "activation ignored after cache eviction";
          [])
        else (
          List.iter
            (fun job -> if not execution.evicted then process_job execution job)
            jobs;
          if execution.evicted then []
          else (
            if not execution.terminal then run_scheduler execution;
            Workflow_context_store.take_commands execution.context)))
  in
  let tags =
    Observability.tags ~operation:"activate" ~duration_ms
      ~workflow_type:(workflow_type execution) ~job_count
      ~command_count:(List.length commands) ()
  in
  report ~src:Observability.Source.workflow Logs.Debug ~tags
    "workflow activation processed";
  commands
