(** The OCaml function stored in a local workflow definition. *)
type ('input, 'output) implementation =
  'input -> ('output, Temporal_base.Error.t) result

(** Shared logging vocabulary. It deliberately contains reporter exceptions so
    workflow command and error semantics never depend on application logging. *)
module Observability = Temporal_base.Observability

(** The complete incoming event made available to a native signal handler. The
    activation translator has already validated and copied every payload. *)
type signal = {
  input : Temporal_base.Codec.payload list;
  identity : string;
  headers : (string * Temporal_base.Codec.payload) list;
}

(** A handler is kept private to the runtime so public callers cannot observe
    scheduler callbacks, continuations, or native boundary values. *)
type signal_handler = {
  name : string;
  dispatch : signal -> (unit, Temporal_base.Error.t) result;
}

(** Uses the same bounded identifier contract as workflow and activity names.
    Invalid names are internal registration defects because public signal
    definitions have already performed this validation. *)
let validate_signal_name name =
  if String.equal name "" then invalid_arg "signal handler name is empty";
  if String.contains name '\000' then
    invalid_arg "signal handler name contains NUL";
  if String.length name > 65_536 then
    invalid_arg "signal handler name exceeds 65536 bytes";
  if not (Temporal_base.Codec.valid_utf_8 name) then
    invalid_arg "signal handler name must be valid UTF-8"

(** Builds one scheduler-owned callback package. *)
let make_signal_handler ~name ~dispatch =
  validate_signal_name name;
  { name; dispatch }

(** Returns the stable lookup key of an internal handler. *)
let signal_handler_name handler = handler.name

(** Signal names are resolved through an immutable map. Construction validates
    duplicates in caller order; execution lookup never depends on hash-table
    iteration or mutable global state. *)
module Signal_map = Map.Make (String)

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
  signal_handlers : signal_handler Signal_map.t;
  mutable started : bool;
  mutable terminal : bool;
  mutable evicted : bool;
}

(** Creates execution state without calling user workflow code. The code starts
    only after Temporal delivers a start job, matching replay behavior. *)
let start ?(task_queue = "default") ?(signal_handlers = []) definition input =
  let signal_handlers =
    List.fold_left
      (fun handlers handler ->
        let name = signal_handler_name handler in
        if Signal_map.mem name handlers then
          invalid_arg ("duplicate signal handler: " ^ name)
        else Signal_map.add name handler handlers)
      Signal_map.empty signal_handlers
  in
  let scheduler = Scheduler.create () in
  let execution =
    {
      definition;
      input;
      scheduler;
      context = Workflow_context_store.create ~task_queue scheduler;
      signal_handlers;
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

(** Updates the reusable execution context with the current activation clock.
    Keeping this setter behind the execution abstraction prevents native code
    from reaching into the context record or bypassing its lifecycle rules. *)
let set_activation_timestamp execution timestamp =
  Workflow_context_store.set_activation_timestamp execution.context timestamp

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
    | Schedule_activity _ | Start_child_workflow _ | Request_cancel_activity _
    | Cancel_child_workflow _ | Start_timer _ | Cancel_timer _
    | Continue_as_new _ -> ())

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
            begin match
              try implementation execution.input with
              | exn ->
                  Error
                    (Temporal_base.Error.defect
                       ~message:
                         ("workflow implementation raised: "
                         ^ Printexc.to_string exn))
            with
            | Error error -> fail execution error
            | Ok output ->
                begin match
                  try
                    Temporal_base.Codec.encode
                      (Temporal_base.Definition.output execution.definition)
                      output
                  with
                  | exn ->
                      Error
                        (Temporal_base.Error.defect
                           ~message:
                             ("workflow result encoder raised: "
                             ^ Printexc.to_string exn))
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
  | Resolve_child_workflow_start { seq; result } -> (
      match
        Workflow_context_store.resolve_child_workflow_start execution.context
          ~seq result
      with
      | Ok () -> ()
      | Error error -> fail execution error)
  | Resolve_child_workflow { seq; result } -> (
      match
        Workflow_context_store.resolve_child_workflow execution.context ~seq result
      with
      | Ok () -> ()
      | Error error -> fail execution error)
  | Activation.Signal_workflow { signal_name; input; identity; headers } ->
      (* Signals are queued as scheduler work rather than dispatched inline.
         This preserves FIFO ordering with root and resolver continuations and
         gives a handler the same direct-style suspension semantics as the
         workflow body. *)
      let signal = { input; identity; headers } in
      begin match Signal_map.find_opt signal_name execution.signal_handlers with
      | None ->
          let tags =
            Observability.tags ~operation:"workflow_signal_unhandled"
              ~workflow_type:(workflow_type execution) ()
          in
          report ~src:Observability.Source.workflow Logs.Error ~tags
            "workflow signal has no registered handler";
          fail execution
            (Temporal_base.Error.make ~non_retryable:true ~category:`Workflow
               ~message:("unhandled workflow signal: " ^ signal_name) ())
      | Some handler ->
          let tags =
            Observability.tags ~operation:"workflow_signal_received"
              ~workflow_type:(workflow_type execution) ()
          in
          report ~src:Observability.Source.workflow Logs.Debug ~tags
            "workflow signal queued for its registered handler";
          Scheduler.spawn execution.scheduler (fun () ->
              match handler.dispatch signal with
              | Ok () ->
                  let tags =
                    Observability.tags ~operation:"workflow_signal_handled"
                      ~workflow_type:(workflow_type execution) ()
                  in
                  report ~src:Observability.Source.workflow Logs.Debug ~tags
                    "workflow signal handler completed"
              | Error error -> fail execution error)
      end
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

(** Releases every paused fiber and pending operation table for this execution.
    Safe to call more than once: the context and scheduler ignore a second
    shutdown after they have already become inactive. Adapters must call this
    whenever a run is removed from the registry for a path that did not already
    emit a terminal or eviction command, so one-shot effect continuations cannot
    leak after a rejected activation. *)
let shutdown execution =
  (* Teardown must not raise into lease-ack bookkeeping. Continuations that
     mishandle the private shutdown exception are contained here. *)
  try Workflow_context_store.shutdown execution.context with _ -> ()

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
            (fun job ->
              if (not execution.evicted) && not execution.terminal then
                process_job execution job)
            jobs;
          if execution.evicted then []
          else (
            if not execution.terminal then run_scheduler execution;
            let commands = Workflow_context_store.take_commands execution.context in
            (* Terminal commands emitted through [terminate] (continue-as-new or
               a Fail_workflow from a failed continue-as-new encode) do not go
               through [emit_terminal], so finalize here after the scheduler
               has stopped. Any terminal command seals the run so a later
               activation cannot append a second terminal. *)
            if
              List.exists
                (function
                  | Activation.Complete_workflow _
                  | Fail_workflow _
                  | Cancel_workflow_execution
                  | Continue_as_new _ -> true
                  | _ -> false)
                commands
            then (
              execution.terminal <- true;
              Workflow_context_store.shutdown execution.context);
            commands)))
  in
  let tags =
    Observability.tags ~operation:"activate" ~duration_ms
      ~workflow_type:(workflow_type execution) ~job_count
      ~command_count:(List.length commands) ()
  in
  report ~src:Observability.Source.workflow Logs.Debug ~tags
    "workflow activation processed";
  commands
