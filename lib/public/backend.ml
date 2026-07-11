(** Implements the private deterministic backend seam used by unit tests.

    The [mock://] transport is intentionally tiny: it exercises public API
    ownership and lifecycle behavior while the native adapter is developed.
    It is not a production URL contract. The native Rust/Core adapter will use
    separate activation/completion semantic types and explicit admission and
    finalization operations rather than implementing this mock task protocol.
    Every value crossing this module is copied before it is retained, keeping
    ownership explicit at the future FFI boundary. *)

(** Connection settings copied into each backend graph. *)
type config = {
  target_url : string;
  namespace : string;
  identity : string;
  task_queue : string option;
}

(** Client start input after public codec encoding. *)
type start_request = {
  workflow_name : string;
  workflow_id : string;
  task_queue : string;
  input : Payload.t;
}

(** Server-issued workflow identity. *)
type start_response = {
  workflow_id : string;
  run_id : string;
}

(** Exact-run wait selector. *)
type wait_request = {
  workflow_id : string;
  run_id : string;
}

(** Terminal workflow outcome represented independently from transport errors. *)
type terminal_result =
  | Completed of Payload.t
  | Failed of Error.t
  | Cancelled of Error.t
  | Terminated of Error.t
  | Timed_out of Error.t
  | Continued_as_new of {
      workflow_id : string;
      run_id : string;
    }

(** Synthetic workflow task delivered by the deterministic unit-test seam. *)
type workflow_task = {
  task_token : string;
  workflow_name : string;
  input : Payload.t;
}

(** Synthetic activity task delivered by the deterministic unit-test seam. *)
type activity_task = {
  task_token : string;
  activity_name : string;
  input : Payload.t;
}

(** Poll state shared by workflow and activity streams. *)
type 'task poll_result =
  | Task of 'task
  | Idle
  | Shutdown

(** Synthetic workflow completion used by the deterministic unit-test seam. *)
type workflow_completion =
  | Workflow_completed of {
      task_token : string;
      output : Payload.t;
    }
  | Workflow_failed of {
      task_token : string;
      error : Error.t;
    }

(** Synthetic activity completion used by the deterministic unit-test seam. *)
type activity_completion =
  | Activity_completed of {
      task_token : string;
      output : Payload.t;
    }
  | Activity_failed of {
      task_token : string;
      error : Error.t;
    }

(** A completed mock execution is retained so a later exact wait has stable
    semantics even when callers await the same handle more than once. *)
type mock_execution = {
  run_id : string;
  input : Payload.t;
}

(** A client graph has one mutable lifecycle bit and an exact-run ledger. *)
type mock_client = {
  _namespace : string;
  mutable closed : bool;
  mutable next_run : int;
  executions : (string, mock_execution) Hashtbl.t;
}

(** The private client representation leaves room for the native adapter while
    keeping tests deterministic today. *)
type client = Mock_client of mock_client

(** Poll streams have independent queues because Core forbids overlapping polls
    of the same kind but permits workflow and activity streams together. *)
type mock_worker = {
  _namespace : string;
  _task_queue : string;
  mutable closed : bool;
  workflow_tasks : workflow_task Queue.t;
  activity_tasks : activity_task Queue.t;
  outstanding_workflows : (string, unit) Hashtbl.t;
  outstanding_activities : (string, unit) Hashtbl.t;
  mutable idle_workflow_polls : int;
  mutable idle_activity_polls : int;
}

(** The private worker representation leaves room for the supervisor adapter. *)
type worker = Mock_worker of mock_worker

(** Copies payload bytes before retaining them in a backend-owned ledger. *)
let copy_payload (payload : Payload.t) : Payload.t =
  { payload with data = Bytes.copy payload.data }

(** Creates a structured bridge error without exposing a backend exception. *)
let bridge_error message = Temporal_base.Error.make ~category:`Bridge ~message ()

(** Creates a structured defect for malformed configuration or impossible local
    state transitions. *)
let defect message = Error.defect ~message

(** Recognizes only URLs accepted by this boundary. The mock scheme remains
    private to unit tests; normal callers must use HTTP or HTTPS. *)
let valid_target_url url =
  String.starts_with ~prefix:"http://" url
  || String.starts_with ~prefix:"https://" url
  || String.starts_with ~prefix:"mock://" url

(** Rejects empty names before they enter backend state. *)
let valid_nonempty field value =
  if String.equal value "" then
    Error (defect (field ^ " must not be empty"))
  else if String.contains value '\000' then
    Error (defect (field ^ " must not contain NUL"))
  else Ok ()

(** Validates shared connection settings once at the private boundary. *)
let validate_config { target_url; namespace; identity; task_queue } =
  if not (valid_target_url target_url) then
    Error (defect "target_url must use http, https, or the private mock scheme")
  else
    match valid_nonempty "namespace" namespace with
    | Error _ as error -> error
    | Ok () -> (
        match valid_nonempty "identity" identity with
        | Error _ as error -> error
        | Ok () -> (
            match task_queue with
            | None -> Ok ()
            | Some task_queue -> valid_nonempty "task queue" task_queue))

(** Constructs an empty client ledger after configuration validation. *)
let client_create config =
  match validate_config config with
  | Error error -> Error error
  | Ok () ->
      if String.starts_with ~prefix:"mock://" config.target_url then
        Ok
          (Mock_client
             {
               _namespace = config.namespace;
               closed = false;
               next_run = 0;
               executions = Hashtbl.create 16;
             })
      else
        Error
          (bridge_error
             "native client adapter is not connected in this build")

(** Starts a mock execution and preserves the exact request payload. *)
let client_start (Mock_client client) (request : start_request) =
  if client.closed then Error (bridge_error "client is shut down")
  else if Hashtbl.mem client.executions request.workflow_id then
    Error
      (Temporal_base.Error.make ~category:`Workflow
         ~message:"workflow id already exists" ())
  else
    let run_id =
      client.next_run <- client.next_run + 1;
      Printf.sprintf "mock-run-%d" client.next_run
    in
    Hashtbl.add client.executions request.workflow_id
      { run_id; input = copy_payload request.input };
    let response : start_response = { workflow_id = request.workflow_id; run_id } in
    Ok response

(** Waits for a mock execution and echoes its input as the completed output.
    Echoing is sufficient to test typed output decoding without coupling this
    private transport to any application workflow implementation. *)
let client_wait (Mock_client client) request =
  if client.closed then Error (bridge_error "client is shut down")
  else
    match Hashtbl.find_opt client.executions request.workflow_id with
    | None -> Error (bridge_error "workflow execution was not started")
    | Some execution when not (String.equal execution.run_id request.run_id) ->
        Error (bridge_error "workflow run id does not match the started run")
    | Some execution -> Ok (Completed (copy_payload execution.input))

(** Marks a mock client closed while preserving idempotent cleanup. *)
let client_shutdown (Mock_client client) =
  client.closed <- true;
  Ok ()

(** The canonical empty payload used by the deterministic mock worker tasks. *)
let unit_payload =
  { Payload.metadata = [ ("encoding", "binary/null") ]; data = Bytes.empty }

(** Registers one mock task per local definition. The input is [unit] so tests
    can observe dispatch without adding a test-only payload protocol. *)
let worker_create config ~workflow_names ~activity_names =
  match validate_config config with
  | Error error -> Error error
  | Ok () ->
      if config.task_queue = None then
        Error (defect "worker task queue is required")
      else if not (String.starts_with ~prefix:"mock://" config.target_url) then
        Error
          (bridge_error
             "native worker adapter is not connected in this build")
      else
        let workflow_tasks = Queue.create () in
        let activity_tasks = Queue.create () in
        List.iter
          (fun name ->
            Queue.add
              {
                task_token = "mock-workflow-" ^ name;
                workflow_name = name;
                input = copy_payload unit_payload;
              }
              workflow_tasks)
          workflow_names;
        List.iter
          (fun name ->
            Queue.add
              {
                task_token = "mock-activity-" ^ name;
                activity_name = name;
                input = copy_payload unit_payload;
              }
              activity_tasks)
          activity_names;
        Ok
          (Mock_worker
             {
               _namespace = config.namespace;
               _task_queue = Option.get config.task_queue;
               closed = false;
               workflow_tasks;
               activity_tasks;
               outstanding_workflows = Hashtbl.create 16;
               outstanding_activities = Hashtbl.create 16;
               idle_workflow_polls = 0;
               idle_activity_polls = 0;
             })

(** Polls the workflow queue and records ownership before exposing a task. *)
let worker_poll_workflow (Mock_worker worker) =
  if worker.closed then Ok Shutdown
  else if Queue.is_empty worker.workflow_tasks then (
    worker.idle_workflow_polls <- worker.idle_workflow_polls + 1;
    Ok Shutdown)
  else
    let task = Queue.take worker.workflow_tasks in
    Hashtbl.replace worker.outstanding_workflows task.task_token ();
    Ok (Task task)

(** Polls the activity queue and records ownership before exposing a task. *)
let worker_poll_activity (Mock_worker worker) =
  if worker.closed then Ok Shutdown
  else if Queue.is_empty worker.activity_tasks then (
    worker.idle_activity_polls <- worker.idle_activity_polls + 1;
    Ok Shutdown)
  else
    let task = Queue.take worker.activity_tasks in
    Hashtbl.replace worker.outstanding_activities task.task_token ();
    Ok (Task task)

(** Removes an outstanding workflow task exactly once. *)
let require_workflow_token worker token =
  if Hashtbl.mem worker.outstanding_workflows token then (
    Hashtbl.remove worker.outstanding_workflows token;
    Ok ())
  else Error (bridge_error "workflow completion token is unknown or reused")

(** Removes an outstanding activity task exactly once. *)
let require_activity_token worker token =
  if Hashtbl.mem worker.outstanding_activities token then (
    Hashtbl.remove worker.outstanding_activities token;
    Ok ())
  else Error (bridge_error "activity completion token is unknown or reused")

(** Completes a workflow task after checking its one-shot token lease. *)
let worker_complete_workflow (Mock_worker worker) completion =
  if worker.closed then Error (bridge_error "worker is shut down")
  else
    let token =
      match completion with
      | Workflow_completed { task_token; _ }
      | Workflow_failed { task_token; _ } -> task_token
    in
    require_workflow_token worker token

(** Completes an activity task after checking its one-shot token lease. *)
let worker_complete_activity (Mock_worker worker) completion =
  if worker.closed then Error (bridge_error "worker is shut down")
  else
    let token =
      match completion with
      | Activity_completed { task_token; _ }
      | Activity_failed { task_token; _ } -> task_token
    in
    require_activity_token worker token

(** Closes worker admission; Core-backed implementations will first drain
    pollers and outstanding leases before returning from this function. *)
let worker_shutdown (Mock_worker worker) =
  worker.closed <- true;
  Ok ()
