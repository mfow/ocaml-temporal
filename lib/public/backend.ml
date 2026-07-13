(** Implements the private transport selected by the public client and worker.

    [mock://] remains a deterministic in-memory seam for unit tests. HTTP(S)
    targets use the private supervisor, which owns the Rust runtime and
    serializes every native call on one owner Domain. Keeping this routing in a
    private module lets the public API stay independent of JSON, Rust handles,
    and lifecycle implementation details. Every payload is copied when it is
    converted between the public and protocol representations. *)

module Bridge = Temporal_core_bridge.Native_bridge
module Native = Sdk_supervisor.Native
module Client_protocol = Temporal_protocol.Client_protocol
module Workflow_protocol = Temporal_protocol.Workflow_protocol
module Failure_diagnostic = Temporal_protocol.Failure_diagnostic

(** Connection settings copied into each backend graph. *)
type config = {
  target_url : string;
  namespace : string;
  identity : string;
  task_queue : string option;
}

(** Client start input after public codec encoding. *)
type start_request = {
  (* Optional caller-owned Temporal request ID. [None] asks the native
     adapter to allocate one fresh ID for this logical start call; [Some id]
     lets a caller retry an uncertain result with the same idempotency key. *)
  request_id : string option;
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

(** Exact workflow/run pair and stable control-operation metadata. *)
type cancel_request = {
  workflow_id : string;
  run_id : string;
  request_id : string;
  reason : string;
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

(** Terminal state for one mock execution. The state is monotone: once a wait
    observes completion, a later cancellation cannot rewrite that terminal
    result, which mirrors Temporal's immutable execution history. *)
type mock_terminal = Mock_pending | Mock_completed | Mock_cancelled

(** A mock execution is retained so repeated exact waits return the same
    terminal result and cancellation can only affect work still pending. *)
type mock_execution = {
  run_id : string;
  input : Payload.t;
  mutable terminal : mock_terminal;
}

(** A client graph has one mutable lifecycle bit and an exact-run ledger.
    The mutex covers both fields because unit tests may exercise ordinary
    client calls from more than one Domain even though the native supervisor
    itself already serializes its own graph. *)
type mock_client = {
  _namespace : string;
  mutable closed : bool;
  mutable next_run : int;
  mutex : Mutex.t;
  executions : (string, mock_execution) Hashtbl.t;
}

(** Native client state retained by the private backend.

    [supervisor] is the sole owner of the Rust runtime, connected client, and
    any asynchronous start tickets. [next_request_id] is language-side state
    only; its atomic increment gives concurrent ordinary client callers unique
    logical request IDs without adding a second lock around native state. *)
type native_client = {
  namespace : string;
  supervisor : Native.t;
  next_request_id : int Atomic.t;
  closed : bool Atomic.t;
}

(** The private client representation selects the deterministic mock or the
    real Rust/Core adapter from the endpoint scheme. *)
type client = Mock_client of mock_client | Native_client of native_client

(** Poll streams have independent queues because Core forbids overlapping polls
    of the same kind but permits workflow and activity streams together. *)
type mock_worker = {
  _namespace : string;
  _task_queue : string;
  mutable closed : bool;
  (** Protects queues, outstanding tables, and idle counters. Unit tests may
      exercise the mock worker from more than one Domain. *)
  mutex : Mutex.t;
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
let bridge_error message = Error.make ~category:`Bridge ~message ()

(** Creates a structured defect for malformed configuration or impossible local
    state transitions. *)
let defect message = Error.defect ~message

(** Converts a native status to a stable diagnostic label without depending on
    the private logging implementation in [Native_bridge]. Unknown statuses
    remain visible, which makes an ABI mismatch diagnosable without exposing a
    Rust-owned error buffer. *)
let native_status_name = function
  | Bridge.Invalid_argument -> "invalid_argument"
  | Bridge.Abi_mismatch -> "abi_mismatch"
  | Bridge.Panic -> "panic"
  | Bridge.Internal -> "internal"
  | Bridge.Invalid_state -> "invalid_state"
  | Bridge.Configuration -> "configuration"
  | Bridge.Connection -> "connection"
  | Bridge.Worker -> "worker"
  | Bridge.Outstanding_tasks -> "outstanding_tasks"
  | Bridge.Not_ready -> "not_ready"
  | Bridge.Protocol -> "protocol"
  | Bridge.Already_started -> "already_started"
  | Bridge.Retryable -> "retryable"
  | Bridge.Unknown code -> Printf.sprintf "unknown(%d)" code

(** Converts a supervisor failure to the public bridge/defect vocabulary.
    Backend errors are expected operational failures; a supervisor exception
    is an internal invariant failure and is therefore deliberately marked
    non-retryable. Worker statuses are normalized here as a final defense
    against Core/gRPC diagnostic prose escaping through a client callsite. *)
let native_supervisor_error = function
  | Native.Backend { Bridge.status; message } ->
      let message =
        match status with
        | Bridge.Worker -> "native worker operation failed"
        | Bridge.Outstanding_tasks -> "native worker has outstanding tasks"
        | _ -> message
      in
      bridge_error
        (Printf.sprintf "native client bridge %s: %s" (native_status_name status)
           message)
  | Native.Closed -> bridge_error "native client supervisor is shut down"
  | Native.Supervisor_failed exception_ ->
      Error.defect
        ~message:
          (Printf.sprintf "native client supervisor failed: %s"
             (Printexc.to_string exception_))

(** Converts a structured native client operation failure while preserving its
    semantic distinction from a transport/lifecycle error. Temporal's
    duplicate-workflow response is a workflow failure; RPC and protocol
    failures remain bridge failures because no terminal workflow result exists. *)
let native_client_error = function
  | Client_protocol.Already_started { workflow_id; existing_run_id } ->
      let existing_run_id =
        match existing_run_id with
        | None -> ""
        | Some run_id -> "; existing_run_id=" ^ run_id
      in
      Error.make ~non_retryable:true ~category:`Workflow
        ~message:
          (Printf.sprintf "workflow %S is already started%s" workflow_id
             existing_run_id)
        ()
  | Client_protocol.Rpc { code } ->
      bridge_error ("Temporal client RPC failed: " ^ code)
  | Client_protocol.Protocol { code } ->
      bridge_error ("Temporal client protocol rejected the response: " ^ code)

(** Copies one public payload into the binary-safe protocol representation.
    Metadata values are strings in the public codec API but bytes in the closed
    JSON protocol; [Bytes.of_string] preserves every byte without assuming
    UTF-8. *)
let protocol_payload (payload : Payload.t) : Client_protocol.payload =
  {
    Workflow_protocol.metadata =
      List.map
        (fun (key, value) -> (key, Bytes.of_string value))
        payload.metadata;
    data = Bytes.copy payload.data;
  }

(** Copies one protocol payload into the public representation. OCaml strings
    are byte strings, so converting metadata with [Bytes.to_string] is
    lossless even for a future binary metadata value. *)
let public_payload (payload : Client_protocol.payload) : Payload.t =
  {
    Payload.metadata =
      List.map
        (fun (key, value) -> (key, Bytes.to_string value))
        payload.metadata;
    data = Bytes.copy payload.data;
  }

(** Collects all application/cancellation detail payloads through a bounded
    failure cause chain. Protocol decoding already applies a depth limit; this
    second guard also protects callers that construct protocol values directly. *)
let failure_details failure =
  let rec loop depth reversed (value : Workflow_protocol.failure) =
    let reversed =
      match value.info with
      | Workflow_protocol.Application { details; _ }
      | Workflow_protocol.Canceled { details; _ } ->
          List.rev_append details reversed
      | Workflow_protocol.Activity _ | Workflow_protocol.Child_workflow _ ->
          reversed
      | Workflow_protocol.Timeout_failure { last_heartbeat_details; _ } ->
          List.rev_append last_heartbeat_details reversed
    in
    match value.cause with
    | Some cause when depth < 128 -> loop (depth + 1) reversed cause
    | None | Some _ -> List.rev reversed
  in
  loop 0 [] failure |> List.map public_payload

(** Converts a native workflow failure into the broad public error type while
    retaining retryability, structured details, and a bounded diagnostic. A
    terminal client failure remains a workflow failure even when its Core
    diagnostic contains an activity or child-workflow wrapper; those more
    specific categories are reserved for errors observed inside a running
    workflow through an activity or child future. *)
let workflow_failure_error ?(category = `Workflow)
    (failure : Workflow_protocol.failure) =
  let non_retryable = Workflow_protocol.failure_non_retryable failure in
  Error.make ~non_retryable ~category
    ~details:(failure_details failure)
    ~message:(Failure_diagnostic.failure_diagnostic failure) ()

(** Builds a typed cancellation/termination error from terminal detail
    payloads. Details remain binary-safe and are not interpolated into logs. *)
let terminal_details_error ~category ~message details =
  Error.make ~category
    ~details:(List.map public_payload details) ~message ()

(** Canonical [Codec.unit] / [binary/null] payload. The worker maps unit
    completions to zero Core payloads; the client reconstructs this marker so
    unit-output workflows decode instead of becoming a spurious codec error. *)
let unit_null_payload : Payload.t =
  { Payload.metadata = [ ("encoding", "binary/null") ]; data = Bytes.empty }

(** Converts one native wait response into the existing public terminal-result
    algebra. A single payload is the normal case; zero payloads are the unit
    completion marker; multiple payloads remain a codec error. *)
let native_terminal_result (response : Client_protocol.wait_response) =
  match response.outcome with
  | Client_protocol.Completed { result; successor = _ } -> (
      match result with
      | [ payload ] -> Ok (Completed (public_payload payload))
      | [] -> Ok (Completed unit_null_payload)
      | _ ->
          Error
            (Error.make ~category:`Codec
               ~message:
                 "Temporal completed with multiple output payloads; the public client expects one"
               ()))
  | Client_protocol.Failed { failure; successor = _ } ->
      Ok (Failed (workflow_failure_error failure))
  | Client_protocol.Cancelled { details } ->
      Ok
        (Cancelled
           (terminal_details_error ~category:`Cancelled
              ~message:"workflow execution was cancelled" details))
  | Client_protocol.Terminated { details } ->
      Ok
        (Terminated
           (Error.make ~non_retryable:true ~category:`Terminated
              ~details:(List.map public_payload details)
              ~message:"workflow execution was terminated" ()))
  | Client_protocol.Timed_out { successor } ->
      let successor =
        match successor with
        | None -> ""
        | Some successor -> "; successor_run_id=" ^ successor.run_id
      in
      Ok
        (Timed_out
           (Error.make ~category:`Timeout
              ~message:("workflow execution timed out" ^ successor) ()))
  | Client_protocol.Continued_as_new { successor } ->
      Ok
        (Continued_as_new
           {
             workflow_id = successor.workflow_id;
             run_id = successor.run_id;
           })

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

(** Constructs either the deterministic mock ledger or the real native client.
    Native creation first validates the endpoint, then creates the complete
    supervisor graph, connects the official Rust client, and cleans up the
    graph if any step fails. No partially connected value is published. *)
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
               mutex = Mutex.create ();
               executions = Hashtbl.create 16;
             })
      else
        match
          Native.client_config ~target_url:config.target_url
            ~identity:config.identity
        with
        | Error error -> Error (bridge_error (Printf.sprintf "native client configuration failed: %s" error.message))
        | Ok native_config -> (
            match Native.create ~capacity:32 () with
            | Error error -> Error (native_supervisor_error error)
            | Ok supervisor -> (
                match Native.perform supervisor (Native.Connect_client native_config) with
                | Ok () ->
                    Ok
                      (Native_client
                         {
                           namespace = config.namespace;
                           supervisor;
                           next_request_id = Atomic.make 0;
                           closed = Atomic.make false;
                         })
                | Error error ->
                    (* The supervisor contract consumes the complete graph on
                       every shutdown result, including an error. *)
                    ignore (Native.shutdown supervisor);
                    Error (native_supervisor_error error)))

(** Allocates a process-local logical request ID without sharing mutable
    protocol state across producer Domains. The ID identifies the logical
    start, not an individual network retry. *)
let native_request_id client =
  let sequence = Atomic.fetch_and_add client.next_request_id 1 in
  Printf.sprintf "ocaml-client-start-%d" sequence

(** Converts one public start request to the closed native protocol value. *)
let native_start_request client (request : start_request) : Client_protocol.start_request =
  let request_id =
    match request.request_id with
    | Some request_id -> request_id
    | None -> native_request_id client
  in
  {
    request_id;
    namespace = client.namespace;
    workflow_id = request.workflow_id;
    workflow_type = request.workflow_name;
    task_queue = request.task_queue;
    input = [ protocol_payload request.input ];
  }

(** Starts one native workflow through the asynchronous ticket path. Each
    bounded wait releases the OCaml runtime lock inside the C bridge; retrying
    [None] keeps the supervisor mailbox able to accept shutdown and other
    lifecycle messages. *)
let native_client_start (client : native_client) (request : start_request) :
    (start_response, Error.t) result =
  if Atomic.get client.closed then Error (bridge_error "client is shut down")
  else
    let request = native_start_request client request in
    match Native.perform client.supervisor (Native.Client_begin_start_workflow request) with
    | Error error -> Error (native_supervisor_error error)
    | Ok (Error error) -> Error (native_client_error error)
    | Ok (Ok ticket) ->
        let rec await_outcome () : (start_response, Error.t) result =
          if Atomic.get client.closed then
            Error (bridge_error "client is shut down")
          else
            match
              Native.perform client.supervisor
                (Native.Client_wait_start_workflow ticket)
            with
            | Error error -> Error (native_supervisor_error error)
            | Ok None ->
                (* The native wait is intentionally bounded. Yielding here is
                   only on the ordinary caller Domain, never on the supervisor
                   owner or a workflow scheduler fiber. *)
                Thread.yield ();
                await_outcome ()
          | Ok (Some (Client_protocol.Accepted { execution })) ->
              Ok
                {
                  workflow_id = execution.workflow_id;
                  run_id = execution.run_id;
                }
          | Ok (Some (Client_protocol.Rejected error)) ->
              Error (native_client_error error)
          | Ok
              (Some
                 (Client_protocol.Unknown { request_id; workflow_id })) ->
              Error
                (Error.make ~non_retryable:true ~category:`Bridge
                   ~message:
                     (Printf.sprintf
                        "Temporal did not prove whether workflow start %S was accepted (request_id=%S)"
                        workflow_id request_id)
                   ())
        in
        await_outcome ()

(** Starts a mock execution and preserves the exact request payload. *)
let mock_client_start (client : mock_client) (request : start_request) =
  Mutex.lock client.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock client.mutex)
    (fun () ->
      if client.closed then Error (bridge_error "client is shut down")
      else if Hashtbl.mem client.executions request.workflow_id then
        Error
          (Error.make ~non_retryable:true ~category:`Workflow
             ~message:"workflow id already exists" ())
      else
        let run_id =
          client.next_run <- client.next_run + 1;
          Printf.sprintf "mock-run-%d" client.next_run
        in
        Hashtbl.add client.executions request.workflow_id
          { run_id; input = copy_payload request.input; terminal = Mock_pending };
        let response : start_response =
          { workflow_id = request.workflow_id; run_id }
        in
        Ok response)

(** Starts a workflow on the selected private transport. *)
let client_start client request =
  match client with
  | Mock_client client -> mock_client_start client request
  | Native_client client -> native_client_start client request

(** Waits for one exact native run. Open runs return [Not_ready] from each
    bounded history wait; retrying through the supervisor preserves exact-run
    identity while allowing shutdown to linearize between attempts. *)
let native_client_wait (client : native_client) (request : wait_request) =
  if Atomic.get client.closed then Error (bridge_error "client is shut down")
  else
    let request : Client_protocol.wait_request =
      {
        namespace = client.namespace;
        workflow_id = request.workflow_id;
        run_id = request.run_id;
      }
    in
    let rec await_terminal () =
      if Atomic.get client.closed then Error (bridge_error "client is shut down")
      else
        match Native.perform client.supervisor (Native.Client_wait_workflow request) with
        | Error (Native.Backend { Bridge.status = Bridge.Not_ready; _ }) ->
            Thread.yield ();
            await_terminal ()
        | Error error -> Error (native_supervisor_error error)
        | Ok (Error error) -> Error (native_client_error error)
        | Ok (Ok response) -> native_terminal_result response
    in
    await_terminal ()

(** Waits for a mock execution and echoes its input as the completed output.
    Echoing is sufficient to test typed output decoding without coupling this
    private transport to any application workflow implementation. *)
let mock_client_wait (client : mock_client) (request : wait_request) =
  Mutex.lock client.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock client.mutex)
    (fun () ->
      if client.closed then Error (bridge_error "client is shut down")
      else
        match Hashtbl.find_opt client.executions request.workflow_id with
        | None -> Error (bridge_error "workflow execution was not started")
        | Some execution
          when not (String.equal execution.run_id request.run_id) ->
            Error (bridge_error "workflow run id does not match the started run")
        | Some execution -> (
            match execution.terminal with
            | Mock_pending ->
                (* The first wait linearizes the synthetic execution into its
                   completed terminal state before returning the copied
                   payload. *)
                execution.terminal <- Mock_completed;
                Ok (Completed (copy_payload execution.input))
            | Mock_completed -> Ok (Completed (copy_payload execution.input))
            | Mock_cancelled ->
                Ok
                  (Cancelled
                     (Error.make ~category:`Cancelled
                        ~message:"workflow execution was cancelled" ()))))

(** Waits for a terminal result on the selected private transport. *)
let client_wait client (request : wait_request) =
  match client with
  | Mock_client client -> mock_client_wait client request
  | Native_client client -> native_client_wait client request

(** Converts one public cancellation request to the closed native protocol
    representation. The namespace is supplied by the connected client rather
    than copied from a caller-controlled handle. *)
let native_cancel_request client (request : cancel_request) :
    Client_protocol.cancel_request =
  {
    execution =
      {
        namespace = client.namespace;
        workflow_id = request.workflow_id;
        run_id = request.run_id;
      };
    request_id = request.request_id;
    reason = request.reason;
  }

(** Requests cancellation through the serialized supervisor operation. The
    returned acknowledgement only means Temporal accepted the RPC; the exact
    wait path remains responsible for observing the eventual terminal state. *)
let native_client_cancel (client : native_client) (request : cancel_request) :
    (unit, Error.t) result =
  if Atomic.get client.closed then Error (bridge_error "client is shut down")
  else
    match
      Native.perform client.supervisor
        (Native.Client_cancel_workflow (native_cancel_request client request))
    with
    | Error error -> Error (native_supervisor_error error)
    | Ok (Error error) -> Error (native_client_error error)
    | Ok (Ok ()) -> Ok ()

(** Marks one exact mock execution cancelled. Repeated cancellation requests
    are idempotent, while a mismatched workflow/run pair is rejected so the
    deterministic seam exercises the same identity contract as native Core.
    A completed execution is deliberately left unchanged: terminal history is
    immutable even when a caller sends a late cancellation request. *)
let mock_client_cancel (client : mock_client) (request : cancel_request) =
  Mutex.lock client.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock client.mutex)
    (fun () ->
      if client.closed then Error (bridge_error "client is shut down")
      else
        match Hashtbl.find_opt client.executions request.workflow_id with
        | None -> Error (bridge_error "workflow execution was not started")
        | Some execution
          when not (String.equal execution.run_id request.run_id) ->
            Error (bridge_error "workflow run id does not match the started run")
        | Some execution ->
            (match execution.terminal with
            | Mock_pending -> execution.terminal <- Mock_cancelled
            | Mock_completed | Mock_cancelled -> ());
            Ok ())

(** Requests cancellation on the selected private transport. *)
let client_cancel client request =
  match client with
  | Mock_client client -> mock_client_cancel client request
  | Native_client client -> native_client_cancel client request

(** Marks a client closed while preserving idempotent cleanup. Native shutdown
    runs the supervisor's reverse-order worker/client/runtime release path;
    mock shutdown only flips its local lifecycle bit. The supervisor caches the
    terminal shutdown result and its backend contract guarantees that an error
    still consumes or invalidates the complete native graph, so recording the
    public closed bit before returning an error cannot strand a live handle. *)
let client_shutdown = function
  | Mock_client client ->
      Mutex.lock client.mutex;
      Fun.protect
        ~finally:(fun () -> Mutex.unlock client.mutex)
        (fun () ->
          client.closed <- true;
          Ok ())
  | Native_client client ->
      if Atomic.exchange client.closed true then Ok ()
      else
        match Native.shutdown client.supervisor with
        | Ok () -> Ok ()
        | Error error -> Error (native_supervisor_error error)

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
               mutex = Mutex.create ();
               workflow_tasks;
               activity_tasks;
               outstanding_workflows = Hashtbl.create 16;
               outstanding_activities = Hashtbl.create 16;
               idle_workflow_polls = 0;
               idle_activity_polls = 0;
             })

(** Polls the workflow queue and records ownership before exposing a task. *)
let worker_poll_workflow (Mock_worker worker) =
  Mutex.lock worker.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock worker.mutex)
    (fun () ->
      if worker.closed then Ok Shutdown
      else if Queue.is_empty worker.workflow_tasks then (
        worker.idle_workflow_polls <- worker.idle_workflow_polls + 1;
        Ok Shutdown)
      else
        let task = Queue.take worker.workflow_tasks in
        Hashtbl.replace worker.outstanding_workflows task.task_token ();
        Ok (Task task))

(** Polls the activity queue and records ownership before exposing a task. *)
let worker_poll_activity (Mock_worker worker) =
  Mutex.lock worker.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock worker.mutex)
    (fun () ->
      if worker.closed then Ok Shutdown
      else if Queue.is_empty worker.activity_tasks then (
        worker.idle_activity_polls <- worker.idle_activity_polls + 1;
        Ok Shutdown)
      else
        let task = Queue.take worker.activity_tasks in
        Hashtbl.replace worker.outstanding_activities task.task_token ();
        Ok (Task task))

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
  Mutex.lock worker.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock worker.mutex)
    (fun () ->
      if worker.closed then Error (bridge_error "worker is shut down")
      else
        let token =
          match completion with
          | Workflow_completed { task_token; _ }
          | Workflow_failed { task_token; _ } -> task_token
        in
        require_workflow_token worker token)

(** Completes an activity task after checking its one-shot token lease. *)
let worker_complete_activity (Mock_worker worker) completion =
  Mutex.lock worker.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock worker.mutex)
    (fun () ->
      if worker.closed then Error (bridge_error "worker is shut down")
      else
        let token =
          match completion with
          | Activity_completed { task_token; _ }
          | Activity_failed { task_token; _ } -> task_token
        in
        require_activity_token worker token)

(** Closes worker admission; Core-backed implementations will first drain
    pollers and outstanding leases before returning from this function. *)
let worker_shutdown (Mock_worker worker) =
  Mutex.lock worker.mutex;
  Fun.protect
    ~finally:(fun () -> Mutex.unlock worker.mutex)
    (fun () ->
      worker.closed <- true;
      Ok ())
