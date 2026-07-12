(** Private production wiring for the public OCaml worker.

    The public [Temporal.Worker] module owns registration ergonomics and the
    deterministic mock seam used by unit tests. This module owns the real
    integration: one [Sdk_supervisor.Native] instance, one workflow adapter,
    and one activity adapter. Rust/Core remains behind the supervisor; this
    module never stores a native pointer or a raw JSON document. *)

module Native = Sdk_supervisor.Native
module Bridge = Temporal_core_bridge.Native_bridge
module Base_error = Temporal_base.Error
module Observability = Temporal_base.Observability
module Workflow_adapter = Temporal_runtime.Native_worker_execution
module Activity_adapter = Temporal_runtime.Native_activity_execution

(** Result-bind notation keeps expected startup and lifecycle failures typed. *)
let ( let* ) = Result.bind

(** A bounded diagnostic protects logs and public errors from an unexpectedly
    verbose native exception or server response. Payload bytes are never copied
    into this diagnostic. *)
let bounded_message value =
  let maximum = 1_024 in
  if String.length value <= maximum then value
  else String.sub value 0 (maximum - 3) ^ "..."

(** Converts a bridge status to the stable lowercase label used in diagnostics.
    The mapping is intentionally local because the bridge keeps this helper
    private to avoid exposing Rust-specific naming in the public API. *)
let bridge_status = function
  | Bridge.Invalid_argument -> "invalid_argument"
  | Abi_mismatch -> "abi_mismatch"
  | Panic -> "panic"
  | Internal -> "internal"
  | Invalid_state -> "invalid_state"
  | Configuration -> "configuration"
  | Connection -> "connection"
  | Worker -> "worker"
  | Outstanding_tasks -> "outstanding_tasks"
  | Not_ready -> "not_ready"
  | Protocol -> "protocol"
  | Already_started -> "already_started"
  | Unknown code -> Printf.sprintf "unknown(%d)" code

(** Converts the supervisor's opaque error into a bounded worker diagnostic.
    [Supervisor_failed] is an internal defect; it is still represented as a
    result so callers do not need to catch an exception during shutdown.

    Worker and outstanding-task statuses are closed categories at the public
    boundary. The Rust bridge normally supplies constant messages for them,
    but repeating the mapping here also protects callers from a stale native
    library or a malformed test double that still carries Core/gRPC prose. *)
let native_error_view (error : Native.error) =
  match error with
  | Native.Backend ({ Bridge.status; message } : Bridge.error) ->
      let message =
        match status with
        | Bridge.Worker -> "native worker operation failed"
        | Bridge.Outstanding_tasks -> "native worker has outstanding tasks"
        | _ -> bounded_message message
      in
      (bridge_status status, message)
  | Native.Closed -> ("closed", "native supervisor is shut down")
  | Native.Supervisor_failed exception_ ->
      let message =
        try Printexc.to_string exception_ with _ ->
          "unprintable supervisor exception"
      in
      ("supervisor_failed", bounded_message message)

(** Converts a supervisor failure into the broad public bridge category while
    retaining the operation and native classification in one readable message. *)
let public_native_error operation error =
  let code, message = native_error_view error in
  Base_error.make ~category:`Bridge
    ~message:(Printf.sprintf "%s failed (%s): %s" operation code message)
    ()

(** Converts a configuration error produced before the supervisor exists. The
    bridge configuration helpers use their lower-level error record rather than
    the supervisor's lifecycle variant. *)
let public_bridge_error operation ({ Bridge.status; message } : Bridge.error) =
  Base_error.make ~category:`Bridge
    ~message:(
      Printf.sprintf "%s failed (%s): %s" operation (bridge_status status)
        (bounded_message message))
    ()

(** Converts an adapter diagnostic into a public bridge error without exposing
    the adapter's private record type or any task-token bytes. *)
let public_adapter_error operation
    ({ code; path; message } : Workflow_adapter.error_view) =
  Base_error.make ~category:`Bridge
    ~message:(
      Printf.sprintf "%s failed at %s (%s): %s" operation path code message)
    ()

(** Activity adapter diagnostics have the same shape as workflow diagnostics
    but remain a distinct private type, so this conversion is explicit. *)
let public_activity_error operation
    ({ code; path; message } : Activity_adapter.error_view) =
  Base_error.make ~category:`Bridge
    ~message:(
      Printf.sprintf "%s failed at %s (%s): %s" operation path code message)
    ()

(** The adapter functors need only the two typed operations below. Each call
    still enters the supervisor mailbox, so workflow and activity operations
    cannot race native lifecycle changes. *)
module Workflow_source = struct
  type t = Native.t
  type error = Native.error

  (** Drains one ready workflow activation through the supervisor mailbox. *)
  let try_poll_workflow supervisor =
    Native.perform supervisor Native.Try_poll_workflow

  (** Submits one semantic workflow completion through the supervisor mailbox. *)
  let complete_workflow supervisor completion =
    Native.perform supervisor (Native.Complete_workflow completion)

  (** Returns the stable classification used in adapter diagnostics. *)
  let error_code error = fst (native_error_view error)

  (** Returns the bounded diagnostic used in adapter diagnostics. *)
  let error_message error = snd (native_error_view error)
end

(** Activity operations use the same supervisor instance as workflow operations;
    a separate source module preserves the adapter's typed signatures. *)
module Activity_source = struct
  type t = Native.t
  type error = Native.error

  (** Drains one ready activity task through the supervisor mailbox. *)
  let try_poll_activity supervisor =
    Native.perform supervisor Native.Try_poll_activity

  (** Submits one semantic activity completion through the supervisor mailbox. *)
  let complete_activity supervisor completion =
    Native.perform supervisor (Native.Complete_activity completion)

  (** Records progress for the currently leased activity through the same
      supervisor mailbox as polling and completion. *)
  let record_activity_heartbeat supervisor heartbeat =
    Native.perform supervisor (Native.Record_activity_heartbeat heartbeat)

  (** Returns the stable classification used in adapter diagnostics. *)
  let error_code error = fst (native_error_view error)

  (** Returns the bounded diagnostic used in adapter diagnostics. *)
  let error_message error = snd (native_error_view error)
end

(** Instantiates the workflow adapter with the production supervisor source. *)
module Workflow = Workflow_adapter.Make (Workflow_source)

(** Instantiates the activity adapter with the production supervisor source. *)
module Activity = Activity_adapter.Make (Activity_source)

(** The hidden existential registrations retain each definition's codecs next to
    its implementation. This prevents a completion from being encoded through
    a different type witness than the input that was decoded. *)
type workflow_registration = Workflow_adapter.registered_workflow
type activity_registration = Activity_adapter.registered_activity

(** Packs a workflow definition for [Workflow.create]. *)
let register_workflow definition = Workflow_adapter.register definition

(** Packs an activity definition for [Activity.create]. *)
let register_activity definition = Activity_adapter.register definition

(** Default native worker resource settings. They are deliberately explicit and
    stable so every worker has bounded Core resource usage even before a richer
    public options record is added. *)
let default_build_id = "ocaml-temporal"
let default_max_cached_workflows = 1_000
let default_max_outstanding_workflow_tasks = 1_000
(* Temporal Core requires at least two workflow-task pollers when workflow
   caching is enabled; the bridge validates the same invariant on both sides. *)
let default_max_concurrent_workflow_task_polls = 2
let default_graceful_shutdown_timeout_ms = 30_000L
let supervisor_capacity = 32

(** Native worker lifecycle state. The atomic flag is the only state observed
    by the polling loop from [shutdown]; adapter maps remain owner-confined to
    the run loop. [shutdown_retryable] distinguishes a failed adapter drain
    (where the native graph is still usable) from a native teardown failure
    (where reopening the public worker would only hide a terminal graph). *)
type t = {
  supervisor : Native.t;
  workflows : Workflow.t;
  activities : Activity.t;
  closed : bool Atomic.t;
  shutdown_retryable : bool Atomic.t;
  run_mutex : Mutex.t;
  (** [Some domain] while [run] holds [run_mutex] on that Domain. Used to
      reject re-entrant [run]/[shutdown] from the same Domain (for example an
      activity implementation calling back into the worker) which would
      otherwise deadlock on the non-recursive mutex. *)
  run_domain : Domain.id option Atomic.t;
}

(** Reports worker lifecycle events without allowing a logging backend defect to
    alter lease ownership or shutdown ordering. *)
let report level ~operation ?error_kind () =
  try
    let tags = Observability.tags ~operation ?error_kind () in
    Observability.report ~src:Observability.Source.lifecycle level ~tags
      "native public worker event"
  with _ -> ()

(** Returns [true] only for the bounded readiness timeout. Other native errors
    must propagate because they may indicate a lost worker or connection. *)
let is_not_ready = function
  | Native.Backend { Bridge.status = Bridge.Not_ready; _ } -> true
  | _ -> false

(** Converts a successful adapter summary into progress. A rejected task has
    already been acknowledged with a failure completion and therefore must not
    stop the worker loop. *)
type progress = Progress | Not_ready

(** Maps one workflow adapter poll and keeps only the scheduling information the
    outer loop needs. The adapter's detailed rejection is logged without
    copying a run ID into an error message. *)
let poll_workflow worker =
  match Workflow.poll worker.workflows with
  | Ok Workflow_adapter.Not_ready -> Ok Not_ready
  | Ok (Workflow_adapter.Completed _) -> Ok Progress
  | Ok (Workflow_adapter.Rejected { error; lease_retired = true; _ }) ->
      report Logs.Warning ~operation:"workflow_task_rejected"
        ~error_kind:error.code ();
      Ok Progress
  | Ok (Workflow_adapter.Rejected { error; lease_retired = false; _ }) ->
      Error (public_adapter_error "workflow task completion" error)
  | Error error -> Error (public_adapter_error "workflow task poll" error)

(** Maps one activity adapter poll using the same lease-retirement rule as the
    workflow path. An acknowledged activity failure is ordinary progress. *)
let poll_activity worker =
  match Activity.poll worker.activities with
  | Ok Activity_adapter.Not_ready -> Ok Not_ready
  | Ok (Activity_adapter.Completed _) -> Ok Progress
  | Ok (Activity_adapter.Rejected { error; lease_retired = true; _ }) ->
      report Logs.Warning ~operation:"activity_task_rejected"
        ~error_kind:error.code ();
      Ok Progress
  | Ok (Activity_adapter.Rejected { error; lease_retired = false; _ }) ->
      Error (public_activity_error "activity task completion" error)
  | Error error -> Error (public_activity_error "activity task poll" error)

(** Waits on one bounded native readiness lane. The C bridge releases the OCaml
    runtime lock during this operation, and the bounded result lets [shutdown]
    regain the supervisor mailbox without waiting forever. *)
let wait_for_lane worker ~workflow_lane =
  let operation : unit Native.operation =
    if workflow_lane then Native.Wait_workflow else Native.Wait_activity
  in
  match Native.perform worker.supervisor operation with
  | Ok () -> Ok ()
  | Error error when is_not_ready error -> Ok ()
  | Error error -> Error (public_native_error "worker readiness wait" error)

(** Runs one serialized worker loop. It alternates readiness lanes when both
    queues are empty so an activity-only workload cannot be starved by workflow
    waits (and vice versa). *)
let run worker =
  let self = Domain.self () in
  (match Atomic.get worker.run_domain with
  | Some domain_id when domain_id = self ->
      Error
        (Base_error.defect
           ~message:
             "worker run is re-entrant on the same Domain; activity or host code must not call Worker.run while a run loop is active")
  | _ ->
      Mutex.lock worker.run_mutex;
      Atomic.set worker.run_domain (Some self);
      Fun.protect
        ~finally:(fun () ->
          Atomic.set worker.run_domain None;
          Mutex.unlock worker.run_mutex)
        (fun () ->
      let rec loop wait_workflow_lane =
        if Atomic.get worker.closed then Ok ()
        else
          let workflow_result = poll_workflow worker in
          let activity_result =
            match workflow_result with
            | Error _ as error -> error
            | Ok _ -> poll_activity worker
          in
          match (workflow_result, activity_result) with
          | Error _, _ | _, Error _ when Atomic.get worker.closed -> Ok ()
          | Error error, _ -> Error error
          | _, Error error -> Error error
          | Ok Progress, _ | _, Ok Progress -> loop (not wait_workflow_lane)
          | Ok Not_ready, Ok Not_ready ->
              let* () = wait_for_lane worker ~workflow_lane:wait_workflow_lane in
              loop (not wait_workflow_lane)
      in
      report Logs.Info ~operation:"worker_run_started" ();
      let result = loop true in
      report Logs.Info ~operation:"worker_run_finished" ();
      result))

(** Stops polling first, then waits for the loop mutex so no adapter-held lease
    remains when native worker shutdown begins. Adapter completion maps are
    drained while that mutex is held; native teardown is started only after
    both maps prove empty. If a drain fails, the graph remains usable and the
    caller can retry without losing the exact pending completion. *)
let shutdown worker =
  let self = Domain.self () in
  (match Atomic.get worker.run_domain with
  | Some domain_id when domain_id = self ->
      (* Public Worker.shutdown may already have flipped its closed bit and
         only reopens it when shutdown_retryable is true. Mark retryable so
         the public wrapper can recover instead of permanently stranding a
         still-running native worker. *)
      Atomic.set worker.shutdown_retryable true;
      Error
        (Base_error.defect
           ~message:
             "cannot shut down a worker from inside its run loop on the same Domain; that would deadlock the run mutex")
  | _ ->
  if Atomic.compare_and_set worker.closed false true then begin
    Mutex.lock worker.run_mutex;
    let drained =
      Fun.protect
        ~finally:(fun () -> Mutex.unlock worker.run_mutex)
        (fun () ->
          match Workflow.drain worker.workflows with
          | Error error ->
              Error (public_adapter_error "workflow completion drain" error)
          | Ok () -> (
              match Activity.drain worker.activities with
              | Ok () -> Ok ()
              | Error error ->
                  Error
                    (public_activity_error "activity completion drain" error)))
    in
    match drained with
    | Error error ->
        (* The native graph has not been touched. Reopening both flags leaves
           [run] and a later [shutdown] able to retry the exact completion. *)
        Atomic.set worker.closed false;
        Atomic.set worker.shutdown_retryable true;
        Error error
    | Ok () ->
        Atomic.set worker.shutdown_retryable false;
        match Native.shutdown worker.supervisor with
        | Ok () as result ->
            report Logs.Info ~operation:"worker_shutdown" ();
            result
        | Error error -> Error (public_native_error "worker shutdown" error)
  end
  else Ok ())

(** Schedules forgotten-worker cleanup off the GC finalizer thread. A finalizer
    must not block on [run_mutex] or the supervisor mailbox; the detached
    thread runs the ordinary drain-then-shutdown path. If a system thread
    cannot be created during process teardown, mark the worker closed and shut
    the supervisor so native runtime dispose can still reclaim the graph. *)
let cleanup_abandoned worker =
  if not (Atomic.get worker.closed) then
    (* Keep [worker] (and therefore [supervisor]) reachable for the lifetime of
       the cleanup thread so the supervisor's own finalizer cannot tear the
       native graph down before drain completes. The thread owns this root. *)
    match
      Thread.create
        (fun instance ->
          try ignore (shutdown instance)
          with _ ->
            Atomic.set instance.closed true;
            ignore (Native.shutdown instance.supervisor))
        worker
    with
    | _thread -> ()
    | exception _ ->
        (* Cannot spawn a helper. Do not block the finalizer Domain on the
           mailbox owner. Mark closed and request supervisor teardown without
           awaiting drain; residual native reclaim still goes through the
           runtime custom-block finalizer. *)
        Atomic.set worker.closed true;
        ignore (Native.shutdown worker.supervisor)

(** Builds the native graph and both OCaml registries. Every failure after
    [Native.create] enters [cleanup], which joins the supervisor owner Domain
    and closes all native resources before returning. Successful construction
    attaches a GC finalizer so abandoned workers still drain leases. *)
let create ~target_url ~namespace ~identity ~task_queue ~workflows ~activities () =
  let* client_config =
    Native.client_config ~target_url ~identity
    |> Result.map_error (public_bridge_error "client configuration")
  in
  let* worker_config =
    Native.worker_config ~namespace ~task_queue ~build_id:default_build_id
      ~max_cached_workflows:default_max_cached_workflows
      ~max_outstanding_workflow_tasks:default_max_outstanding_workflow_tasks
      ~max_concurrent_workflow_task_polls:
        default_max_concurrent_workflow_task_polls
      ~graceful_shutdown_timeout_ms:default_graceful_shutdown_timeout_ms
    |> Result.map_error (public_bridge_error "worker configuration")
  in
  let* supervisor =
    Native.create ~capacity:supervisor_capacity ()
    |> Result.map_error (public_native_error "native runtime creation")
  in
  let cleanup error =
    ignore (Native.shutdown supervisor);
    Error error
  in
  let setup =
    let* () =
      Native.perform supervisor (Native.Connect_client client_config)
      |> Result.map_error (public_native_error "client connection")
    in
    let* () =
      Native.perform supervisor (Native.Start_worker worker_config)
      |> Result.map_error (public_native_error "worker startup")
    in
    let* workflows =
      Workflow.create ~task_queue ~supervisor ~workflows ()
      |> Result.map_error (public_adapter_error "workflow registration")
    in
    let* activities =
      Activity.create ~supervisor ~activities
      |> Result.map_error (public_activity_error "activity registration")
    in
    Ok
      {
        supervisor;
        workflows;
        activities;
        closed = Atomic.make false;
        shutdown_retryable = Atomic.make false;
        run_mutex = Mutex.create ();
        run_domain = Atomic.make None;
      }
  in
  match setup with
  | Ok worker ->
      (* Explicit [shutdown] is the supported path. The finalizer is a last
         resort for abandoned workers: it schedules the same drain-then-native
         teardown so GC of a live [t] cannot leave Core leases without an
         OCaml completion document. *)
      Gc.finalise cleanup_abandoned worker;
      Ok worker
  | Error error -> cleanup error

(** Reports whether the most recent shutdown failure occurred before native
    teardown. The public wrapper uses this private state to reopen its own
    admission flag only for a safe adapter-drain retry. *)
let shutdown_retryable worker = Atomic.get worker.shutdown_retryable
