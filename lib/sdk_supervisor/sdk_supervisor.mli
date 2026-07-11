(** Private owner-Domain supervision for one SDK instance.

    The supervisor serializes a complete native resource graph behind typed
    operations. Its entry points block ordinary producer Domains and therefore
    must be offloaded by any cooperative scheduler adapter. No backend state or
    native handle can be obtained through this interface. *)

(** A native resource graph and the typed operations which may use it. *)
module type Backend = sig
  (** Immutable information required to create one graph. *)
  type config

  (** Owner-confined graph state. The supervisor never returns this value to a
      producer and invokes all functions receiving it on one Domain. *)
  type state

  (** Expected backend failure copied into OCaml-owned memory. *)
  type error

  (** Typed operations over the graph. The parameter selects the successful
      result without exposing [state]. *)
  type _ operation

  (** Creates the complete initial graph on the owner Domain. Returning
      [Error] means no state was created and therefore no shutdown is needed. *)
  val create : config -> (state, error) result

  (** Executes one operation on owner-confined [state]. Expected operational
      failures use [Error]; exceptions denote backend defects. *)
  val perform : state -> 'result operation -> ('result, error) result

  (** Releases every resource in reverse ownership order. It is invoked at
      most once for successfully created state. An [Error] must still mean the
      backend consumed or invalidated the complete graph. *)
  val shutdown : state -> (unit, error) result
end

(** Builds one supervisor implementation for a private backend protocol. *)
module Make (Backend : Backend) : sig
  (** Failure visible to internal callers. Backend errors are expected;
      [Supervisor_failed] contains an unexpected owner-Domain defect. *)
  type error =
    | Backend of Backend.error
    | Closed
        (** The graph has begun or completed shutdown. *)
    | Supervisor_failed of exn
        (** The owner contained an unexpected exception and terminated. *)

  (** An abstract instance containing the mailbox, owner Domain, and cached
      terminal result. It contains no publicly accessible native handle. *)
  type t

  (** [create ~capacity config] starts the owner Domain and creates the backend
      graph on it. [capacity] is the positive number of admitted operations
      which may wait behind the active operation; invalid capacity is a
      programmer error reported by [Invalid_argument]. *)
  val create : capacity:int -> Backend.config -> (t, error) result

  (** [perform instance operation] blocks an ordinary producer Domain until
      the owner has executed the typed operation. Calls are serialized in the
      mailbox admission order. *)
  val perform : t -> 'result Backend.operation -> ('result, error) result

  (** [initiate_shutdown instance] atomically admits the one terminal request
      and closes operation admission, but does not wait for earlier backend
      work or join the owner Domain. Repeated calls are harmless. This is an
      internal lifecycle seam; application code should call [shutdown]. *)
  val initiate_shutdown : t -> unit

  (** [shutdown instance] waits for earlier admitted operations, releases the
      graph once, stops and joins the owner Domain, and caches the exact
      terminal result for every later call. Abandoning an instance schedules
      this same path on a dedicated system thread; the garbage-collector
      finalizer itself never waits on mailbox locks or native teardown. *)
  val shutdown : t -> (unit, error) result
end

(** The production supervisor over one native runtime-client-worker graph. *)
module Native : sig
  (** Pure conversion functions at the native-worker boundary. Keeping these
      functions private but independently testable makes both directions of
      the JSON contract reviewable without constructing a Core worker. *)
  module Protocol_adapter : sig
    val decode_workflow_activation :
      bytes ->
      ( Temporal_protocol.Workflow_protocol.activation,
        Temporal_core_bridge.Native_bridge.error )
      result
    (** Strictly validates one workflow activation returned by Rust. *)

    val workflow_poll_result :
      reject:(bytes -> (unit, Temporal_core_bridge.Native_bridge.error) result) ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result ->
      ( Temporal_protocol.Workflow_protocol.activation option,
        Temporal_core_bridge.Native_bridge.error )
      result
    (** Maps native [Not_ready] to [None]. If successful bytes fail semantic
        validation, [reject] must retire their exact native lease first. *)

    val encode_workflow_completion :
      Temporal_protocol.Workflow_protocol.completion ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result
    (** Canonically serializes and reparses one workflow completion. *)

    val decode_activity_task :
      bytes ->
      ( Temporal_protocol.Activity_protocol.task,
        Temporal_core_bridge.Native_bridge.error )
      result
    (** Strictly validates one remote activity task returned by Rust. *)

    val activity_poll_result :
      reject:(bytes -> (unit, Temporal_core_bridge.Native_bridge.error) result) ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result ->
      ( Temporal_protocol.Activity_protocol.task option,
        Temporal_core_bridge.Native_bridge.error )
      result
    (** Maps native [Not_ready] to [None]. If successful bytes fail semantic
        validation, [reject] must retire their exact native lease first. *)

    val encode_activity_completion :
      Temporal_protocol.Activity_protocol.completion ->
      (bytes, Temporal_core_bridge.Native_bridge.error) result
    (** Canonically serializes and reparses one remote activity completion. *)
  end

  (** Validated client settings whose representation remains bridge-private. *)
  type client_config

  (** Validated workflow-only worker settings whose representation is private. *)
  type worker_config

  (** Typed lifecycle operations serialized by the owner Domain. *)
  type _ operation =
    | Check_compatibility : unit operation
    | Connect_client : client_config -> unit operation
    | Client_start_workflow : bytes -> bytes operation
        (** Starts one workflow using a private strict JSON request. *)
    | Client_wait_workflow : bytes -> bytes operation
        (** Waits for one exact run using a private strict JSON request. *)
    | Start_worker : worker_config -> unit operation
    | Try_poll_workflow :
        Temporal_protocol.Workflow_protocol.activation option operation
        (** Takes and validates one already-ready workflow activation. [None]
            means the native lane was empty at that instant. *)
    | Complete_workflow :
        Temporal_protocol.Workflow_protocol.completion -> unit operation
        (** Validates and submits one typed workflow completion. *)
    | Try_poll_activity :
        Temporal_protocol.Activity_protocol.task option operation
        (** Takes and validates one already-ready remote activity task. [None]
            means the native lane was empty at that instant. *)
    | Complete_activity :
        Temporal_protocol.Activity_protocol.completion -> unit operation
        (** Validates and submits one typed remote activity completion. *)
    | Shutdown_worker : unit operation
    | Disconnect_client : unit operation

  (** Production lifecycle and bridge failures. *)
  type error =
    | Backend of Temporal_core_bridge.Native_bridge.error
    | Closed
    | Supervisor_failed of exn

  (** An abstract SDK instance which owns the complete Rust handle graph. *)
  type t

  (** Validates client settings without network access. *)
  val client_config :
    target_url:string -> identity:string -> (client_config, Temporal_core_bridge.Native_bridge.error) result

  (** Validates explicit worker resource settings without network access. *)
  val worker_config :
    namespace:string ->
    task_queue:string ->
    build_id:string ->
    max_cached_workflows:int ->
    max_outstanding_workflow_tasks:int ->
    max_concurrent_workflow_task_polls:int ->
    graceful_shutdown_timeout_ms:int64 ->
    (worker_config, Temporal_core_bridge.Native_bridge.error) result

  (** Starts a dedicated owner Domain and creates the real Rust runtime. *)
  val create : capacity:int -> unit -> (t, error) result

  (** Runs one typed bridge operation on the sole owner Domain. Network waits
      enter Rust through C stubs which release the OCaml runtime lock. *)
  val perform : t -> 'result operation -> ('result, error) result

  (** Closes operation admission without waiting for native teardown. This is
      an internal lifecycle seam; application code should call [shutdown]. *)
  val initiate_shutdown : t -> unit

  (** Deterministically closes worker, client, runtime, then owner Domain. *)
  val shutdown : t -> (unit, error) result
end
