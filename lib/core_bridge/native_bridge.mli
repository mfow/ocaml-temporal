(** Reason a call to the linked Rust library failed. [Unknown code] retains a
    numeric status introduced by a newer Rust bridge instead of losing it. *)
type status =
  | Invalid_argument
  | Abi_mismatch
  | Panic
  | Internal
  | Invalid_state
  | Configuration
  | Connection
  | Worker
  | Outstanding_tasks
  | Not_ready
  | Protocol
  | Unknown of int

(** Error copied into the OCaml heap. Once returned, it contains no pointer to
    Rust memory. *)
type error = {
  status : status;
  message : string;
}

(** Private owner of the Temporal Core runtime and Tokio executor for one SDK
    instance. It is intentionally abstract and never enters the public API. *)
type runtime

(** Validated settings for one official Temporal client connection. The JSON
    representation and concrete fields remain private so callers cannot bypass
    sender-side transport validation. *)
type client_config

(** Validated settings for one workflow-only Core worker. Construction does
    not perform network access; the supervisor sends it to Rust at start. *)
type worker_config

(** Version of the C-compatible interface expected by this OCaml code. *)
val abi_version : int32

(** Checks whether the linked Rust library implements [requested_version]. *)
val check_abi_version : int32 -> (unit, error) result

(** Sends bytes through the Rust allocation boundary and copies them back. This
    exists to test memory ownership; workflow code does not use it. *)
val echo : bytes -> (bytes, error) result

(** Waits in Rust for at most 1,000 milliseconds while allowing other OCaml
    Domains to run. This tests the blocking-call design used by future worker
    polling. Values outside 0 through 1,000 return an error. *)
val conformance_wait_ms : int -> (unit, error) result

(** Validates connection settings without opening a connection. The bridge
    applies only transport-safety checks; Core and Temporal Server retain
    authority over namespace-configurable semantic limits. *)
val client_config :
  target_url:string -> identity:string -> (client_config, error) result

(** Validates workflow-only worker settings without constructing a worker.
    Counts are explicit so resource policy is visible to the application. *)
val worker_config :
  namespace:string ->
  task_queue:string ->
  build_id:string ->
  max_cached_workflows:int ->
  max_outstanding_workflow_tasks:int ->
  max_concurrent_workflow_task_polls:int ->
  graceful_shutdown_timeout_ms:int64 ->
  (worker_config, error) result

(** Creates a native runtime after checking that the statically linked bridge
    implements the compatibility contract expected by this OCaml build. *)
val runtime_create : unit -> (runtime, error) result

(** Connects the official Core-based Temporal client. The network wait occurs
    in Rust while the C stub has released the OCaml runtime lock. *)
val client_connect : runtime -> client_config -> (unit, error) result

(** Constructs a workflow-only worker and completes Core namespace validation
    before publishing it into the owned graph. *)
val worker_start : runtime -> worker_config -> (unit, error) result

(** Takes one ready workflow activation without waiting. [Not_ready] is an
    expected empty-lane result. Successful bytes are a closed semantic JSON
    document copied into the OCaml heap. *)
val worker_try_poll_workflow : runtime -> (bytes, error) result

(** Waits for workflow readiness without consuming a task. The native wait is
    bounded and releases the OCaml runtime lock; [Not_ready] requests a retry
    so the supervisor mailbox can process lifecycle messages. *)
val worker_wait_workflow : runtime -> (unit, error) result

(** Validates and completes one previously leased workflow activation. The
    completion JSON must identify the exact run returned by the poll operation.
    Rust retains no input bytes after the call returns. *)
val worker_complete_workflow_json :
  runtime -> bytes -> (unit, error) result

(** Returns the exact Rust-produced activation after an OCaml semantic decode
    failure. Rust reparses and matches the complete retained activation before
    failing Core and retiring its one-shot lease. *)
val worker_reject_workflow_json : runtime -> bytes -> (unit, error) result

(** Takes one ready remote activity task without waiting. Successful bytes are
    a closed semantic activity-task JSON document. *)
val worker_try_poll_activity : runtime -> (bytes, error) result

(** Waits for remote-activity readiness without consuming a task. It has the
    same bounded lock-release semantics as [worker_wait_workflow]. *)
val worker_wait_activity : runtime -> (unit, error) result

(** Validates and completes one previously leased remote activity task. The
    opaque task token in the JSON must match the poll result exactly. *)
val worker_complete_activity_json :
  runtime -> bytes -> (unit, error) result

(** Returns the exact Rust-produced task after an OCaml semantic decode
    failure. Rust matches the complete retained task, extracts its canonical
    opaque token, and retires that native obligation exactly once. *)
val worker_reject_activity_json : runtime -> bytes -> (unit, error) result

(** Gracefully finalizes the worker. Absence is treated as already shut down,
    making sequential repeated calls safe. *)
val worker_shutdown : runtime -> (unit, error) result

(** Drops the connected client after its worker is absent. Absence is treated
    as already disconnected. *)
val client_disconnect : runtime -> (unit, error) result

(** Destroys the complete native graph in worker-client-runtime order.
    Repeating this call on the same value is safe; explicit child operations
    remain useful for deterministic diagnostics but are not required for
    leak-free defensive cleanup. *)
val runtime_close : runtime -> (unit, error) result
