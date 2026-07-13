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
  | Already_started
  | Retryable
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

(** Starts one dynamically named workflow through the connected Rust client.
    The returned bytes are a strictly validated client-start response; a
    duplicate workflow ID is returned as [Already_started] with its closed
    structured error document. *)
val client_start_workflow_json : runtime -> bytes -> (bytes, error) result

(** Requests cancellation of one exact workflow run. A successful response is
    only the server acknowledgement; callers must use the exact-run wait
    operation to observe the eventual [Cancelled] terminal outcome. *)
val client_cancel_workflow_json : runtime -> bytes -> (bytes, error) result

(** Admits one asynchronous workflow start and returns a strict opaque ticket
    document. Rust owns the pending task and its request metadata until a
    later poll or bounded wait reaches a terminal outcome. *)
val client_begin_start_workflow_json : runtime -> bytes -> (bytes, error) result

(** Polls one asynchronous start ticket without waiting. [Not_ready] means the
    request remains in flight; a successful response is a terminal
    accepted/rejected/unknown outcome document and retires the ticket. *)
val client_poll_start_workflow_json : runtime -> bytes -> (bytes, error) result

(** Waits for one bounded interval for an asynchronous start ticket. The C
    binding releases the OCaml runtime lock around the native wait, and
    [Not_ready] asks the supervisor to service its mailbox and retry. *)
val client_wait_start_workflow_json : runtime -> bytes -> (bytes, error) result

(** Waits for one exact workflow run. Rust performs a close-event long poll for
    at most 100 ms while the C stub releases the OCaml runtime lock. An open
    run returns [Not_ready] without a terminal response so a caller can retry;
    continued-as-new is returned as a terminal response and is never followed
    implicitly. *)
val client_wait_workflow_json : runtime -> bytes -> (bytes, error) result

(** Completes an activity already handed off with [WillCompleteAsync] through
    the namespace-bound Temporal client. This does not touch the worker's
    outstanding-task ledger. *)
val client_complete_async_activity_json : runtime -> bytes -> (unit, error) result

(** Records a heartbeat for an admitted asynchronous activity through the
    namespace-bound client. *)
val client_record_async_activity_heartbeat_json :
  runtime -> bytes -> (unit, error) result

(** Constructs a workflow-only worker and completes Core namespace validation
    before publishing it into the owned graph. *)
val worker_start : runtime -> worker_config -> (unit, error) result

(** Constructs the private workflow-only replay worker. It does not require a
    client connection because histories arrive through the bounded feeder. *)
val replay_worker_start : runtime -> worker_config -> (unit, error) result

(** Validates and feeds one strict replay-history JSON document. The native
    OCaml side checks the closed envelope and canonical payload first; the
    native feeder repeats those checks, accepts one queued history, and applies
    backpressure to later calls. *)
val replay_worker_feed_history : runtime -> bytes -> (unit, error) result

(** Closes replay input. Already queued histories remain available to drain. *)
val replay_worker_finish_input : runtime -> (unit, error) result

(** Takes one ready replay activation without waiting. *)
val replay_worker_try_poll_workflow : runtime -> (bytes, error) result

(** Waits for replay readiness without consuming a task. *)
val replay_worker_wait_workflow : runtime -> (unit, error) result

(** Validates and completes one previously leased replay activation. *)
val replay_worker_complete_workflow_json :
  runtime -> bytes -> (unit, error) result

(** Retires one replay activation that OCaml could not decode. *)
val replay_worker_reject_workflow_json :
  runtime -> bytes -> (unit, error) result

(** Finalizes a naturally drained replay. A failure retains the native graph. *)
val replay_worker_finalize : runtime -> (unit, error) result

(** Explicitly abandons replay and force-completes native debts. *)
val replay_worker_dispose : runtime -> (unit, error) result

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

(** Applies the fixed native delay used only after an explicit retryable
    activity-completion transport outcome. *)
val worker_wait_activity_completion_retry_backoff :
  runtime -> (unit, error) result

(** Validates and completes one previously leased remote activity task. The
    opaque task token in the JSON must match the poll result exactly. *)
val worker_complete_activity_json :
  runtime -> bytes -> (unit, error) result

(** Validates and submits progress for a currently leased remote activity. The
    task remains outstanding for its later terminal completion. *)
val worker_record_activity_heartbeat_json :
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
