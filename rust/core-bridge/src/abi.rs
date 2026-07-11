use crate::worker_bridge::{PollLaneError, PollLanes, ReadinessWait, WorkerBridgeError};
use crate::{activity_protocol, client_protocol, workflow_protocol};
use serde::Deserialize;
use std::collections::{HashMap, hash_map::Entry};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{
    Receiver, RecvTimeoutError, SyncSender, TryRecvError, channel, sync_channel,
};
use std::time::Duration;
use temporalio_client::{Connection, ConnectionOptions};
use temporalio_sdk_core::{
    CoreRuntime, PollerBehavior, RuntimeOptions, TokioRuntimeBuilder, WorkerConfig,
    WorkerVersioningStrategy,
};
use tokio::task::JoinHandle;
use uuid::Uuid;

/// Version of the native ABI implemented by this crate.
pub const ABI_VERSION: u32 = 1;

/// Fixed-width status type shared with the C header.
pub type Status = i32;

/// Operation completed and `value` may own bytes.
pub const STATUS_OK: Status = 0;
/// A pointer, length, range, or other caller argument violated the ABI.
pub const STATUS_INVALID_ARGUMENT: Status = 1;
/// The caller requested an ABI version this bridge does not implement.
pub const STATUS_ABI_MISMATCH: Status = 2;
/// A Rust panic was contained before it crossed the C boundary.
pub const STATUS_PANIC: Status = 3;
/// Reserved non-panic bridge implementation failure.
pub const STATUS_INTERNAL: Status = 4;
/// An operation was requested in an incompatible graph lifecycle state.
pub const STATUS_INVALID_STATE: Status = 5;
/// A strict lifecycle configuration document was malformed or invalid.
pub const STATUS_CONFIGURATION: Status = 6;
/// Temporal client connection failed after configuration validation.
pub const STATUS_CONNECTION: Status = 7;
/// Official Core worker construction or namespace validation failed.
pub const STATUS_WORKER: Status = 8;
/// Worker shutdown is draining tasks that still require language completion.
pub const STATUS_OUTSTANDING_TASKS: Status = 9;
/// A bounded readiness operation has no result ready for handoff yet.
pub const STATUS_NOT_READY: Status = 10;
/// A semantic workflow or activity document failed strict validation.
pub const STATUS_PROTOCOL: Status = 11;
/// Temporal rejected a workflow start because the workflow ID is already in
/// use; the error buffer contains a closed client-error JSON document.
pub const STATUS_ALREADY_STARTED: Status = 12;

/// Maximum accepted lifecycle configuration document size.
const MAX_LIFECYCLE_CONFIG_BYTES: usize = 64 * 1024;
/// Private transport-safety ceiling for one string crossing the bridge.
///
/// This is deliberately not a claim about Temporal Server identifier limits,
/// which can be namespace-configurable and differ by field. Core and Server
/// remain responsible for those semantic checks.
const MAX_TRANSPORT_STRING_BYTES: usize = 64 * 1024;
/// Prevents accidental allocation of unreasonable in-process worker state.
const MAX_WORKER_COUNT: u32 = 1_000_000;
/// Initial remote-activity concurrency until the private worker config grows a
/// separately tuned activity field in the end-to-end worker slice.
const DEFAULT_MAX_OUTSTANDING_ACTIVITIES: usize = 100;
/// Initial Core server-poll concurrency for remote activity tasks.
const DEFAULT_MAX_CONCURRENT_ACTIVITY_POLLS: usize = 5;
/// Prevents an unbounded graceful-shutdown duration from entering Core.
const MAX_GRACEFUL_SHUTDOWN_MS: u64 = 24 * 60 * 60 * 1_000;
/// Maximum time one exact-run client wait may occupy the supervisor owner.
///
/// The Temporal history request remains a close-event long poll, but the
/// outer ABI operation is deliberately bounded.  When the deadline elapses,
/// Tokio drops the in-flight request and the caller receives `NOT_READY` so
/// its mailbox can admit shutdown or another lifecycle operation before it
/// retries the wait.
const CLIENT_WAIT_TIMEOUT: Duration = Duration::from_millis(100);
/// Bounds the number of in-flight client starts retained by one supervisor.
///
/// Each entry owns one Tokio task, a response channel, and the validated
/// request payload until the RPC completes. A finite ceiling prevents a caller
/// that forgets tickets from turning the supervisor into an unbounded task
/// registry; the caller can submit more work after polling or closing
/// completed tickets.
const MAX_PENDING_STARTS: usize = 64;
/// Maximum time spent in one wait-ticket ABI call before the owner regains
/// control of its mailbox and can service lifecycle messages.
const START_WAIT_TIMEOUT: Duration = Duration::from_millis(100);

const _: () = assert!(size_of::<Status>() == 4);

/// Monotonic test instrumentation for successfully exposed runtime owners.
static RUNTIMES_CREATED: AtomicU64 = AtomicU64::new(0);
/// Monotonic test instrumentation for Core instances whose destructor ran.
static RUNTIMES_CLEANED: AtomicU64 = AtomicU64::new(0);

/// Runs one exact-run history request for a bounded interval.
///
/// A Temporal history long poll can otherwise hold the single supervisor
/// owner Domain inside `Handle::block_on` until the workflow closes.  The
/// timeout is applied outside the Core request so its cancellation drops the
/// tonic future, rather than leaving a detached native operation alive.  A
/// timeout is an expected pending result (`Ok(None)`), while request errors
/// continue through the existing typed client-error conversion path.
async fn bounded_client_wait<F>(
    future: F,
) -> std::result::Result<
    Option<client_protocol::WaitWorkflowResponse>,
    client_protocol::ClientOperationError,
>
where
    F: Future<
        Output = std::result::Result<
            client_protocol::WaitWorkflowResponse,
            client_protocol::ClientOperationError,
        >,
    >,
{
    match tokio::time::timeout(CLIENT_WAIT_TIMEOUT, future).await {
        Ok(response) => response.map(Some),
        Err(_) => Ok(None),
    }
}

/// Byte allocation owned by the Rust bridge.
///
/// Callers must treat this as an opaque field of [`Result`] and release it
/// only through [`ocaml_temporal_core_v1_result_free`].
#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub struct Buffer {
    pub ptr: *mut u8,
    pub len: usize,
}

impl Default for Buffer {
    /// Returns the canonical empty buffer with no allocation ownership.
    fn default() -> Self {
        Self {
            ptr: ptr::null_mut(),
            len: 0,
        }
    }
}

impl Buffer {
    /// Transfers a vector allocation into the ABI, canonicalizing empty input.
    fn from_vec(value: Vec<u8>) -> Self {
        if value.is_empty() {
            return Self::default();
        }

        let len = value.len();
        let ptr = Box::into_raw(value.into_boxed_slice()).cast::<u8>();
        Self { ptr, len }
    }
}

/// Single result shape returned by every fallible ABI operation.
///
/// Exactly one of `value` or `error` can own bytes. A successful operation has
/// `status == STATUS_OK`; every other status may carry a UTF-8 diagnostic in
/// `error`.
#[repr(C)]
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Result {
    pub status: Status,
    pub value: Buffer,
    pub error: Buffer,
}

const _: () = {
    assert!(std::mem::offset_of!(Buffer, ptr) == 0);
    assert!(std::mem::offset_of!(Buffer, len) == size_of::<*mut u8>());
    assert!(std::mem::offset_of!(Result, status) == 0);
    assert!(std::mem::offset_of!(Result, value) == align_of::<Buffer>());
    assert!(
        std::mem::offset_of!(Result, error)
            == std::mem::offset_of!(Result, value) + size_of::<Buffer>()
    );
};

/// Internal structured failure converted into an owned ABI diagnostic.
#[derive(Debug)]
struct Failure {
    status: Status,
    message: String,
}

/// Owns the Tokio executor and shared Temporal Core runtime for one SDK instance.
///
/// The type is opaque to C. Higher-level client and worker handles will retain
/// the same runtime owner rather than creating independent executors.
pub struct Runtime {
    core: Option<CoreRuntime>,
    client: Option<Connection>,
    worker: Option<PollLanes>,
    workflow_activations: HashMap<String, workflow_protocol::Activation>,
    activity_tasks: HashMap<Vec<u8>, Vec<activity_protocol::ActivityTask>>,
    pending_starts: HashMap<String, PendingStart>,
    cleanup: std::sync::mpsc::Sender<RuntimeCleanup>,
}

/// One Rust-owned asynchronous start operation indexed by an opaque ticket.
///
/// The response channel is a one-shot handoff from a Tokio task to the sole
/// runtime owner. No Tokio task invokes OCaml or mutates [`Runtime`]; it only
/// sends the typed result and exits. The owner removes the entry exactly once
/// when a terminal result is observed, then joins the task before releasing
/// the connection clone it captured.
struct PendingStart {
    /// Complete validated request retained for exact retry matching and for
    /// the request/workflow identifiers used by an `Unknown` outcome.  Keeping
    /// the typed value (rather than only namespace and workflow ID) prevents a
    /// caller from accidentally reusing one request ID for a different task
    /// queue, workflow type, or payload and silently receiving the old ticket.
    request: Arc<client_protocol::StartWorkflowRequest>,
    /// One-shot result channel serviced only by the owner Domain.
    receiver: Receiver<
        std::result::Result<
            client_protocol::StartWorkflowResponse,
            client_protocol::ClientOperationError,
        >,
    >,
    /// Tokio task performing the network call through Core's runtime.
    task: JoinHandle<()>,
}

/// Result of one owner-side ticket-channel read.
enum StartRead {
    /// The Tokio task supplied a terminal typed result.
    Ready(
        std::result::Result<
            client_protocol::StartWorkflowResponse,
            client_protocol::ClientOperationError,
        >,
    ),
    /// No result is available in the selected poll interval.
    NotReady,
    /// The task exited without publishing a result, so acceptance is unknown.
    Disconnected,
}

/// Ownership transfer consumed by the runtime's dedicated cleanup thread.
struct RuntimeCleanup {
    core: CoreRuntime,
    client: Option<Connection>,
    worker: Option<PollLanes>,
    /// Aborted asynchronous-start tasks whose join handles must be awaited
    /// before the Core runtime and its connection clones are dropped.  The
    /// non-blocking OCaml finalizer transfers these handles here instead of
    /// dropping them on the caller thread, which would detach the tasks.
    pending_start_tasks: Vec<JoinHandle<()>>,
    completed: Option<SyncSender<Status>>,
}

/// Strict client connection document received from OCaml as UTF-8 JSON.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClientConfig {
    target_url: String,
    identity: String,
}

/// Strict workflow-only Core worker document received from OCaml as JSON.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerConfigInput {
    namespace: String,
    task_queue: String,
    build_id: String,
    max_cached_workflows: u32,
    max_outstanding_workflow_tasks: u32,
    max_concurrent_workflow_task_polls: u32,
    graceful_shutdown_timeout_ms: u64,
}

impl Runtime {
    /// Starts the cleanup thread before exposing a handle, so every successful
    /// runtime allocation already has a non-blocking GC fallback path.
    fn new(core: CoreRuntime) -> std::result::Result<Self, Failure> {
        let (cleanup, receiver) = channel();
        std::thread::Builder::new()
            .name("ocaml-temporal-runtime-cleanup".to_owned())
            .spawn(move || run_runtime_cleanup(receiver))
            .map_err(|error| Failure {
                status: STATUS_INTERNAL,
                message: format!("could not start Temporal runtime cleanup thread: {error}"),
            })?;
        RUNTIMES_CREATED.fetch_add(1, Ordering::Relaxed);
        Ok(Self {
            core: Some(core),
            client: None,
            worker: None,
            workflow_activations: HashMap::new(),
            activity_tasks: HashMap::new(),
            pending_starts: HashMap::new(),
            cleanup,
        })
    }

    /// Connects one official Core client without publishing partial state.
    fn connect_client(&mut self, config: ClientConfig) -> Operation {
        if self.client.is_some() {
            return Err(Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal client is already connected".to_owned(),
            });
        }
        let core = self.core.as_ref().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal runtime is already closed".to_owned(),
        })?;
        let target =
            temporalio_sdk_core::Url::parse(&config.target_url).map_err(|error| Failure {
                status: STATUS_CONFIGURATION,
                message: format!("client target_url is invalid: {error}"),
            })?;
        if !matches!(target.scheme(), "http" | "https") || target.host_str().is_none() {
            return Err(Failure {
                status: STATUS_CONFIGURATION,
                message: "client target_url must be an absolute http or https URL".to_owned(),
            });
        }
        validate_identifier("identity", &config.identity)?;

        let options = ConnectionOptions::new(target)
            .identity(config.identity)
            .maybe_metrics_meter(core.telemetry().get_temporal_metric_meter())
            .build();
        let connection = core
            .tokio_handle()
            .block_on(Connection::connect(options))
            .map_err(|error| Failure {
                status: STATUS_CONNECTION,
                message: format!("Temporal client connection failed: {error}"),
            })?;
        self.client = Some(connection);
        Ok(Vec::new())
    }

    /// Starts one dynamically named workflow through the connected Core
    /// client and returns a strict execution-reference JSON document.
    ///
    /// The request and response are intentionally separate from the public
    /// OCaml `Client` API.  This ABI slice is a low-level adapter that keeps
    /// dynamic workflow names and opaque payloads out of Rust's typed
    /// workflow-definition API until the native OCaml adapter is complete.
    fn start_workflow_json(&mut self, input: &[u8]) -> Operation {
        let text = decode_semantic_input(input)?;
        let request = client_protocol::decode_start_request(text).map_err(protocol_failure)?;
        let connection = self.client.as_ref().cloned().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal client is not connected".to_owned(),
        })?;
        let handle = self
            .core
            .as_ref()
            .ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal runtime is already closed".to_owned(),
            })?
            .tokio_handle();
        let response = handle
            .block_on(client_protocol::start_workflow(connection, request))
            .map_err(client_operation_failure)?;
        let encoded =
            client_protocol::encode_start_response(&response).map_err(protocol_failure)?;
        Ok(encoded.into_bytes())
    }

    /// Begins one workflow start without waiting for the RPC response.
    ///
    /// The owner Domain performs only validation, ticket bookkeeping, and
    /// Tokio task admission here. The task owns a cloned Core connection and
    /// sends exactly one typed result over a standard-library channel; it
    /// never touches this runtime or calls into OCaml. A repeated request ID
    /// while an operation is pending returns the original ticket, preventing a
    /// caller retry from issuing a second Temporal start.
    fn begin_start_workflow_json(&mut self, input: &[u8]) -> Operation {
        let text = decode_semantic_input(input)?;
        let request = client_protocol::decode_start_request(text).map_err(protocol_failure)?;
        let connection = self.client.as_ref().cloned().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal client is not connected".to_owned(),
        })?;
        let handle = self
            .core
            .as_ref()
            .ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal runtime is already closed".to_owned(),
            })?
            .tokio_handle();

        // A caller that retries a begin request with the same logical ID must
        // observe the existing ticket only when every validated request field
        // is identical. Reusing an ID for a changed workflow request is
        // rejected rather than silently aliasing the other operation.
        let request = Arc::new(request);
        if let Some((ticket, pending)) = self
            .pending_starts
            .iter()
            .find(|(_, pending)| pending.request.request_id == request.request_id)
        {
            if !client_protocol::same_start_request(pending.request.as_ref(), request.as_ref()) {
                return Err(Failure {
                    status: STATUS_PROTOCOL,
                    message: "start request_id is already pending for a different request"
                        .to_owned(),
                });
            }
            return Ok(client_protocol::encode_start_ticket(ticket)
                .map_err(protocol_failure)?
                .into_bytes());
        }
        if self.pending_starts.len() >= MAX_PENDING_STARTS {
            return Err(Failure {
                status: STATUS_INVALID_STATE,
                message: "too many Temporal workflow starts are pending".to_owned(),
            });
        }

        let ticket = loop {
            let candidate = Uuid::new_v4().to_string();
            if !self.pending_starts.contains_key(&candidate) {
                break candidate;
            }
        };
        let encoded_ticket = client_protocol::encode_start_ticket(&ticket)
            .map_err(protocol_failure)?
            .into_bytes();
        let (sender, receiver) = channel();
        let task_request = Arc::clone(&request);
        let task = handle.spawn(async move {
            let result =
                client_protocol::start_workflow(connection, task_request.as_ref().clone()).await;
            // A closed receiver means the owner is shutting down or has
            // already consumed the terminal result. Dropping this send is
            // intentional; task ownership remains entirely on the Tokio side.
            let _ = sender.send(result);
        });

        self.pending_starts.insert(
            ticket.clone(),
            PendingStart {
                request,
                receiver,
                task,
            },
        );
        Ok(encoded_ticket)
    }

    /// Polls one start ticket without waiting for the RPC task.
    fn poll_start_workflow_json(&mut self, input: &[u8]) -> Operation {
        self.read_start_workflow_json(input, false)
    }

    /// Waits up to the bounded ticket interval, then returns control to the
    /// supervisor even when Temporal has not produced a response yet.
    fn wait_start_workflow_json(&mut self, input: &[u8]) -> Operation {
        self.read_start_workflow_json(input, true)
    }

    /// Shared ticket decoder and one-shot channel handoff for poll and wait.
    fn read_start_workflow_json(&mut self, input: &[u8], wait: bool) -> Operation {
        let text = decode_semantic_input(input)?;
        let ticket = client_protocol::decode_start_ticket(text).map_err(protocol_failure)?;
        let read = {
            let pending = self
                .pending_starts
                .get_mut(&ticket)
                .ok_or_else(|| Failure {
                    status: STATUS_PROTOCOL,
                    message: "unknown Temporal workflow start ticket".to_owned(),
                })?;
            if wait {
                match pending.receiver.recv_timeout(START_WAIT_TIMEOUT) {
                    Ok(result) => StartRead::Ready(result),
                    Err(RecvTimeoutError::Timeout) => StartRead::NotReady,
                    Err(RecvTimeoutError::Disconnected) => StartRead::Disconnected,
                }
            } else {
                match pending.receiver.try_recv() {
                    Ok(result) => StartRead::Ready(result),
                    Err(TryRecvError::Empty) => StartRead::NotReady,
                    Err(TryRecvError::Disconnected) => StartRead::Disconnected,
                }
            }
        };

        match read {
            StartRead::NotReady => Err(not_ready()),
            StartRead::Ready(result) => self.finish_start(ticket, Some(result)),
            StartRead::Disconnected => self.finish_start(ticket, None),
        }
    }

    /// Removes a terminal ticket, joins its Tokio task, and encodes the
    /// explicit accepted/rejected/unknown outcome. Joining after the channel
    /// handoff proves the task no longer holds a Core connection clone before
    /// the map entry is released.
    fn finish_start(
        &mut self,
        ticket: String,
        result: Option<
            std::result::Result<
                client_protocol::StartWorkflowResponse,
                client_protocol::ClientOperationError,
            >,
        >,
    ) -> Operation {
        let pending = self.pending_starts.remove(&ticket).ok_or_else(|| Failure {
            status: STATUS_PROTOCOL,
            message: "unknown Temporal workflow start ticket".to_owned(),
        })?;

        if let Some(core) = self.core.as_ref() {
            // The task has sent its one-shot result before this join. The
            // bounded join therefore only drains its final bookkeeping and
            // cannot wait on a second network operation.
            let _ = core.tokio_handle().block_on(pending.task);
        } else {
            // Runtime closure is serialized through the same owner, but keep a
            // defensive abort path if an internal caller ever reaches this
            // method after Core ownership was removed.
            pending.task.abort();
        }

        let outcome = match result {
            Some(Ok(response)) => client_protocol::StartWorkflowOutcome::Accepted(response),
            Some(Err(error)) if error.uncertain_start() => {
                client_protocol::StartWorkflowOutcome::Unknown {
                    request_id: pending.request.request_id.clone(),
                    workflow_id: pending.request.workflow_id.clone(),
                }
            }
            Some(Err(error)) => client_protocol::StartWorkflowOutcome::Rejected(error),
            None => client_protocol::StartWorkflowOutcome::Unknown {
                request_id: pending.request.request_id.clone(),
                workflow_id: pending.request.workflow_id.clone(),
            },
        };
        client_protocol::encode_start_outcome(&outcome)
            .map(|encoded| encoded.into_bytes())
            .map_err(protocol_failure)
    }

    /// Aborts every in-flight start and returns handles that need cleanup.
    ///
    /// Explicit shutdown (`wait = true`) joins on the owner thread while the
    /// C stub has released the OCaml runtime lock.  GC fallback (`wait = false`)
    /// must not block the finalizer, so it returns the already-aborted handles
    /// for the dedicated cleanup thread to join before Core is released.  In
    /// either mode no task is detached while it still owns a Core connection.
    fn abort_pending_starts(&mut self, wait: bool) -> Vec<JoinHandle<()>> {
        let handle = self.core.as_ref().map(|core| core.tokio_handle());
        let tasks = self
            .pending_starts
            .drain()
            .map(|(_, pending)| pending.task)
            .collect::<Vec<_>>();
        for task in &tasks {
            task.abort();
        }
        if wait {
            if let Some(handle) = handle {
                handle.block_on(async {
                    for task in tasks {
                        let _ = task.await;
                    }
                });
            }
            Vec::new()
        } else {
            tasks
        }
    }

    /// Waits for one exact run with fixed `follow_runs = false` semantics.
    ///
    /// A continued-as-new close event is returned as a terminal outcome with
    /// successor metadata; the bridge never follows it implicitly.
    fn wait_workflow_json(&mut self, input: &[u8]) -> Operation {
        let text = decode_semantic_input(input)?;
        let request = client_protocol::decode_wait_request(text).map_err(protocol_failure)?;
        let connection = self.client.as_ref().cloned().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal client is not connected".to_owned(),
        })?;
        let handle = self
            .core
            .as_ref()
            .ok_or_else(|| Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal runtime is already closed".to_owned(),
            })?
            .tokio_handle();
        let response = handle
            .block_on(bounded_client_wait(client_protocol::wait_workflow(
                connection, request,
            )))
            .map_err(client_operation_failure)?;
        let response = response.ok_or_else(client_wait_not_ready)?;
        let encoded = client_protocol::encode_wait_response(&response).map_err(protocol_failure)?;
        Ok(encoded.into_bytes())
    }

    /// Constructs and validates one official workflow-only Core worker.
    ///
    /// Validation performs the namespace RPC before the worker enters the
    /// graph. Any construction or validation failure consumes the temporary
    /// worker and leaves the connected client available for a corrected retry.
    fn start_worker(&mut self, config: WorkerConfigInput) -> Operation {
        if self.worker.is_some() {
            return Err(Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal workflow worker is already running".to_owned(),
            });
        }
        let client = self.client.as_ref().cloned().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal client is not connected".to_owned(),
        })?;
        let worker_config = config.into_core()?;
        let core = self.core.as_ref().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal runtime is already closed".to_owned(),
        })?;
        let handle = core.tokio_handle();
        let worker = {
            // Core's synchronous constructor creates poll buffers with
            // `tokio::spawn`. Entering the owned executor here is therefore a
            // construction precondition, even though only validation exposes
            // an async function in Core's public API.
            let _runtime_guard = handle.enter();
            temporalio_sdk_core::init_worker(core, worker_config, client)
        }
        .map_err(|error| Failure {
            status: STATUS_WORKER,
            message: format!("Temporal workflow worker construction failed: {error}"),
        })?;

        if let Err(error) = handle.block_on(worker.validate()) {
            {
                // Core's synchronous shutdown initiation spawns its server
                // deregistration task, so it needs the same runtime context
                // as synchronous worker construction.
                let _runtime_guard = handle.enter();
                worker.initiate_shutdown();
            }
            handle.block_on(worker.finalize_shutdown());
            return Err(Failure {
                status: STATUS_WORKER,
                message: format!("Temporal workflow worker validation failed: {error}"),
            });
        }
        self.worker = Some(PollLanes::start(worker, &handle));
        Ok(Vec::new())
    }

    /// Gracefully finalizes the child worker once; absence is already closed.
    fn shutdown_worker(&mut self) -> Operation {
        let Some(worker) = self.worker.as_mut() else {
            return Ok(Vec::new());
        };
        let handle = self
            .core
            .as_ref()
            .expect("a live worker always has its parent runtime")
            .tokio_handle();
        {
            // `initiate_shutdown` synchronously calls `tokio::spawn`; keep
            // the guard narrower than the subsequent blocking finalization.
            let _runtime_guard = handle.enter();
            worker.initiate_shutdown();
        }
        handle
            .block_on(worker.join_poll_lanes())
            .map_err(poll_lane_failure)?;
        if !worker.can_finalize() {
            return Err(Failure {
                status: STATUS_OUTSTANDING_TASKS,
                message: "Temporal worker is draining outstanding workflow or activity tasks"
                    .to_owned(),
            });
        }
        let worker = self
            .worker
            .take()
            .expect("checked worker remains owned until terminal finalization");
        handle
            .block_on(worker.finalize())
            .map_err(worker_bridge_failure)?;
        Ok(Vec::new())
    }

    /// Takes one workflow activation from the Rust lane without waiting.
    fn try_poll_workflow(&mut self) -> Operation {
        let worker = self.worker.as_mut().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal worker is not running".to_owned(),
        })?;
        let activation = worker
            .try_take_workflow()
            .ok_or_else(not_ready)?
            .map_err(poll_lane_failure)?;
        let semantic = match workflow_protocol::activation_from_core(&activation) {
            Ok(semantic) => semantic,
            Err(error) => {
                self.reject_workflow_delivery(&activation.run_id)?;
                return Err(core_conversion_failure(error));
            }
        };
        let encoded = match workflow_protocol::encode_activation(&semantic) {
            Ok(encoded) => encoded,
            Err(error) => {
                self.reject_workflow_delivery(&activation.run_id)?;
                return Err(protocol_failure(error));
            }
        };
        retain_workflow_activation(&mut self.workflow_activations, semantic)?;
        Ok(encoded.into_bytes())
    }

    /// Waits for workflow-lane readiness without consuming the queued task.
    ///
    /// The native C stub invokes this while the OCaml runtime lock is released.
    /// A bounded timeout keeps a supervisor mailbox responsive to lifecycle
    /// commands when Core is quiet; callers retry after `STATUS_NOT_READY`.
    fn wait_workflow(&self) -> Operation {
        let worker = self.worker.as_ref().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal worker is not running".to_owned(),
        })?;
        match worker.wait_workflow() {
            ReadinessWait::Ready => Ok(Vec::new()),
            ReadinessWait::TimedOut => Err(Failure {
                status: STATUS_NOT_READY,
                message: "Temporal workflow readiness wait timed out; retry".to_owned(),
            }),
            ReadinessWait::Shutdown => Err(Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal workflow readiness wait ended during worker shutdown".to_owned(),
            }),
            ReadinessWait::Error(error) => Err(poll_lane_failure(error)),
        }
    }

    /// Strictly validates and submits one leased workflow completion.
    fn complete_workflow(&mut self, input: &[u8]) -> Operation {
        let text = decode_semantic_input(input)?;
        let semantic = workflow_protocol::decode_completion(text).map_err(protocol_failure)?;
        let activation = self
            .workflow_activations
            .get(&semantic.run_id)
            .ok_or_else(|| Failure {
                status: STATUS_PROTOCOL,
                message: "workflow completion does not match a leased activation".to_owned(),
            })?;
        let completion =
            workflow_protocol::completion_to_core_for_activation(activation, &semantic)
                .map_err(core_conversion_failure)?;
        let worker = self.worker.as_ref().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal worker is not running".to_owned(),
        })?;
        let handle = self
            .core
            .as_ref()
            .expect("worker retains parent runtime")
            .tokio_handle();
        handle
            .block_on(worker.complete_workflow(completion))
            .map_err(worker_bridge_failure)?;
        self.workflow_activations.remove(&semantic.run_id);
        Ok(Vec::new())
    }

    /// Revalidates and retires the exact workflow activation whose OCaml
    /// semantic decode failed after Rust handed off its lease.
    fn reject_polled_workflow(&mut self, input: &[u8]) -> Operation {
        let run_id = workflow_rejection_run_id(&self.workflow_activations, input)?;
        let rejection = self.reject_workflow_delivery(&run_id);
        // WorkerBridge retires the ledger debt even when Core rejects the
        // generated failure. Remove the matching semantic value on the same
        // path so an error cannot leave a stale second ownership record.
        self.workflow_activations.remove(&run_id);
        rejection?;
        Ok(Vec::new())
    }

    /// Takes one remote activity task from its independent lane without waiting.
    fn try_poll_activity(&mut self) -> Operation {
        let worker = self.worker.as_mut().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal worker is not running".to_owned(),
        })?;
        let task = worker
            .try_take_activity()
            .ok_or_else(not_ready)?
            .map_err(poll_lane_failure)?;
        let semantic = match activity_protocol::task_from_core(&task) {
            Ok(semantic) => semantic,
            Err(error) => {
                self.reject_activity_delivery(&task.task_token)?;
                return Err(core_conversion_failure(error));
            }
        };
        match activity_protocol::encode_task(&semantic) {
            Ok(encoded) => {
                retain_activity_task(&mut self.activity_tasks, task.task_token.clone(), semantic);
                Ok(encoded.into_bytes())
            }
            Err(error) => {
                self.reject_activity_delivery(&task.task_token)?;
                Err(protocol_failure(error))
            }
        }
    }

    /// Waits for remote-activity-lane readiness without consuming its task.
    ///
    /// As with workflow readiness, this is a blocking native call with a
    /// bounded duration so the owner supervisor can process shutdown messages.
    fn wait_activity(&self) -> Operation {
        let worker = self.worker.as_ref().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal worker is not running".to_owned(),
        })?;
        match worker.wait_activity() {
            ReadinessWait::Ready => Ok(Vec::new()),
            ReadinessWait::TimedOut => Err(Failure {
                status: STATUS_NOT_READY,
                message: "Temporal activity readiness wait timed out; retry".to_owned(),
            }),
            ReadinessWait::Shutdown => Err(Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal activity readiness wait ended during worker shutdown".to_owned(),
            }),
            ReadinessWait::Error(error) => Err(poll_lane_failure(error)),
        }
    }

    /// Fails and retires a workflow activation that was never exposed to OCaml.
    fn reject_workflow_delivery(&self, run_id: &str) -> std::result::Result<(), Failure> {
        let worker = self.worker.as_ref().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal worker is not running".to_owned(),
        })?;
        let handle = self
            .core
            .as_ref()
            .expect("worker retains parent runtime")
            .tokio_handle();
        handle
            .block_on(worker.reject_workflow_delivery(run_id))
            .map_err(worker_bridge_failure)
    }

    /// Fails and retires an activity task that was never exposed to OCaml.
    fn reject_activity_delivery(&self, task_token: &[u8]) -> std::result::Result<(), Failure> {
        let worker = self.worker.as_ref().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal worker is not running".to_owned(),
        })?;
        let handle = self
            .core
            .as_ref()
            .expect("worker retains parent runtime")
            .tokio_handle();
        handle
            .block_on(worker.reject_activity_delivery(task_token))
            .map_err(worker_bridge_failure)
    }

    /// Strictly validates and submits one leased remote-activity completion.
    fn complete_activity(&mut self, input: &[u8]) -> Operation {
        let text = decode_semantic_input(input)?;
        let semantic = activity_protocol::decode_completion(text).map_err(protocol_failure)?;
        let task_token =
            activity_protocol::decode_token(&semantic.task_token).map_err(protocol_failure)?;
        let completion =
            activity_protocol::completion_to_core(&semantic).map_err(core_conversion_failure)?;
        let worker = self.worker.as_ref().ok_or_else(|| Failure {
            status: STATUS_INVALID_STATE,
            message: "Temporal worker is not running".to_owned(),
        })?;
        let handle = self
            .core
            .as_ref()
            .expect("worker retains parent runtime")
            .tokio_handle();
        handle
            .block_on(worker.complete_activity(completion))
            .map_err(worker_bridge_failure)?;
        retire_activity_semantics(&mut self.activity_tasks, &task_token);
        Ok(Vec::new())
    }

    /// Revalidates Rust-produced task JSON and retires the exact opaque token
    /// when OCaml cannot represent the task after lease handoff.
    fn reject_polled_activity(&mut self, input: &[u8]) -> Operation {
        let task_token = activity_rejection_token(&self.activity_tasks, input)?;
        let rejection = self.reject_activity_delivery(&task_token);
        // The native ledger retires the completion debt even if Core rejects
        // its generated failure, so the semantic correlation state must follow
        // the same one-shot ownership transition.
        retire_activity_semantics(&mut self.activity_tasks, &task_token);
        rejection?;
        Ok(Vec::new())
    }

    /// Drops the client only after its worker child is absent.
    fn disconnect_client(&mut self) -> Operation {
        if self.worker.is_some() {
            return Err(Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal client cannot disconnect while its worker is running".to_owned(),
            });
        }
        if !self.pending_starts.is_empty() {
            return Err(Failure {
                status: STATUS_INVALID_STATE,
                message: "Temporal client cannot disconnect while workflow starts are pending"
                    .to_owned(),
            });
        }
        self.client.take();
        Ok(Vec::new())
    }

    /// Transfers Core to its cleanup thread and optionally waits for disposal.
    ///
    /// Explicit close waits while the OCaml lock is released. GC fallback does
    /// not wait on this caller, so a custom-block finalizer never stalls the
    /// collector; the dedicated cleanup thread joins any aborted start tasks.
    fn close(mut self, wait: bool) -> Status {
        let pending_start_tasks = self.abort_pending_starts(wait);
        let Some(core) = self.core.take() else {
            return STATUS_OK;
        };
        let (completed, receiver) = if wait {
            let (sender, receiver) = sync_channel(1);
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        };
        let message = RuntimeCleanup {
            core,
            client: self.client.take(),
            worker: self.worker.take(),
            pending_start_tasks,
            completed,
        };

        if let Err(error) = self.cleanup.send(message) {
            // The receiver only exits after a message, so this indicates a
            // defect in the cleanup thread itself. Reclaim on this thread to
            // preserve the no-leak guarantee even on that defensive path.
            let message = error.0;
            drop_runtime_graph(
                message.core,
                message.client,
                message.worker,
                message.pending_start_tasks,
            );
            RUNTIMES_CLEANED.fetch_add(1, Ordering::Release);
            return STATUS_INTERNAL;
        }

        match receiver {
            Some(receiver) => receiver.recv().unwrap_or(STATUS_INTERNAL),
            None => STATUS_OK,
        }
    }
}

/// Drops Core away from OCaml's collector and reports completion when asked.
fn run_runtime_cleanup(receiver: Receiver<RuntimeCleanup>) {
    let Ok(message) = receiver.recv() else {
        return;
    };
    let RuntimeCleanup {
        core,
        client,
        worker,
        pending_start_tasks,
        completed,
    } = message;
    let status = if catch_unwind(AssertUnwindSafe(|| {
        drop_runtime_graph(core, client, worker, pending_start_tasks)
    }))
    .is_ok()
    {
        STATUS_OK
    } else {
        STATUS_PANIC
    };
    // Release publishes completion after Core's destructor has returned. The
    // matching Acquire load is used only by the isolated ownership test.
    RUNTIMES_CLEANED.fetch_add(1, Ordering::Release);
    if let Some(completed) = completed {
        let _ = completed.send(status);
    }
}

/// Releases aborted start tasks, then worker, client, and Core on the runtime
/// cleanup thread.  Awaiting the transferred handles is essential: dropping a
/// Tokio [`JoinHandle`] after `abort` would detach its task, allowing the task
/// to retain a cloned `Connection` beyond the lifetime of the runtime graph.
fn drop_runtime_graph(
    core: CoreRuntime,
    client: Option<Connection>,
    worker: Option<PollLanes>,
    pending_start_tasks: Vec<JoinHandle<()>>,
) {
    let handle = core.tokio_handle();
    handle.block_on(async {
        for task in pending_start_tasks {
            let _ = task.await;
        }
    });

    if let Some(mut worker) = worker {
        {
            // Cleanup runs on a plain OS thread, so explicitly enter Core's
            // executor before its synchronous shutdown code spawns a task.
            let _runtime_guard = handle.enter();
            worker.initiate_shutdown();
        }
        let _ = handle.block_on(worker.join_poll_lanes());
        if worker.can_finalize() {
            let _ = handle.block_on(worker.finalize());
        }
    }
    drop(client);
    drop(core);
}

/// Maps private worker state-machine errors to stable ABI failures.
fn worker_bridge_failure(error: WorkerBridgeError) -> Failure {
    let status = match error {
        WorkerBridgeError::OutstandingTasks(_) => STATUS_OUTSTANDING_TASKS,
        _ => STATUS_WORKER,
    };
    Failure {
        status,
        message: format!("Temporal worker bridge failed: {error:?}"),
    }
}

/// Maps the low-level client adapter to a stable ABI status and a structured
/// privacy-safe error body.  In particular, Temporal's AlreadyStarted result
/// is machine-readable JSON rather than a server diagnostic string.
fn client_operation_failure(error: client_protocol::ClientOperationError) -> Failure {
    let status = match &error {
        client_protocol::ClientOperationError::AlreadyStarted { .. } => STATUS_ALREADY_STARTED,
        client_protocol::ClientOperationError::Rpc { .. } => STATUS_CONNECTION,
        client_protocol::ClientOperationError::Core(_) => STATUS_PROTOCOL,
    };
    Failure {
        status,
        message: error.to_json(),
    }
}

/// Maps a fatal background poll-lane error without exposing Core internals.
fn poll_lane_failure(error: PollLaneError) -> Failure {
    Failure {
        status: STATUS_WORKER,
        message: format!("Temporal worker poll lane failed: {error:?}"),
    }
}

/// Constructs the stable result used by empty non-blocking poll lanes.
fn not_ready() -> Failure {
    Failure {
        status: STATUS_NOT_READY,
        message: "no Temporal task is ready".to_owned(),
    }
}

/// Reports an exact-run wait that must be retried without exposing a fake
/// terminal workflow outcome to the OCaml supervisor.
fn client_wait_not_ready() -> Failure {
    Failure {
        status: STATUS_NOT_READY,
        message: "Temporal workflow has not reached a close event; retry".to_owned(),
    }
}

/// Converts strict semantic-protocol failures without reflecting user input.
fn protocol_failure(_error: crate::protocol::ProtocolError) -> Failure {
    Failure {
        status: STATUS_PROTOCOL,
        message: "Temporal semantic JSON failed validation".to_owned(),
    }
}

/// Converts protobuf-boundary failures to a privacy-safe protocol status.
fn core_conversion_failure(error: workflow_protocol::CoreConversionError) -> Failure {
    Failure {
        status: STATUS_PROTOCOL,
        message: format!(
            "Temporal Core task cannot be represented safely: {}",
            error.message
        ),
    }
}

/// Borrows one bounded UTF-8 semantic document for synchronous decoding.
fn decode_semantic_input(input: &[u8]) -> std::result::Result<&str, Failure> {
    if input.len() > crate::protocol::MAX_DOCUMENT_BYTES {
        return Err(Failure {
            status: STATUS_PROTOCOL,
            message: "Temporal semantic JSON exceeds the document limit".to_owned(),
        });
    }
    std::str::from_utf8(input).map_err(|_| Failure {
        status: STATUS_PROTOCOL,
        message: "Temporal semantic JSON is not UTF-8".to_owned(),
    })
}

/// Records one workflow activation without allowing a duplicate poll to
/// replace the semantic document already leased to the language runtime.
fn retain_workflow_activation(
    pending: &mut HashMap<String, workflow_protocol::Activation>,
    activation: workflow_protocol::Activation,
) -> std::result::Result<(), Failure> {
    match pending.entry(activation.run_id.clone()) {
        Entry::Vacant(entry) => {
            entry.insert(activation);
            Ok(())
        }
        Entry::Occupied(_) => Err(Failure {
            status: STATUS_INTERNAL,
            message: "workflow activation lease was duplicated".to_owned(),
        }),
    }
}

/// Extracts a workflow rejection identity only when the complete submitted
/// document equals the Rust semantic value retained at the original handoff.
fn workflow_rejection_run_id(
    pending: &HashMap<String, workflow_protocol::Activation>,
    input: &[u8],
) -> std::result::Result<String, Failure> {
    let text = decode_semantic_input(input)?;
    let semantic = workflow_protocol::decode_activation(text).map_err(protocol_failure)?;
    match pending.get(&semantic.run_id) {
        Some(leased) if leased == &semantic => Ok(semantic.run_id),
        Some(_) => Err(Failure {
            status: STATUS_PROTOCOL,
            message: "workflow rejection does not match the leased activation".to_owned(),
        }),
        None => Err(Failure {
            status: STATUS_PROTOCOL,
            message: "workflow rejection does not match a leased activation".to_owned(),
        }),
    }
}

/// Retains every semantic activity handoff under Core's opaque token. Multiple
/// cancellation updates may share that token and must not overwrite each other.
fn retain_activity_task(
    pending: &mut HashMap<Vec<u8>, Vec<activity_protocol::ActivityTask>>,
    task_token: Vec<u8>,
    task: activity_protocol::ActivityTask,
) {
    pending.entry(task_token).or_default().push(task);
}

/// Clears all handoff documents for one activity token when its single native
/// completion debt is retired. Cancellation updates add documents but, by
/// Temporal contract, never add another ledger obligation for the same token.
fn retire_activity_semantics(
    pending: &mut HashMap<Vec<u8>, Vec<activity_protocol::ActivityTask>>,
    task_token: &[u8],
) {
    pending.remove(task_token);
}

/// Extracts the canonical opaque activity token only when the complete task
/// document matches one retained at a successful Rust-to-OCaml handoff.
fn activity_rejection_token(
    pending: &HashMap<Vec<u8>, Vec<activity_protocol::ActivityTask>>,
    input: &[u8],
) -> std::result::Result<Vec<u8>, Failure> {
    let text = decode_semantic_input(input)?;
    let semantic = activity_protocol::decode_task(text).map_err(protocol_failure)?;
    let task_token =
        activity_protocol::decode_token(&semantic.task_token).map_err(protocol_failure)?;
    match pending.get(&task_token) {
        Some(tasks) if tasks.contains(&semantic) => Ok(task_token),
        Some(_) => Err(Failure {
            status: STATUS_PROTOCOL,
            message: "activity rejection does not match a leased task".to_owned(),
        }),
        None => Err(Failure {
            status: STATUS_PROTOCOL,
            message: "activity rejection does not match a leased task token".to_owned(),
        }),
    }
}

impl WorkerConfigInput {
    /// Performs bridge-owned bounds checks and constructs official Core config.
    fn into_core(self) -> std::result::Result<WorkerConfig, Failure> {
        validate_identifier("namespace", &self.namespace)?;
        validate_identifier("task_queue", &self.task_queue)?;
        validate_identifier("build_id", &self.build_id)?;
        validate_count("max_cached_workflows", self.max_cached_workflows, true)?;
        validate_count(
            "max_outstanding_workflow_tasks",
            self.max_outstanding_workflow_tasks,
            false,
        )?;
        validate_count(
            "max_concurrent_workflow_task_polls",
            self.max_concurrent_workflow_task_polls,
            false,
        )?;
        if self.graceful_shutdown_timeout_ms > MAX_GRACEFUL_SHUTDOWN_MS {
            return Err(Failure {
                status: STATUS_CONFIGURATION,
                message: format!("graceful_shutdown_timeout_ms exceeds {MAX_GRACEFUL_SHUTDOWN_MS}"),
            });
        }

        WorkerConfig::builder()
            .namespace(self.namespace)
            .task_queue(self.task_queue)
            .max_cached_workflows(self.max_cached_workflows as usize)
            .max_outstanding_workflow_tasks(self.max_outstanding_workflow_tasks as usize)
            .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(
                self.max_concurrent_workflow_task_polls as usize,
            ))
            .graceful_shutdown_period(Duration::from_millis(self.graceful_shutdown_timeout_ms))
            .versioning_strategy(WorkerVersioningStrategy::None {
                build_id: self.build_id,
            })
            .max_outstanding_activities(DEFAULT_MAX_OUTSTANDING_ACTIVITIES)
            .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(
                DEFAULT_MAX_CONCURRENT_ACTIVITY_POLLS,
            ))
            .task_types(crate::worker_bridge::bridge_task_types())
            .build()
            .map_err(|message| Failure {
                status: STATUS_CONFIGURATION,
                message: format!("Temporal workflow worker configuration is invalid: {message}"),
            })
    }
}

/// Validates only bridge-owned string invariants before Core sees the value.
fn validate_identifier(name: &str, value: &str) -> std::result::Result<(), Failure> {
    if value.is_empty() {
        return Err(Failure {
            status: STATUS_CONFIGURATION,
            message: format!("{name} must not be empty"),
        });
    }
    if value.len() > MAX_TRANSPORT_STRING_BYTES {
        return Err(Failure {
            status: STATUS_CONFIGURATION,
            message: format!("{name} exceeds {MAX_TRANSPORT_STRING_BYTES} UTF-8 bytes"),
        });
    }
    Ok(())
}

/// Validates a bounded worker count, optionally allowing zero to disable it.
fn validate_count(name: &str, value: u32, allow_zero: bool) -> std::result::Result<(), Failure> {
    if (!allow_zero && value == 0) || value > MAX_WORKER_COUNT {
        return Err(Failure {
            status: STATUS_CONFIGURATION,
            message: format!(
                "{name} must be between {} and {MAX_WORKER_COUNT}",
                u8::from(!allow_zero)
            ),
        });
    }
    Ok(())
}

/// Byte-producing operation accepted by the shared panic/ownership wrapper.
type Operation = std::result::Result<Vec<u8>, Failure>;

/// Constructs the only valid successful result shape.
fn success(value: Vec<u8>) -> Result {
    Result {
        status: STATUS_OK,
        value: Buffer::from_vec(value),
        error: Buffer::default(),
    }
}

/// Constructs the only valid failed result shape with UTF-8 diagnostic bytes.
fn failure(status: Status, message: impl Into<String>) -> Result {
    Result {
        status,
        value: Buffer::default(),
        error: Buffer::from_vec(message.into().into_bytes()),
    }
}

/// Initializes output, contains panics, and commits one fully formed result.
///
/// Writing the empty result before executing user logic ensures every non-null
/// output is safe to free even when the operation panics.
unsafe fn invoke(output: *mut Result, operation: impl FnOnce() -> Operation) -> Status {
    if output.is_null() {
        return STATUS_INVALID_ARGUMENT;
    }

    // SAFETY: The non-null output pointer is required by the ABI contract to
    // be valid and writable. `ptr::write` also permits uninitialized storage.
    unsafe { ptr::write(output, Result::default()) };

    let result = match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(Ok(value)) => success(value),
        Ok(Err(error)) => failure(error.status, error.message),
        Err(_) => failure(
            STATUS_PANIC,
            "Rust panic contained at the native ABI boundary",
        ),
    };
    let status = result.status;

    // SAFETY: The pointer was validated above and no reference to its previous
    // (empty) value is retained.
    unsafe { ptr::write(output, result) };
    status
}

/// Decodes one bounded strict UTF-8 JSON lifecycle configuration.
///
/// # Safety
///
/// A nonzero `input_len` requires `input` to identify that many readable bytes
/// for the duration of this synchronous parse.
unsafe fn decode_config<T: for<'de> Deserialize<'de>>(
    input: *const u8,
    input_len: usize,
) -> std::result::Result<T, Failure> {
    if input_len > MAX_LIFECYCLE_CONFIG_BYTES {
        return Err(Failure {
            status: STATUS_CONFIGURATION,
            message: format!("lifecycle configuration exceeds {MAX_LIFECYCLE_CONFIG_BYTES} bytes"),
        });
    }
    if input_len > 0 && input.is_null() {
        return Err(Failure {
            status: STATUS_INVALID_ARGUMENT,
            message: "configuration input is null but input_len is nonzero".to_owned(),
        });
    }
    let bytes = if input_len == 0 {
        &[]
    } else {
        // SAFETY: The caller promises the readable span documented above, and
        // the nonempty null case was rejected before constructing this slice.
        unsafe { std::slice::from_raw_parts(input, input_len) }
    };
    serde_json::from_slice(bytes).map_err(|error| Failure {
        status: STATUS_CONFIGURATION,
        message: format!("invalid lifecycle configuration JSON: {error}"),
    })
}

/// Borrows one native input span after enforcing a caller-selected ceiling.
///
/// # Safety
///
/// A nonzero length requires a readable input allocation for this call.
unsafe fn input_span<'a>(
    input: *const u8,
    input_len: usize,
    maximum: usize,
) -> std::result::Result<&'a [u8], Failure> {
    if input_len > maximum {
        return Err(Failure {
            status: STATUS_PROTOCOL,
            message: format!("native input exceeds {maximum} bytes"),
        });
    }
    if input_len != 0 && input.is_null() {
        return Err(Failure {
            status: STATUS_INVALID_ARGUMENT,
            message: "input is null but input_len is nonzero".to_owned(),
        });
    }
    if input_len == 0 {
        Ok(&[])
    } else {
        // SAFETY: The caller owns the readable span documented above.
        Ok(unsafe { std::slice::from_raw_parts(input, input_len) })
    }
}

/// Reclaims one bridge allocation and resets the buffer to canonical empty.
unsafe fn free_buffer(buffer: &mut Buffer) {
    if buffer.ptr.is_null() {
        buffer.len = 0;
        return;
    }

    let allocation = ptr::slice_from_raw_parts_mut(buffer.ptr, buffer.len);
    // SAFETY: Only buffers created by `Buffer::from_vec` may be passed to this
    // function. The exact pointer and length reconstruct its boxed slice.
    drop(unsafe { Box::from_raw(allocation) });
    *buffer = Buffer::default();
}

/// Negotiate ABI version 1.
///
/// # Safety
///
/// `output` must be null or point to writable storage for one [`Result`]. It
/// must not contain live bridge-owned allocations when this function starts.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_check_abi_version(
    requested_version: u32,
    output: *mut Result,
) -> Status {
    // SAFETY: `invoke` validates null and otherwise relies on the documented
    // output-pointer contract of this exported function.
    unsafe {
        invoke(output, || {
            if requested_version == ABI_VERSION {
                Ok(Vec::new())
            } else {
                Err(Failure {
                    status: STATUS_ABI_MISMATCH,
                    message: format!(
                        "unsupported ABI version {requested_version}; expected {ABI_VERSION}"
                    ),
                })
            }
        })
    }
}

/// Copy bytes into a Rust-owned result buffer.
///
/// This operation exercises the ownership contract used later for encoded
/// workflow activations and completions.
///
/// # Safety
///
/// When `input_len` is nonzero, `input` must point to that many readable bytes.
/// `output` follows the same contract as
/// [`ocaml_temporal_core_v1_check_abi_version`] and must not overlap `input`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_echo(
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    // SAFETY: `invoke` validates the output pointer. Input is checked for null
    // before constructing the slice; remaining validity is the caller's ABI
    // obligation documented above.
    unsafe {
        invoke(output, || {
            if input_len == 0 {
                return Ok(Vec::new());
            }
            if input.is_null() {
                return Err(Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "input is null but input_len is nonzero".to_owned(),
                });
            }

            // SAFETY: The caller guarantees a readable input allocation of
            // `input_len` bytes and the null case was rejected above.
            Ok(std::slice::from_raw_parts(input, input_len).to_vec())
        })
    }
}

/// Wait on the native side for bridge conformance testing.
///
/// This bounded operation exists to prove that language bindings release
/// their runtime lock around blocking ABI calls. It is not a workflow timer.
///
/// # Safety
///
/// `output` follows the same contract as
/// [`ocaml_temporal_core_v1_check_abi_version`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_conformance_wait_ms(
    milliseconds: u32,
    output: *mut Result,
) -> Status {
    // SAFETY: The output-pointer contract is forwarded unchanged to `invoke`.
    unsafe {
        invoke(output, || {
            if milliseconds > 1_000 {
                return Err(Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "conformance wait cannot exceed 1000 ms".to_owned(),
                });
            }
            std::thread::sleep(Duration::from_millis(u64::from(milliseconds)));
            Ok(Vec::new())
        })
    }
}

/// Create the native runtime that will own later Core clients and workers.
///
/// On success, `runtime` receives one owned opaque handle. The caller must
/// eventually pass that same slot to [`ocaml_temporal_core_v1_runtime_free`].
///
/// # Safety
///
/// `runtime` must be null or point to writable storage for one runtime pointer.
/// `output` follows the result contract of
/// [`ocaml_temporal_core_v1_check_abi_version`]. A non-null runtime slot must
/// not already contain a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_runtime_new(
    runtime: *mut *mut Runtime,
    output: *mut Result,
) -> Status {
    // Canonicalize any writable handle slot even when the independent result
    // pointer is invalid. A caller can therefore inspect a known null value on
    // every failing return instead of retaining indeterminate ownership state.
    if !runtime.is_null() {
        // SAFETY: A non-null runtime argument promises writable pointer storage.
        unsafe { ptr::write(runtime, ptr::null_mut()) };
    }
    if output.is_null() {
        return STATUS_INVALID_ARGUMENT;
    }
    if runtime.is_null() {
        // SAFETY: The result pointer was checked above and the closure does
        // not inspect the missing runtime slot.
        return unsafe {
            invoke(output, || {
                Err(Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime output pointer is null".to_owned(),
                })
            })
        };
    }

    // SAFETY: Both output locations were validated above.
    unsafe {
        invoke(output, || {
            let options = RuntimeOptions::builder()
                .build()
                .map_err(|message| Failure {
                    status: STATUS_INTERNAL,
                    message: format!("could not configure Temporal Core runtime: {message}"),
                })?;
            let core =
                CoreRuntime::new(options, TokioRuntimeBuilder::default()).map_err(|error| {
                    Failure {
                        status: STATUS_INTERNAL,
                        message: format!("could not create Temporal Core runtime: {error}"),
                    }
                })?;
            let owned = Box::into_raw(Box::new(Runtime::new(core)?));

            // SAFETY: The runtime slot remains exclusively owned by this call
            // until it returns and was validated before invoking the closure.
            ptr::write(runtime, owned);
            Ok(Vec::new())
        })
    }
}

/// Connect the runtime graph's single official Temporal client from strict
/// JSON. Successful return publishes the client; every failure leaves the
/// graph without a newly constructed child.
///
/// # Safety
///
/// `runtime` must be a live handle created by
/// [`ocaml_temporal_core_v1_runtime_new`] and exclusively owned for this call.
/// `input` follows [`decode_config`]'s byte-span contract, while `output`
/// follows [`ocaml_temporal_core_v1_check_abi_version`]'s result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_client_connect_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    // SAFETY: `invoke` validates output. The closure checks the opaque handle
    // before dereferencing and forwards the documented input span to decoding.
    unsafe {
        invoke(output, || {
            let runtime = runtime.as_mut().ok_or_else(|| Failure {
                status: STATUS_INVALID_ARGUMENT,
                message: "runtime pointer is null".to_owned(),
            })?;
            let config = decode_config::<ClientConfig>(input, input_len)?;
            runtime.connect_client(config)
        })
    }
}

/// Start one dynamically named workflow through the connected Core client.
///
/// The input is a strict client-start JSON document.  The successful value is
/// a strict execution-reference response.  Temporal AlreadyStarted failures
/// use `STATUS_ALREADY_STARTED` and place a closed JSON error body in `error`.
///
/// # Safety
///
/// `runtime` must be a live, exclusively owned runtime handle.  The input
/// span is borrowed only for this synchronous call and `output` follows the
/// standard initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_client_start_workflow_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .start_workflow_json(input)
        })
    }
}

/// Begin one workflow start and return an opaque asynchronous ticket.
///
/// The native call only validates and admits the request, then schedules the
/// Tokio operation. Poll or wait on the returned ticket to obtain a terminal
/// accepted/rejected/unknown outcome. A pending entry is owned by the runtime
/// supervisor and is never accessed concurrently from a Tokio task.
///
/// # Safety
///
/// `runtime` must be a live, exclusively owned runtime handle. The input span
/// is borrowed only for this admission call and `output` follows the standard
/// initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_client_begin_start_workflow_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .begin_start_workflow_json(input)
        })
    }
}

/// Poll one asynchronous workflow-start ticket without waiting.
///
/// `STATUS_NOT_READY` is an expected result while the RPC remains in flight.
/// Once terminal, the successful value is a strict start-outcome document and
/// the ticket is retired, so a second poll reports an unknown-ticket protocol
/// error rather than duplicating or replaying the result.
///
/// # Safety
///
/// The runtime, input, and output contracts match
/// [`ocaml_temporal_core_v1_client_begin_start_workflow_json`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_client_poll_start_workflow_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .poll_start_workflow_json(input)
        })
    }
}

/// Wait for one asynchronous workflow-start ticket for a bounded interval.
///
/// The wait never blocks the OCaml runtime lock: the C stub releases that lock
/// around this ABI call, and the owner Domain regains control after at most
/// [`START_WAIT_TIMEOUT`]. A timeout is returned as `STATUS_NOT_READY`; the
/// caller can continue servicing its mailbox and wait again.
///
/// # Safety
///
/// The runtime, input, and output contracts match
/// [`ocaml_temporal_core_v1_client_begin_start_workflow_json`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_client_wait_start_workflow_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .wait_start_workflow_json(input)
        })
    }
}

/// Wait for one exact workflow run without following continued-as-new.
///
/// The successful value is a strict terminal-outcome document.  A
/// continued-as-new event is returned with its successor metadata instead of
/// being followed by the Rust bridge.
///
/// # Safety
///
/// `runtime` must be a live, exclusively owned runtime handle.  The input
/// span is borrowed only for this synchronous call and `output` follows the
/// standard initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_client_wait_workflow_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .wait_workflow_json(input)
        })
    }
}

/// Start and validate one workflow-only official Core worker from strict JSON.
/// Success is not exposed until the namespace validation RPC completes.
///
/// # Safety
///
/// The runtime, input, and output contracts match
/// [`ocaml_temporal_core_v1_client_connect_json`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_worker_start_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    // SAFETY: `invoke` and `decode_config` enforce their respective pointer
    // preconditions before the runtime graph is mutated.
    unsafe {
        invoke(output, || {
            let runtime = runtime.as_mut().ok_or_else(|| Failure {
                status: STATUS_INVALID_ARGUMENT,
                message: "runtime pointer is null".to_owned(),
            })?;
            let config = decode_config::<WorkerConfigInput>(input, input_len)?;
            runtime.start_worker(config)
        })
    }
}

/// Non-blockingly take one validated workflow activation JSON document.
///
/// # Safety
///
/// `runtime` must be a live exclusively owned runtime handle and `output`
/// must satisfy the standard initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_worker_try_poll_workflow(
    runtime: *mut Runtime,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .try_poll_workflow()
        })
    }
}

/// Wait for workflow-lane readiness without consuming the queued activation.
///
/// The caller must release the OCaml runtime lock around this operation. The
/// wait is bounded so a supervisor mailbox can regain control and process a
/// shutdown request even when Core has no task to deliver.
///
/// # Safety
///
/// `runtime` must be a live exclusively owned runtime handle and `output` must
/// satisfy the standard initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_worker_wait_workflow(
    runtime: *mut Runtime,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            runtime
                .as_ref()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .wait_workflow()
        })
    }
}

/// Validate and submit one workflow completion JSON document.
///
/// # Safety
///
/// The runtime and output contracts match the workflow poll operation. A
/// nonzero input length requires that many readable bytes for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_worker_complete_workflow_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .complete_workflow(input)
        })
    }
}

/// Reject one Rust-produced workflow activation that OCaml could not decode.
///
/// Rust strictly reparses the original poll document and requires its complete
/// semantic activation to equal the one-shot value retained for that run.
///
/// # Safety
///
/// The runtime and output contracts match the workflow poll operation. A
/// nonzero input length requires that many readable bytes for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_worker_reject_workflow_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .reject_polled_workflow(input)
        })
    }
}

/// Non-blockingly take one validated remote activity task JSON document.
///
/// # Safety
///
/// `runtime` must be a live exclusively owned runtime handle and `output`
/// must satisfy the standard initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_worker_try_poll_activity(
    runtime: *mut Runtime,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .try_poll_activity()
        })
    }
}

/// Wait for remote-activity-lane readiness without consuming the queued task.
///
/// The C binding releases the OCaml runtime lock while this bounded native wait
/// runs. A timeout is returned as `STATUS_NOT_READY` so the owner supervisor
/// can service other mailbox messages and retry.
///
/// # Safety
///
/// `runtime` must be a live exclusively owned runtime handle and `output` must
/// satisfy the standard initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_worker_wait_activity(
    runtime: *mut Runtime,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            runtime
                .as_ref()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .wait_activity()
        })
    }
}

/// Validate and submit one remote activity completion JSON document.
///
/// # Safety
///
/// The runtime and output contracts match the activity poll operation. A
/// nonzero input length requires that many readable bytes for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_worker_complete_activity_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .complete_activity(input)
        })
    }
}

/// Reject one Rust-produced remote activity task that OCaml could not decode.
///
/// The closed decoder requires complete equality with a retained handoff task
/// before recovering its bounded opaque token. The native ledger makes
/// rejection one-shot and refuses unknown or repeated tokens.
///
/// # Safety
///
/// The runtime and output contracts match the activity poll operation. A
/// nonzero input length requires that many readable bytes for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_worker_reject_activity_json(
    runtime: *mut Runtime,
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    unsafe {
        invoke(output, || {
            let input = input_span(input, input_len, crate::protocol::MAX_DOCUMENT_BYTES)?;
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .reject_polled_activity(input)
        })
    }
}

/// Gracefully stop and remove the runtime graph's workflow worker.
///
/// Repeating this operation after success is safe and returns success.
///
/// # Safety
///
/// `runtime` must be a live exclusively owned runtime handle. `output` follows
/// the standard initialized-result contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_worker_shutdown(
    runtime: *mut Runtime,
    output: *mut Result,
) -> Status {
    // SAFETY: `invoke` validates the result before the closure checks and uses
    // the opaque runtime pointer.
    unsafe {
        invoke(output, || {
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .shutdown_worker()
        })
    }
}

/// Remove the connected client after its worker child has stopped.
///
/// Repeating this operation when no client exists is safe and returns success.
///
/// # Safety
///
/// The pointer contracts match [`ocaml_temporal_core_v1_worker_shutdown`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_client_disconnect(
    runtime: *mut Runtime,
    output: *mut Result,
) -> Status {
    // SAFETY: `invoke` validates output and the opaque handle is checked for
    // null before producing the unique mutable reference required by graph use.
    unsafe {
        invoke(output, || {
            runtime
                .as_mut()
                .ok_or_else(|| Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime pointer is null".to_owned(),
                })?
                .disconnect_client()
        })
    }
}

/// Destroy one native runtime and clear the caller's slot.
///
/// Passing the same slot again after a successful call is safe because the
/// first call stores null before dropping the owner.
///
/// # Safety
///
/// `runtime` must be null or point to a slot initialized by
/// [`ocaml_temporal_core_v1_runtime_new`]. The slot must not be accessed
/// concurrently, and all future child handles must be closed first.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_runtime_free(runtime: *mut *mut Runtime) -> Status {
    if runtime.is_null() {
        return STATUS_INVALID_ARGUMENT;
    }

    let outcome = catch_unwind(AssertUnwindSafe(|| {
        // Clear first so even a defensive panic during destruction cannot
        // invite a second attempt to free the same allocation.
        // SAFETY: The caller guarantees exclusive writable access to the slot.
        let owned = unsafe { ptr::replace(runtime, ptr::null_mut()) };
        if owned.is_null() {
            STATUS_OK
        } else {
            // SAFETY: Non-null values in this slot originate from `Box::into_raw`
            // in `runtime_new` and have not been reclaimed previously.
            let runtime = unsafe { Box::from_raw(owned) };
            runtime.close(true)
        }
    }));

    match outcome {
        Ok(status) => status,
        Err(_) => STATUS_PANIC,
    }
}

/// Transfer a runtime to its cleanup thread without waiting for destruction.
///
/// This is reserved for the OCaml custom-block finalizer. Normal supervisor
/// shutdown uses [`ocaml_temporal_core_v1_runtime_free`] and waits while the
/// OCaml runtime lock is released.
///
/// # Safety
///
/// `runtime` has the same exclusive slot contract as
/// [`ocaml_temporal_core_v1_runtime_free`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_runtime_dispose(
    runtime: *mut *mut Runtime,
) -> Status {
    if runtime.is_null() {
        return STATUS_INVALID_ARGUMENT;
    }

    let outcome = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: The caller guarantees exclusive writable access to the slot.
        let owned = unsafe { ptr::replace(runtime, ptr::null_mut()) };
        if owned.is_null() {
            STATUS_OK
        } else {
            // SAFETY: The pointer was created by `runtime_new` and is consumed
            // exactly once by this pointer-to-pointer operation.
            let runtime = unsafe { Box::from_raw(owned) };
            runtime.close(false)
        }
    }));

    match outcome {
        Ok(status) => status,
        Err(_) => STATUS_PANIC,
    }
}

/// Release both owned buffers in a result and reset it to the empty state.
///
/// Repeated calls with the same result object are safe. Copying a live result
/// and freeing both copies is forbidden by the ownership contract.
///
/// # Safety
///
/// `result` must be null or point to a result initialized by this ABI. The
/// caller must not mutate its pointer or length fields.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_result_free(result: *mut Result) -> Status {
    if result.is_null() {
        return STATUS_INVALID_ARGUMENT;
    }

    let outcome = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: The caller promises this pointer refers to a bridge result
        // and has not altered either owned buffer.
        let result = unsafe { &mut *result };
        // SAFETY: Both buffers originate from this bridge or are empty.
        unsafe {
            free_buffer(&mut result.value);
            free_buffer(&mut result.error);
        }
        result.status = STATUS_OK;
    }));

    if outcome.is_ok() {
        STATUS_OK
    } else {
        STATUS_PANIC
    }
}

/// Rust-only probe proving that the shared ABI wrapper contains panics.
///
/// This symbol is not exported through the C header or assigned a stable ABI
/// name. It exists solely for the integration test crate.
///
/// # Safety
///
/// `output` follows the same contract as
/// [`ocaml_temporal_core_v1_check_abi_version`].
#[doc(hidden)]
pub unsafe fn test_invoke_panic(output: *mut Result) -> Status {
    // SAFETY: The pointer contract is forwarded unchanged to `invoke`.
    unsafe { invoke(output, || panic!("intentional ABI containment probe")) }
}

/// Returns process-local lifecycle counts for the isolated cleanup test.
///
/// This is intentionally not part of the C ABI. Keeping the counters monotonic
/// lets the test wait for asynchronous destruction without reaching into the
/// runtime owner or depending on timing alone.
#[doc(hidden)]
pub fn test_runtime_cleanup_counts() -> (u64, u64) {
    (
        RUNTIMES_CREATED.load(Ordering::Acquire),
        RUNTIMES_CLEANED.load(Ordering::Acquire),
    )
}

#[cfg(test)]
#[path = "../tests/support/abi_rejection.rs"]
mod rejection_tests;

#[cfg(test)]
mod client_wait_tests {
    use super::bounded_client_wait;
    use crate::client_protocol::{ExecutionRef, WaitWorkflowResponse, WorkflowOutcome};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::task::{Context, Poll};

    /// Future used to prove a timed-out client request is dropped promptly.
    struct PendingWait {
        /// Set by [`Drop`] when timeout cancellation releases the request.
        dropped: Arc<AtomicBool>,
    }

    impl Future for PendingWait {
        type Output =
            std::result::Result<WaitWorkflowResponse, crate::client_protocol::ClientOperationError>;

        /// Remains pending forever, like an open Temporal history long poll.
        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for PendingWait {
        /// Records that timeout cancellation reclaimed the in-flight request.
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
        }
    }

    /// Builds a runtime with Tokio's timer driver for bounded-wait tests.
    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("client wait test runtime should build")
    }

    #[test]
    /// Returns `None` and drops a request that never produces a close event.
    fn timeout_cancels_pending_client_wait() {
        let runtime = test_runtime();
        let dropped = Arc::new(AtomicBool::new(false));
        let result = runtime.block_on(bounded_client_wait(PendingWait {
            dropped: Arc::clone(&dropped),
        }));

        assert_eq!(result, Ok(None));
        assert!(dropped.load(Ordering::Acquire));
    }

    #[test]
    /// Preserves a completed terminal response instead of turning it pending.
    fn completed_client_wait_passes_through() {
        let runtime = test_runtime();
        let response = WaitWorkflowResponse {
            execution: ExecutionRef {
                namespace: "default".to_owned(),
                workflow_id: "workflow-1".to_owned(),
                run_id: "run-1".to_owned(),
            },
            outcome: WorkflowOutcome::Completed {
                result: Vec::new(),
                successor: None,
            },
        };
        let result = runtime.block_on(bounded_client_wait(async { Ok(response.clone()) }));

        assert_eq!(result, Ok(Some(response)));
    }
}

#[cfg(test)]
#[path = "../tests/support/pending_start_cleanup.rs"]
mod pending_start_cleanup_tests;
