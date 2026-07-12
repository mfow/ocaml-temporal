//! Task ownership and shutdown admission for the private Core worker bridge.
//!
//! This module contains no OCaml-facing types. It centralizes the invariants
//! shared by the Rust-owned workflow and activity poll lanes so that task
//! identity, completion, and worker finalization cannot race through separate
//! ad-hoc state machines.

use std::collections::{HashMap, hash_map::Entry};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use temporalio_common::protos::coresdk::{
    ActivityTaskCompletion,
    activity_result::ActivityExecutionResult,
    activity_task::{ActivityTask, activity_task},
    workflow_activation::WorkflowActivation,
    workflow_completion::WorkflowActivationCompletion,
};
use temporalio_common::protos::temporal::api::enums::v1::WorkflowTaskFailedCause;
use temporalio_common::worker::WorkerTaskTypes;
use temporalio_sdk_core::{PollError, Worker};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task::JoinHandle;

/// Maximum UTF-8 byte length accepted for a workflow run identifier.
const MAX_RUN_ID_BYTES: usize = 64 * 1024;
/// Maximum opaque task-token length admitted into the private bridge.
const MAX_TASK_TOKEN_BYTES: usize = 128 * 1024 * 1024;

/// Builds the bounded failure text sent to Core when a workflow activation
/// cannot cross the private semantic boundary.
///
/// The caller can provide only a process-static string. This keeps payloads,
/// headers, run identifiers, and other workflow-controlled data out of the
/// server-visible diagnostic while still identifying the conversion category.
pub fn workflow_rejection_message(reason: &'static str) -> String {
    format!("OCaml bridge could not represent the workflow activation: {reason}")
}

/// Returns the exact Core task surface implemented by the first worker slice.
pub fn bridge_task_types() -> WorkerTaskTypes {
    WorkerTaskTypes {
        enable_workflows: true,
        enable_local_activities: false,
        enable_remote_activities: true,
        enable_nexus: false,
    }
}

/// Fatal reason a guarded poll lane stopped before ordinary Core shutdown.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PollLaneError {
    /// Core reported a non-shutdown polling error.
    Core(String),
    /// Core emitted a task identity that violated bridge ownership rules.
    Admission(AdmitError),
    /// Core emitted a second outstanding task with the same identity.
    DuplicateIdentity,
    /// Core emitted an activity without a known start or cancel variant.
    InvalidActivityVariant,
}

/// Failure while completing or consuming a draining Core worker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkerBridgeError {
    /// The completion does not refer to a task leased to OCaml.
    Completion(CompleteError),
    /// Core rejected a workflow completion.
    CoreWorkflow(String),
    /// Core rejected an activity completion.
    CoreActivity(String),
    /// Finalization was attempted while tasks remain outstanding.
    OutstandingTasks(usize),
    /// A poll task still retained the worker after both joins completed.
    WorkerStillShared,
}

/// One task or terminal lane error waiting for the OCaml supervisor.
pub type ReadyTask<T> = Result<T, PollLaneError>;

/// Result of waiting for one poll lane's next owner-domain action.
///
/// `Ready` means that at least one message is queued and the caller should use
/// the corresponding non-blocking drain operation. `Shutdown` is returned only
/// after the lane is closed and its queued messages have been drained. `Error`
/// preserves a fatal lane error when no earlier queued message remains. The
/// wait is deliberately synchronous because its C caller releases the OCaml
/// runtime lock and invokes it only from the dedicated supervisor owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadinessWait {
    /// At least one task or queued lane error is ready to be drained.
    Ready,
    /// The lane closed normally and has no queued messages left.
    Shutdown,
    /// The lane stopped with a fatal bridge error after its queued messages.
    Error(PollLaneError),
    /// No event arrived before the bounded supervisor-mailbox wait elapsed.
    ///
    /// A timeout is intentional: the supervisor must regain its mailbox loop
    /// periodically so a queued shutdown command can run even while no Core
    /// task is available to wake this lane.
    TimedOut,
}

/// Maximum time one owner-domain readiness call may hold the supervisor loop.
///
/// This bound is a liveness guard rather than a workflow timer. It leaves the
/// supervisor mailbox responsive to lifecycle messages while still avoiding a
/// polling spin when Core is quiet.
pub const READINESS_WAIT_TIMEOUT: Duration = Duration::from_millis(100);

/// Shared wake state for one Rust-owned poll queue.
///
/// The state mutex is held across the unbounded-channel send and pending-count
/// update. The owner-domain drain holds the same mutex across `try_recv` and
/// decrement. This ordering makes the count and queue linearizable: a waiter
/// can never observe a queued message with a zero count, and a drain can never
/// decrement a count before the producer increments it. The condition variable
/// is only a wake mechanism; every waiter rechecks the state predicate while
/// holding the mutex, so notifications cannot be lost before a wait begins.
struct Readiness {
    state: Mutex<ReadinessState>,
    wake: Condvar,
}

/// Mutable predicate protected by [`Readiness::state`].
#[derive(Debug, Default)]
struct ReadinessState {
    /// Number of channel messages that have been committed by a producer but
    /// not yet consumed by the owner Domain.
    pending: usize,
    /// A fatal poll-lane error. It remains visible after its error message is
    /// drained so later waits cannot block after the lane has failed.
    error: Option<PollLaneError>,
    /// No new Core poll result is expected after this flag is set. In-flight
    /// polls may still enqueue messages while the flag is true; pending work
    /// always takes precedence over this terminal state.
    closed: bool,
}

impl Readiness {
    /// Creates an open signal with no queued work.
    fn new() -> Self {
        Self {
            state: Mutex::new(ReadinessState::default()),
            wake: Condvar::new(),
        }
    }

    /// Atomically publishes one queue message and its pending-count update.
    ///
    /// The producer sends while holding the state mutex. `UnboundedSender::send`
    /// is non-blocking, so this short critical section cannot stall Tokio or
    /// the OCaml owner; it only establishes the queue/count ordering required
    /// by the wait predicate.
    fn enqueue<T>(
        &self,
        sender: &mpsc::UnboundedSender<ReadyTask<T>>,
        message: ReadyTask<T>,
    ) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if sender.send(message).is_err() {
            // The owner has gone away. No future caller can drain this lane,
            // but marking it closed also prevents a defensive waiter from
            // sleeping forever if it is still holding the runtime graph.
            state.closed = true;
            self.wake.notify_all();
            return false;
        }
        // Core's outstanding-task permits keep this count far below `usize::MAX`
        // in normal operation. Saturation is still safer than panicking in a
        // background lane if a future producer violates that assumption.
        state.pending = state.pending.saturating_add(1);
        self.wake.notify_all();
        true
    }

    /// Consumes one queue message while atomically retiring its pending count.
    ///
    /// Only the owner Domain calls this method, but the producer uses the same
    /// mutex while publishing, which prevents a send/receive reordering race.
    fn take<T>(
        &self,
        receiver: &mut mpsc::UnboundedReceiver<ReadyTask<T>>,
    ) -> Option<ReadyTask<T>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match receiver.try_recv() {
            Ok(message) => {
                // A successful receive is paired with exactly one successful
                // enqueue while this mutex was held. Keep release builds
                // defensive in case a future channel implementation changes
                // that invariant.
                debug_assert!(state.pending > 0);
                state.pending = state.pending.saturating_sub(1);
                Some(message)
            }
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => {
                state.closed = true;
                self.wake.notify_all();
                None
            }
        }
    }

    /// Records a fatal poll-lane error and wakes the owner immediately.
    fn fail(&self, error: PollLaneError) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.error = Some(error);
        self.wake.notify_all();
    }

    /// Marks the lane as normally closed while retaining queued work for drain.
    fn close(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.closed = true;
        self.wake.notify_all();
    }

    /// Blocks until work, a fatal error, or terminal closure is observable.
    fn wait(&self) -> ReadinessWait {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let deadline = Instant::now() + READINESS_WAIT_TIMEOUT;
        loop {
            // Queued messages always win over terminal flags: the supervisor
            // must drain all messages before it reports shutdown or failure.
            if state.pending > 0 {
                return ReadinessWait::Ready;
            }
            if let Some(error) = state.error.clone() {
                return ReadinessWait::Error(error);
            }
            if state.closed {
                return ReadinessWait::Shutdown;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return ReadinessWait::TimedOut;
            }
            let (next_state, timeout) = self
                .wake
                .wait_timeout(state, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next_state;
            if timeout.timed_out() {
                return ReadinessWait::TimedOut;
            }
        }
    }
}

/// Rust-owned pair of guarded Core poll lanes for one worker.
///
/// Exactly one Tokio task invokes each Core poll API. The channels are only
/// consumed by the owner Domain through non-blocking `try_take_*` calls, so a
/// long Core poll can never block lifecycle or completion messages in the
/// OCaml supervisor mailbox.
pub struct PollLanes {
    worker: Arc<Worker>,
    ledger: Arc<Mutex<TaskLedger>>,
    workflow_ready: mpsc::UnboundedReceiver<ReadyTask<WorkflowActivation>>,
    activity_ready: mpsc::UnboundedReceiver<ReadyTask<ActivityTask>>,
    workflow_signal: Arc<Readiness>,
    activity_signal: Arc<Readiness>,
    workflow_lane: Option<JoinHandle<()>>,
    activity_lane: Option<JoinHandle<()>>,
    shutdown_started: bool,
}

impl PollLanes {
    /// Starts the sole workflow poll and sole remote-activity poll loops.
    pub fn start(worker: Worker, handle: &tokio::runtime::Handle) -> Self {
        let worker = Arc::new(worker);
        let ledger = Arc::new(Mutex::new(TaskLedger::new()));
        // Core's configured outstanding-task permits provide the actual queue
        // bound. An unbounded Tokio handoff is required here because awaiting a
        // full bounded send would prevent the serialized supervisor from
        // joining poll lanes during shutdown.
        let (workflow_sender, workflow_ready) = mpsc::unbounded_channel();
        let (activity_sender, activity_ready) = mpsc::unbounded_channel();
        let workflow_signal = Arc::new(Readiness::new());
        let activity_signal = Arc::new(Readiness::new());

        let workflow_lane = handle.spawn(run_workflow_lane(
            Arc::clone(&worker),
            Arc::clone(&ledger),
            workflow_sender,
            Arc::clone(&workflow_signal),
        ));
        let activity_lane = handle.spawn(run_activity_lane(
            Arc::clone(&worker),
            Arc::clone(&ledger),
            activity_sender,
            Arc::clone(&activity_signal),
        ));
        Self {
            worker,
            ledger,
            workflow_ready,
            activity_ready,
            workflow_signal,
            activity_signal,
            workflow_lane: Some(workflow_lane),
            activity_lane: Some(activity_lane),
            shutdown_started: false,
        }
    }

    /// Takes one ready activation without waiting for Core or a channel lock.
    pub fn try_take_workflow(&mut self) -> Option<ReadyTask<WorkflowActivation>> {
        let ready = self.workflow_signal.take(&mut self.workflow_ready)?;
        if let Ok(activation) = &ready {
            let mut ledger = self
                .ledger
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Err(error) = ledger.lease_workflow(&activation.run_id) {
                return Some(Err(PollLaneError::Admission(match error {
                    CompleteError::UnknownWorkflow | CompleteError::NotLeased => {
                        AdmitError::InvalidIdentity
                    }
                    CompleteError::UnknownActivity => unreachable!(),
                })));
            }
        }
        Some(ready)
    }

    /// Takes one ready remote activity without blocking the supervisor Domain.
    pub fn try_take_activity(&mut self) -> Option<ReadyTask<ActivityTask>> {
        let ready = self.activity_signal.take(&mut self.activity_ready)?;
        if let Ok(task) = &ready {
            let mut ledger = self
                .ledger
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Err(error) = ledger.lease_activity(&task.task_token) {
                return Some(Err(PollLaneError::Admission(match error {
                    CompleteError::UnknownActivity | CompleteError::NotLeased => {
                        AdmitError::InvalidIdentity
                    }
                    CompleteError::UnknownWorkflow => unreachable!(),
                })));
            }
        }
        Some(ready)
    }

    /// Closes admission before waking both Core poll futures for shutdown.
    ///
    /// The caller must enter the worker's Tokio runtime before this synchronous
    /// method because Core spawns its deregistration task internally.
    pub fn initiate_shutdown(&mut self) {
        if self.shutdown_started {
            return;
        }
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .begin_draining();
        // Wake any supervisor wait immediately. In-flight Core polls may still
        // enqueue tasks; `Readiness::wait` prioritizes those pending messages
        // before returning the terminal shutdown state.
        self.workflow_signal.close();
        self.activity_signal.close();
        self.worker.initiate_shutdown();
        self.shutdown_started = true;
    }

    /// Waits for the next workflow-lane message without holding the OCaml lock.
    pub fn wait_workflow(&self) -> ReadinessWait {
        self.workflow_signal.wait()
    }

    /// Waits for the next activity-lane message without holding the OCaml lock.
    pub fn wait_activity(&self) -> ReadinessWait {
        self.activity_signal.wait()
    }

    /// Waits until both guarded poll futures have observed Core shutdown.
    pub async fn join_poll_lanes(&mut self) -> Result<(), PollLaneError> {
        if let Some(workflow_lane) = self.workflow_lane.take() {
            workflow_lane.await.map_err(|error| {
                PollLaneError::Core(format!("workflow poll lane failed: {error}"))
            })?;
        }
        if let Some(activity_lane) = self.activity_lane.take() {
            activity_lane.await.map_err(|error| {
                PollLaneError::Core(format!("activity poll lane failed: {error}"))
            })?;
        }
        Ok(())
    }

    /// Reports whether every task admitted before shutdown has completed.
    pub fn can_finalize(&self) -> bool {
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .can_finalize()
    }

    /// Sends a leased workflow completion to Core, then retires its debt.
    ///
    /// The ledger entry is deliberately retained when Core rejects the value,
    /// allowing the language side to correct a validation failure rather than
    /// losing track of ownership after a partial bridge conversion.
    pub async fn complete_workflow(
        &self,
        completion: WorkflowActivationCompletion,
    ) -> Result<(), WorkerBridgeError> {
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .ensure_workflow_leased(&completion.run_id)
            .map_err(WorkerBridgeError::Completion)?;
        let run_id = completion.run_id.clone();
        self.worker
            .complete_workflow_activation(completion)
            .await
            .map_err(|error| WorkerBridgeError::CoreWorkflow(error.to_string()))?;
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .complete_workflow(&run_id)
            .map_err(WorkerBridgeError::Completion)
    }

    /// Sends a leased remote-activity completion to Core, then retires it.
    pub async fn complete_activity(
        &self,
        completion: ActivityTaskCompletion,
    ) -> Result<(), WorkerBridgeError> {
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .ensure_activity_leased(&completion.task_token)
            .map_err(WorkerBridgeError::Completion)?;
        let task_token = completion.task_token.clone();
        self.worker
            .complete_activity_task(completion)
            .await
            .map_err(|error| WorkerBridgeError::CoreActivity(error.to_string()))?;
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .complete_activity(&task_token)
            .map_err(WorkerBridgeError::Completion)
    }

    /// Fails an activation that could not cross the semantic JSON boundary.
    ///
    /// The activation was leased by [`Self::try_take_workflow`] but was never
    /// exposed to OCaml, so no language-side caller can return its completion.
    /// This method makes exactly one Core completion attempt and then retires
    /// the private debt even if Core rejects that attempt. A rejection is a
    /// fatal worker error, but it must not also fabricate an eternally leased
    /// task that prevents deterministic shutdown.
    pub async fn reject_workflow_delivery(&self, run_id: &str) -> Result<(), WorkerBridgeError> {
        self.reject_workflow_delivery_with_reason(
            run_id,
            "semantic workflow activation conversion failed",
        )
        .await
    }

    /// Rejects a leased activation with a privacy-safe static conversion
    /// category and then retires the lease exactly once.
    pub async fn reject_workflow_delivery_with_reason(
        &self,
        run_id: &str,
        reason: &'static str,
    ) -> Result<(), WorkerBridgeError> {
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .ensure_workflow_leased(run_id)
            .map_err(WorkerBridgeError::Completion)?;
        let completion = WorkflowActivationCompletion::fail(
            run_id,
            workflow_rejection_message(reason).into(),
            Some(WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure),
        );
        let core_result = self
            .worker
            .complete_workflow_activation(completion)
            .await
            .map_err(|error| WorkerBridgeError::CoreWorkflow(error.to_string()));
        let ledger_result = self
            .ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retire_rejected_workflow(run_id)
            .map_err(WorkerBridgeError::Completion);
        core_result.and(ledger_result)
    }

    /// Fails a remote activity task that semantic conversion could not expose.
    ///
    /// As with workflow rejection, the generated failure is attempted once
    /// and the inaccessible token is retired on every outcome. Retaining it
    /// after conversion failure would make graceful shutdown impossible
    /// because OCaml never received the token needed to complete it.
    pub async fn reject_activity_delivery(
        &self,
        task_token: &[u8],
    ) -> Result<(), WorkerBridgeError> {
        self.ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .ensure_activity_leased(task_token)
            .map_err(WorkerBridgeError::Completion)?;
        let completion = ActivityTaskCompletion {
            task_token: task_token.to_vec(),
            result: Some(ActivityExecutionResult::fail(
                "OCaml bridge could not represent the activity task".into(),
            )),
        };
        let core_result = self
            .worker
            .complete_activity_task(completion)
            .await
            .map_err(|error| WorkerBridgeError::CoreActivity(error.to_string()));
        let ledger_result = self
            .ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retire_rejected_activity(task_token)
            .map_err(WorkerBridgeError::Completion);
        core_result.and(ledger_result)
    }

    /// Consumes a fully drained worker and runs Core's terminal finalizer.
    pub async fn finalize(self) -> Result<(), WorkerBridgeError> {
        let outstanding = self
            .ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .outstanding();
        if outstanding != 0 {
            return Err(WorkerBridgeError::OutstandingTasks(outstanding));
        }
        let worker =
            Arc::try_unwrap(self.worker).map_err(|_| WorkerBridgeError::WorkerStillShared)?;
        worker.finalize_shutdown().await;
        Ok(())
    }
}

/// Polls workflow activations serially and records ownership before enqueueing.
async fn run_workflow_lane(
    worker: Arc<Worker>,
    ledger: Arc<Mutex<TaskLedger>>,
    sender: mpsc::UnboundedSender<ReadyTask<WorkflowActivation>>,
    signal: Arc<Readiness>,
) {
    loop {
        let activation = match worker.poll_workflow_activation().await {
            Ok(activation) => activation,
            Err(PollError::ShutDown) => {
                signal.close();
                return;
            }
            Err(error) => {
                let error = PollLaneError::Core(error.to_string());
                let _ = signal.enqueue(&sender, Err(error.clone()));
                signal.fail(error);
                return;
            }
        };
        let admission = ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .admit_polled_workflow(&activation.run_id);
        let message = match admission {
            Ok(Admission::New) => Ok(activation),
            Ok(_) => Err(PollLaneError::DuplicateIdentity),
            Err(error) => Err(PollLaneError::Admission(error)),
        };
        if !signal.enqueue(&sender, message) {
            return;
        }
    }
}

/// Polls remote activities serially and associates cancellation with its start.
async fn run_activity_lane(
    worker: Arc<Worker>,
    ledger: Arc<Mutex<TaskLedger>>,
    sender: mpsc::UnboundedSender<ReadyTask<ActivityTask>>,
    signal: Arc<Readiness>,
) {
    loop {
        let task = match worker.poll_activity_task().await {
            Ok(task) => task,
            Err(PollError::ShutDown) => {
                signal.close();
                return;
            }
            Err(error) => {
                let error = PollLaneError::Core(error.to_string());
                let _ = signal.enqueue(&sender, Err(error.clone()));
                signal.fail(error);
                return;
            }
        };
        let kind = match task.variant {
            Some(activity_task::Variant::Start(_)) => ActivityAdmission::Start,
            Some(activity_task::Variant::Cancel(_)) => ActivityAdmission::Cancel,
            None => {
                let error = PollLaneError::InvalidActivityVariant;
                if !signal.enqueue(&sender, Err(error.clone())) {
                    return;
                }
                continue;
            }
        };
        let admission = ledger
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .admit_polled_activity(&task.task_token, kind);
        let message = match admission {
            Ok(Admission::New | Admission::ExistingCancellation) => Ok(task),
            Ok(Admission::Duplicate) => Err(PollLaneError::DuplicateIdentity),
            Err(error) => Err(PollLaneError::Admission(error)),
        };
        if !signal.enqueue(&sender, message) {
            return;
        }
    }
}

/// Describes whether a newly polled Core task changed outstanding ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Admission {
    /// The task creates one new completion obligation to Core.
    New,
    /// The task identity is already outstanding and must not be delivered twice.
    Duplicate,
    /// An activity cancellation updates the already outstanding start task.
    ExistingCancellation,
}

/// Distinguishes the two activity messages emitted by Core's activity poll.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActivityAdmission {
    /// Core supplied the initial remote activity task.
    Start,
    /// Core requested cancellation of a previously started remote activity.
    Cancel,
}

/// A task could not be admitted into the worker's ownership ledger.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmitError {
    /// Shutdown has begun, so no new task may cross into the language runtime.
    Draining,
    /// The task identity is empty or exceeds the bridge transport ceiling.
    InvalidIdentity,
    /// Core supplied cancellation for an activity the bridge does not own.
    UnknownActivityCancellation,
}

/// A language completion did not match one outstanding Core task.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompleteError {
    /// No workflow activation with this run identifier is outstanding.
    UnknownWorkflow,
    /// No activity with this opaque task token is outstanding.
    UnknownActivity,
    /// The task is still Rust-owned and has not been handed to OCaml.
    NotLeased,
}

/// Admission phase controlling poll delivery and final shutdown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    /// Both poll lanes may admit tasks.
    Open,
    /// Poll delivery is closed while existing tasks may still complete.
    Draining,
}

/// Single source of truth for Core tasks owed a language-side completion.
///
/// Callers protect one instance with a short-held mutex. No method performs
/// I/O or awaits, so the mutex is never retained across a Core future. A task
/// is inserted immediately after polling and before it is made visible to
/// OCaml; it is removed only after Core accepts the matching completion.
#[derive(Debug)]
pub struct TaskLedger {
    phase: Phase,
    workflows: HashMap<String, bool>,
    activities: HashMap<Vec<u8>, ActivityState>,
}

/// Mutable delivery state associated with one activity completion debt.
#[derive(Clone, Copy, Debug, Default)]
struct ActivityState {
    cancelled: bool,
    leased: bool,
}

impl TaskLedger {
    /// Creates an open ledger with no outstanding completion obligations.
    pub fn new() -> Self {
        Self {
            phase: Phase::Open,
            workflows: HashMap::new(),
            activities: HashMap::new(),
        }
    }

    /// Records one workflow activation before its delivery to OCaml.
    pub fn admit_workflow(&mut self, run_id: &str) -> Result<Admission, AdmitError> {
        self.ensure_open()?;
        self.record_workflow(run_id)
    }

    /// Records a task returned by a Core poll already in flight at shutdown.
    fn admit_polled_workflow(&mut self, run_id: &str) -> Result<Admission, AdmitError> {
        self.record_workflow(run_id)
    }

    /// Applies workflow identity rules after admission-phase handling.
    fn record_workflow(&mut self, run_id: &str) -> Result<Admission, AdmitError> {
        if run_id.is_empty() || run_id.len() > MAX_RUN_ID_BYTES {
            return Err(AdmitError::InvalidIdentity);
        }
        match self.workflows.entry(run_id.to_owned()) {
            Entry::Vacant(entry) => {
                entry.insert(false);
                Ok(Admission::New)
            }
            Entry::Occupied(_) => Ok(Admission::Duplicate),
        }
    }

    /// Records an activity start or associates cancellation with its start.
    ///
    /// The boolean map value remembers whether cancellation has already been
    /// observed. Repeated cancellation is a duplicate delivery, while the
    /// first cancellation preserves the original single completion debt.
    pub fn admit_activity(
        &mut self,
        task_token: &[u8],
        kind: ActivityAdmission,
    ) -> Result<Admission, AdmitError> {
        if kind == ActivityAdmission::Start {
            self.ensure_open()?;
        }
        self.record_activity(task_token, kind)
    }

    /// Records an activity returned by a Core poll already in flight at shutdown.
    fn admit_polled_activity(
        &mut self,
        task_token: &[u8],
        kind: ActivityAdmission,
    ) -> Result<Admission, AdmitError> {
        self.record_activity(task_token, kind)
    }

    /// Applies token and cancellation rules after admission-phase handling.
    fn record_activity(
        &mut self,
        task_token: &[u8],
        kind: ActivityAdmission,
    ) -> Result<Admission, AdmitError> {
        if task_token.is_empty() || task_token.len() > MAX_TASK_TOKEN_BYTES {
            return Err(AdmitError::InvalidIdentity);
        }
        match (kind, self.activities.get_mut(task_token)) {
            (ActivityAdmission::Start, Some(_)) => Ok(Admission::Duplicate),
            (ActivityAdmission::Start, None) => {
                self.activities
                    .insert(task_token.to_vec(), ActivityState::default());
                Ok(Admission::New)
            }
            (ActivityAdmission::Cancel, Some(state)) if !state.cancelled => {
                state.cancelled = true;
                Ok(Admission::ExistingCancellation)
            }
            (ActivityAdmission::Cancel, Some(_)) => Ok(Admission::Duplicate),
            (ActivityAdmission::Cancel, None) => Err(AdmitError::UnknownActivityCancellation),
        }
    }

    /// Marks a ready workflow activation as handed to the OCaml supervisor.
    pub fn lease_workflow(&mut self, run_id: &str) -> Result<(), CompleteError> {
        let leased = self
            .workflows
            .get_mut(run_id)
            .ok_or(CompleteError::UnknownWorkflow)?;
        *leased = true;
        Ok(())
    }

    /// Marks a ready activity task as handed to the OCaml supervisor.
    pub fn lease_activity(&mut self, task_token: &[u8]) -> Result<(), CompleteError> {
        let state = self
            .activities
            .get_mut(task_token)
            .ok_or(CompleteError::UnknownActivity)?;
        state.leased = true;
        Ok(())
    }

    /// Removes the exact workflow completion obligation named by `run_id`.
    pub fn complete_workflow(&mut self, run_id: &str) -> Result<(), CompleteError> {
        match self.workflows.get(run_id) {
            Some(true) => {
                self.workflows.remove(run_id);
                Ok(())
            }
            Some(false) => Err(CompleteError::NotLeased),
            None => Err(CompleteError::UnknownWorkflow),
        }
    }

    /// Verifies that a workflow completion is authorized without mutating it.
    pub fn ensure_workflow_leased(&self, run_id: &str) -> Result<(), CompleteError> {
        match self.workflows.get(run_id) {
            Some(true) => Ok(()),
            Some(false) => Err(CompleteError::NotLeased),
            None => Err(CompleteError::UnknownWorkflow),
        }
    }

    /// Removes the exact activity completion obligation named by its token.
    pub fn complete_activity(&mut self, task_token: &[u8]) -> Result<(), CompleteError> {
        match self.activities.get(task_token) {
            Some(state) if state.leased => {
                self.activities.remove(task_token);
                Ok(())
            }
            Some(_) => Err(CompleteError::NotLeased),
            None => Err(CompleteError::UnknownActivity),
        }
    }

    /// Retires a leased workflow that failed before OCaml could observe it.
    ///
    /// This is distinct from ordinary completion only at the call site: both
    /// consume one exact leased debt, but rejection is permitted solely after
    /// the bridge has attempted its own failure completion with Core.
    pub fn retire_rejected_workflow(&mut self, run_id: &str) -> Result<(), CompleteError> {
        self.complete_workflow(run_id)
    }

    /// Retires a leased activity token that semantic conversion could not
    /// expose, after the bridge has attempted a generated failure completion.
    pub fn retire_rejected_activity(&mut self, task_token: &[u8]) -> Result<(), CompleteError> {
        self.complete_activity(task_token)
    }

    /// Verifies that an activity completion is authorized without mutating it.
    pub fn ensure_activity_leased(&self, task_token: &[u8]) -> Result<(), CompleteError> {
        match self.activities.get(task_token) {
            Some(state) if state.leased => Ok(()),
            Some(_) => Err(CompleteError::NotLeased),
            None => Err(CompleteError::UnknownActivity),
        }
    }

    /// Closes admission before Core shutdown wakes the two poll lanes.
    pub fn begin_draining(&mut self) {
        self.phase = Phase::Draining;
    }

    /// Returns whether Core finalization can consume the worker safely.
    pub fn can_finalize(&self) -> bool {
        self.phase == Phase::Draining && self.outstanding() == 0
    }

    /// Returns the number of workflow activations awaiting completion.
    pub fn outstanding_workflows(&self) -> usize {
        self.workflows.len()
    }

    /// Returns the number of remote activities awaiting completion.
    pub fn outstanding_activities(&self) -> usize {
        self.activities.len()
    }

    /// Returns all completion obligations currently owned by the bridge.
    pub fn outstanding(&self) -> usize {
        self.outstanding_workflows() + self.outstanding_activities()
    }

    /// Rejects admission once shutdown has atomically entered draining.
    fn ensure_open(&self) -> Result<(), AdmitError> {
        match self.phase {
            Phase::Open => Ok(()),
            Phase::Draining => Err(AdmitError::Draining),
        }
    }
}

impl Default for TaskLedger {
    /// Uses the same empty open state as [`TaskLedger::new`].
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod readiness_tests {
    use super::{PollLaneError, Readiness, ReadinessWait, ReadyTask};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use tokio::sync::mpsc;

    /// Confirms a notification committed before waiting is observed immediately.
    #[test]
    fn notification_before_wait_returns_ready() {
        let signal = Readiness::new();
        let (sender, mut receiver) = mpsc::unbounded_channel::<ReadyTask<()>>();

        assert!(signal.enqueue(&sender, Ok(())));
        assert_eq!(signal.wait(), ReadinessWait::Ready);
        assert!(signal.take(&mut receiver).is_some());
    }

    /// Confirms queued work cannot be received before its pending count exists.
    ///
    /// The producer runs on another OS thread to exercise the same ordering as
    /// a Tokio poll lane racing an owner-domain drain.
    #[test]
    fn queued_work_has_a_linearizable_send_and_receive_order() {
        let signal = Arc::new(Readiness::new());
        let (sender, mut receiver) = mpsc::unbounded_channel::<ReadyTask<usize>>();
        let producer_signal = Arc::clone(&signal);
        let producer = thread::spawn(move || {
            for value in 0..128 {
                assert!(producer_signal.enqueue(&sender, Ok(value)));
            }
        });

        let mut received = Vec::new();
        while received.len() < 128 {
            assert_eq!(signal.wait(), ReadinessWait::Ready);
            while let Some(Ok(value)) = signal.take(&mut receiver) {
                received.push(value);
            }
        }
        producer.join().expect("producer must not panic");
        received.sort_unstable();
        assert_eq!(received, (0..128).collect::<Vec<_>>());
    }

    /// Confirms closing a quiet lane wakes a waiter instead of leaving it
    /// blocked until the bounded timeout expires.
    #[test]
    fn shutdown_wakes_a_waiting_owner() {
        let signal = Arc::new(Readiness::new());
        let waiter_signal = Arc::clone(&signal);
        let waiter = thread::spawn(move || waiter_signal.wait());
        thread::sleep(Duration::from_millis(10));
        signal.close();

        assert_eq!(
            waiter.join().expect("waiter must not panic"),
            ReadinessWait::Shutdown
        );
    }

    /// Confirms a quiet lane returns control to its supervisor after the
    /// documented bound instead of waiting forever for an event.
    #[test]
    fn quiet_lane_wait_is_bounded() {
        let signal = Readiness::new();

        assert_eq!(signal.wait(), ReadinessWait::TimedOut);
    }

    /// Confirms a fatal lane error remains observable after its wakeup.
    #[test]
    fn lane_error_wakes_and_remains_terminal() {
        let signal = Readiness::new();
        let error = PollLaneError::Core("poll failed".to_owned());
        signal.fail(error.clone());

        assert_eq!(signal.wait(), ReadinessWait::Error(error.clone()));
        assert_eq!(signal.wait(), ReadinessWait::Error(error));
    }
}
