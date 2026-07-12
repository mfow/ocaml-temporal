//! Closed semantic JSON protocol for workflow activations and completions.
//!
//! OCaml and Rust intentionally validate the same language-neutral document.
//! Protobuf values exist only in the conversion functions at the bottom of
//! this module and can therefore never escape into the OCaml API.

use std::collections::BTreeMap;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::protocol::{self, JsonValue, MAX_PAYLOAD_BYTES, MAX_STRING_BYTES, ProtocolError};

/// Decodes an explicitly present nullable field.
///
/// Serde otherwise maps both an omitted `Option` field and JSON `null` to
/// `None`. The private protocol distinguishes those cases: fields declared
/// required by the schema must appear, even when their value is null.
pub(crate) fn required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

/// Opaque Temporal payload with binary-safe metadata and body bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Payload {
    /// Metadata values keyed by their Temporal metadata name.
    pub metadata: BTreeMap<String, Vec<u8>>,
    /// Opaque payload body.
    pub data: Vec<u8>,
}

/// Wire representation of one canonical base64 byte value.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EncodedBytes {
    encoding: String,
    data: String,
}

/// Wire representation used solely by the custom [`Payload`] serializer.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PayloadWire {
    metadata: BTreeMap<String, EncodedBytes>,
    data: EncodedBytes,
}

/// Converts bytes to the only encoding accepted by the protocol.
fn encode_bytes(value: &[u8]) -> Result<EncodedBytes, &'static str> {
    if value.len() > MAX_PAYLOAD_BYTES {
        return Err("payload exceeds the byte limit");
    }
    Ok(EncodedBytes {
        encoding: "base64".to_owned(),
        data: STANDARD.encode(value),
    })
}

/// Validates and decodes one canonical padded base64 wrapper.
fn decode_bytes(value: EncodedBytes) -> Result<Vec<u8>, &'static str> {
    if value.encoding != "base64" {
        return Err("unsupported payload encoding");
    }
    let decoded = STANDARD
        .decode(value.data.as_bytes())
        .map_err(|_| "payload is not canonical padded base64")?;
    if decoded.len() > MAX_PAYLOAD_BYTES || STANDARD.encode(&decoded) != value.data {
        return Err("payload is not canonical padded base64");
    }
    Ok(decoded)
}

impl Serialize for Payload {
    /// Emits the canonical binary-safe payload object.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let metadata = self
            .metadata
            .iter()
            .map(|(key, value)| {
                if key.is_empty() || key.len() > MAX_STRING_BYTES {
                    return Err(serde::ser::Error::custom(
                        "metadata key length is outside protocol limits",
                    ));
                }
                Ok((
                    key.clone(),
                    encode_bytes(value).map_err(serde::ser::Error::custom)?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, S::Error>>()?;
        PayloadWire {
            metadata,
            data: encode_bytes(&self.data).map_err(serde::ser::Error::custom)?,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Payload {
    /// Accepts only closed canonical base64 payload objects.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = PayloadWire::deserialize(deserializer)?;
        let metadata = wire
            .metadata
            .into_iter()
            .map(|(key, value)| {
                if key.is_empty() || key.len() > MAX_STRING_BYTES {
                    return Err(de::Error::custom(
                        "metadata key length is outside protocol limits",
                    ));
                }
                Ok((key, decode_bytes(value).map_err(de::Error::custom)?))
            })
            .collect::<Result<BTreeMap<_, _>, D::Error>>()?;
        Ok(Self {
            metadata,
            data: decode_bytes(wire.data).map_err(de::Error::custom)?,
        })
    }
}

/// Exact protobuf timestamp components without floating-point conversion.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Timestamp {
    /// Whole seconds from the Unix epoch.
    pub seconds: i64,
    /// Fractional nanoseconds in the inclusive range 0 through 999,999,999.
    pub nanoseconds: i32,
}

/// Exact normalized nonnegative protobuf duration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Duration {
    /// Whole nonnegative seconds.
    pub seconds: i64,
    /// Fractional nanoseconds in the inclusive range 0 through 999,999,999.
    pub nanoseconds: i32,
}

/// Workflow and run identifiers for the root execution of a child workflow.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowExecution {
    /// Stable workflow identifier.
    pub workflow_id: String,
    /// Concrete workflow run identifier.
    pub run_id: String,
}

/// Parent execution identity, including its namespace.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespacedWorkflowExecution {
    /// Namespace containing the parent execution.
    pub namespace: String,
    /// Stable parent workflow identifier.
    pub workflow_id: String,
    /// Concrete parent run identifier.
    pub run_id: String,
}

/// Exact workflow priority values delivered by Core.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowPriority {
    /// Lower positive values represent higher priority; zero requests default behavior.
    pub priority_key: i32,
    /// Fairness group key used by Temporal matching.
    pub fairness_key: String,
    /// Raw IEEE-754 bits preserve Core's `f32` without using JSON fractions.
    pub fairness_weight_bits: u32,
}

/// Normal fields delivered with an ordinary root workflow initialization.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitializeContext {
    /// User headers delivered to workflow interceptors.
    pub headers: BTreeMap<String, Payload>,
    /// Identity of the client that started this execution.
    pub identity: String,
    /// Parent execution for a child workflow, otherwise absent.
    #[serde(deserialize_with = "required_nullable")]
    pub parent_workflow: Option<NamespacedWorkflowExecution>,
    /// Timeout across retries and continue-as-new runs.
    #[serde(deserialize_with = "required_nullable")]
    pub workflow_execution_timeout: Option<Duration>,
    /// Timeout for this individual workflow run.
    #[serde(deserialize_with = "required_nullable")]
    pub workflow_run_timeout: Option<Duration>,
    /// Timeout for one workflow task.
    #[serde(deserialize_with = "required_nullable")]
    pub workflow_task_timeout: Option<Duration>,
    /// First run in this execution chain.
    pub first_execution_run_id: String,
    /// Time at which the execution-start event was written.
    #[serde(deserialize_with = "required_nullable")]
    pub start_time: Option<Timestamp>,
    /// Root execution for a child workflow, otherwise absent.
    #[serde(deserialize_with = "required_nullable")]
    pub root_workflow: Option<WorkflowExecution>,
    /// Workflow scheduling priority, when supplied by Core.
    #[serde(deserialize_with = "required_nullable")]
    pub priority: Option<WorkflowPriority>,
}

/// Deployment version responsible for the current workflow task.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerDeploymentVersion {
    pub deployment_name: String,
    pub build_id: String,
}

/// Why the service suggests continuing the workflow as new.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SuggestContinueAsNewReason {
    Unspecified,
    HistorySizeTooLarge,
    TooManyHistoryEvents,
    TooManyUpdates,
}

/// Activation-wide metadata consumed by a language SDK.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivationMetadata {
    pub available_internal_flags: Vec<u32>,
    pub history_size_bytes: String,
    pub continue_as_new_suggested: bool,
    #[serde(deserialize_with = "required_nullable")]
    pub deployment_version_for_current_task: Option<WorkerDeploymentVersion>,
    pub last_sdk_version: String,
    pub suggest_continue_as_new_reasons: Vec<SuggestContinueAsNewReason>,
    pub target_worker_deployment_version_changed: bool,
}

/// Retry disposition attached to an activity failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryState {
    Unspecified,
    InProgress,
    NonRetryableFailure,
    Timeout,
    MaximumAttemptsReached,
    RetryPolicyNotSet,
    InternalServerError,
    CancelRequested,
}

/// Supported closed subset of Temporal failure information.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FailureInfo {
    Application {
        #[serde(rename = "type")]
        type_name: String,
        non_retryable: bool,
        details: Vec<Payload>,
    },
    Canceled {
        details: Vec<Payload>,
        identity: String,
    },
    Activity {
        scheduled_event_id: i64,
        started_event_id: i64,
        identity: String,
        activity_type: String,
        activity_id: String,
        retry_state: RetryState,
    },
}

/// Recursive Temporal failure with one explicitly supported info variant.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Failure {
    pub message: String,
    pub source: String,
    pub stack_trace: String,
    #[serde(deserialize_with = "required_nullable")]
    pub encoded_attributes: Option<Payload>,
    #[serde(deserialize_with = "required_nullable")]
    pub cause: Option<Box<Failure>>,
    pub info: FailureInfo,
}

/// Result delivered for an activity sequence number.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActivityResolution {
    Completed {
        #[serde(deserialize_with = "required_nullable")]
        payload: Option<Payload>,
    },
    Failed {
        failure: Failure,
    },
    Cancelled {
        failure: Failure,
    },
}

/// Why Core requested removal of a workflow from the language cache.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvictionReason {
    Unspecified,
    CacheFull,
    CacheMiss,
    Nondeterminism,
    LangFail,
    LangRequested,
    TaskNotFound,
    UnhandledCommand,
    Fatal,
    PaginationOrHistoryFetch,
    WorkflowExecutionEnding,
}

/// Supported activation instruction sent from Core to OCaml.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActivationJob {
    InitializeWorkflow {
        workflow_id: String,
        workflow_type: String,
        arguments: Vec<Payload>,
        randomness_seed: String,
        attempt: i32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context: Option<InitializeContext>,
    },
    ResolveActivity {
        seq: u32,
        result: ActivityResolution,
    },
    FireTimer {
        seq: u32,
    },
    CancelWorkflow {
        reason: String,
    },
    RemoveFromCache {
        message: String,
        reason: EvictionReason,
    },
}

/// Complete worker activation delivered to one deterministic workflow run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Activation {
    pub run_id: String,
    /// Core omits time only on its synthetic cache-eviction activation.
    #[serde(deserialize_with = "required_nullable")]
    pub timestamp: Option<Timestamp>,
    pub is_replaying: bool,
    pub history_length: u32,
    pub jobs: Vec<ActivationJob>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ActivationMetadata>,
}

/// How a workflow waits for cancellation of a scheduled activity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityCancellationType {
    TryCancel,
    WaitCancellationCompleted,
    Abandon,
}

/// Supported deterministic command emitted by OCaml for Core.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CompletionCommand {
    ScheduleActivity {
        seq: u32,
        activity_id: String,
        activity_type: String,
        task_queue: String,
        arguments: Vec<Payload>,
        #[serde(deserialize_with = "required_nullable")]
        schedule_to_close_timeout: Option<Duration>,
        #[serde(deserialize_with = "required_nullable")]
        schedule_to_start_timeout: Option<Duration>,
        #[serde(deserialize_with = "required_nullable")]
        start_to_close_timeout: Option<Duration>,
        #[serde(deserialize_with = "required_nullable")]
        heartbeat_timeout: Option<Duration>,
        cancellation_type: ActivityCancellationType,
        do_not_eagerly_execute: bool,
    },
    /// Starts a child workflow using the fields currently exposed by the
    /// OCaml runtime. Core-only options stay at their documented defaults
    /// until the language API grows explicit controls for them.
    StartChildWorkflow {
        seq: u32,
        workflow_id: String,
        workflow_type: String,
        input: Vec<Payload>,
    },
    RequestCancelActivity {
        seq: u32,
    },
    StartTimer {
        seq: u32,
        start_to_fire_timeout: Duration,
    },
    CancelTimer {
        seq: u32,
    },
    CompleteWorkflow {
        #[serde(deserialize_with = "required_nullable")]
        result: Option<Payload>,
    },
    FailWorkflow {
        failure: Failure,
    },
    CancelWorkflow,
}

/// Successful activation completion for one workflow run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Completion {
    pub run_id: String,
    pub commands: Vec<CompletionCommand>,
}

/// Converts the duplicate-aware foundation tree to Serde's owned value.
fn to_serde(value: JsonValue) -> serde_json::Value {
    match value {
        JsonValue::Null => serde_json::Value::Null,
        JsonValue::Bool(value) => serde_json::Value::Bool(value),
        JsonValue::Signed(value) => value.into(),
        JsonValue::Unsigned(value) => value.into(),
        JsonValue::String(value) => value.into(),
        JsonValue::Array(values) => values.into_iter().map(to_serde).collect(),
        JsonValue::Object(entries) => entries
            .into_iter()
            .map(|(key, value)| (key, to_serde(value)))
            .collect(),
    }
}

/// Converts Serde output back into the restricted integral JSON tree.
fn from_serde(value: serde_json::Value) -> Result<JsonValue, ProtocolError> {
    match value {
        serde_json::Value::Null => Ok(JsonValue::Null),
        serde_json::Value::Bool(value) => Ok(JsonValue::Bool(value)),
        serde_json::Value::Number(value) => value
            .as_i64()
            .map(JsonValue::Signed)
            .or_else(|| value.as_u64().map(JsonValue::Unsigned))
            .ok_or_else(|| ProtocolError::invalid("$", "non-integral number in semantic output")),
        serde_json::Value::String(value) => Ok(JsonValue::String(value)),
        serde_json::Value::Array(values) => values
            .into_iter()
            .map(from_serde)
            .collect::<Result<Vec<_>, _>>()
            .map(JsonValue::Array),
        serde_json::Value::Object(entries) => entries
            .into_iter()
            .map(|(key, value)| Ok((key, from_serde(value)?)))
            .collect::<Result<Vec<_>, _>>()
            .map(JsonValue::Object),
    }
}

/// Requires a bounded nonempty Temporal identifier.
pub(crate) fn identifier(value: &str, path: &str) -> Result<(), ProtocolError> {
    if value.is_empty() || value.len() > MAX_STRING_BYTES {
        Err(ProtocolError::invalid(
            path,
            "identifier is empty or exceeds the protocol string safety limit",
        ))
    } else {
        Ok(())
    }
}

/// Retains the ordinary text ceiling while the foundation parser temporarily
/// allows larger base64 strings at semantic payload paths.
fn bounded_text(value: &str, path: &str) -> Result<(), ProtocolError> {
    if value.len() > MAX_STRING_BYTES {
        Err(ProtocolError::invalid(
            path,
            "decoded JSON string limit exceeded",
        ))
    } else {
        Ok(())
    }
}

/// Validates exact protobuf time component ranges.
pub(crate) fn validate_time(
    seconds: i64,
    nanos: i32,
    duration: bool,
    path: &str,
) -> Result<(), ProtocolError> {
    if !(0..=999_999_999).contains(&nanos) {
        return Err(ProtocolError::invalid(
            path,
            "nanoseconds are outside protobuf range",
        ));
    }
    if duration && seconds < 0 {
        return Err(ProtocolError::invalid(
            path,
            "duration must not be negative",
        ));
    }
    Ok(())
}

/// Validates failure identifiers recursively.
pub(crate) fn validate_failure(value: &Failure, path: &str) -> Result<(), ProtocolError> {
    bounded_text(&value.message, path)?;
    bounded_text(&value.source, path)?;
    bounded_text(&value.stack_trace, path)?;
    match &value.info {
        FailureInfo::Application { type_name, .. } => bounded_text(type_name, path)?,
        FailureInfo::Canceled { identity, .. } => bounded_text(identity, path)?,
        FailureInfo::Activity {
            scheduled_event_id,
            started_event_id,
            identity,
            activity_type,
            activity_id,
            retry_state: _,
        } => {
            if *scheduled_event_id < 0 || *started_event_id < 0 {
                return Err(ProtocolError::invalid(
                    path,
                    "activity failure event IDs must not be negative",
                ));
            }
            bounded_text(identity, path)?;
            identifier(activity_type, path)?;
            identifier(activity_id, path)?;
        }
    }
    if let Some(cause) = &value.cause {
        validate_failure(cause, path)?;
    }
    Ok(())
}

/// Applies invariants that derive-based closed-shape validation cannot express.
fn validate_activation(value: &Activation) -> Result<(), ProtocolError> {
    identifier(&value.run_id, "$.run_id")?;
    let eviction_count = value
        .jobs
        .iter()
        .filter(|job| matches!(job, ActivationJob::RemoveFromCache { .. }))
        .count();
    if eviction_count != 0 && (eviction_count != 1 || value.jobs.len() != 1) {
        return Err(ProtocolError::invalid(
            "$.jobs",
            "cache eviction must be the activation's only job",
        ));
    }
    match value.timestamp {
        Some(timestamp) => validate_time(
            timestamp.seconds,
            timestamp.nanoseconds,
            false,
            "$.timestamp",
        )?,
        None if eviction_count == 1 => {}
        None => {
            return Err(ProtocolError::invalid(
                "$.timestamp",
                "timestamp may be null only for cache eviction",
            ));
        }
    }
    let initialize_count = value
        .jobs
        .iter()
        .filter(|job| matches!(job, ActivationJob::InitializeWorkflow { .. }))
        .count();
    if initialize_count > 1
        || (initialize_count == 1
            && !matches!(
                value.jobs.first(),
                Some(ActivationJob::InitializeWorkflow { .. })
            ))
    {
        return Err(ProtocolError::invalid(
            "$.jobs",
            "initialize_workflow must occur at most once and first",
        ));
    }
    for job in &value.jobs {
        match job {
            ActivationJob::InitializeWorkflow {
                workflow_id,
                workflow_type,
                randomness_seed,
                attempt,
                context,
                ..
            } => {
                identifier(workflow_id, "$.jobs.workflow_id")?;
                identifier(workflow_type, "$.jobs.workflow_type")?;
                if *attempt < 1 {
                    return Err(ProtocolError::invalid(
                        "$.jobs.attempt",
                        "attempt must be positive",
                    ));
                }
                let parsed = randomness_seed.parse::<u64>().map_err(|_| {
                    ProtocolError::invalid(
                        "$.jobs.randomness_seed",
                        "value is not canonical unsigned 64-bit decimal",
                    )
                })?;
                if parsed.to_string() != *randomness_seed {
                    return Err(ProtocolError::invalid(
                        "$.jobs.randomness_seed",
                        "value is not canonical unsigned 64-bit decimal",
                    ));
                }
                if let Some(context) = context {
                    for key in context.headers.keys() {
                        identifier(key, "$.jobs.context.headers")?;
                    }
                    bounded_text(&context.identity, "$.jobs.context.identity")?;
                    if let Some(parent) = &context.parent_workflow {
                        identifier(
                            &parent.namespace,
                            "$.jobs.context.parent_workflow.namespace",
                        )?;
                        identifier(
                            &parent.workflow_id,
                            "$.jobs.context.parent_workflow.workflow_id",
                        )?;
                        identifier(&parent.run_id, "$.jobs.context.parent_workflow.run_id")?;
                    }
                    identifier(
                        &context.first_execution_run_id,
                        "$.jobs.context.first_execution_run_id",
                    )?;
                    for duration in [
                        context.workflow_execution_timeout.as_ref(),
                        context.workflow_run_timeout.as_ref(),
                        context.workflow_task_timeout.as_ref(),
                    ]
                    .into_iter()
                    .flatten()
                    {
                        validate_time(
                            duration.seconds,
                            duration.nanoseconds,
                            true,
                            "$.jobs.context.timeout",
                        )?;
                    }
                    if let Some(start_time) = context.start_time {
                        validate_time(
                            start_time.seconds,
                            start_time.nanoseconds,
                            false,
                            "$.jobs.context.start_time",
                        )?;
                    }
                    if let Some(root) = &context.root_workflow {
                        identifier(
                            &root.workflow_id,
                            "$.jobs.context.root_workflow.workflow_id",
                        )?;
                        identifier(&root.run_id, "$.jobs.context.root_workflow.run_id")?;
                    }
                    if let Some(priority) = &context.priority
                        && priority.fairness_key.len() > 64
                    {
                        return Err(ProtocolError::invalid(
                            "$.jobs.context.priority.fairness_key",
                            "fairness key exceeds Core's 64-byte limit",
                        ));
                    }
                }
            }
            ActivationJob::ResolveActivity { result, .. } => match result {
                ActivityResolution::Completed { .. } => {}
                ActivityResolution::Failed { failure }
                | ActivityResolution::Cancelled { failure } => {
                    validate_failure(failure, "$.jobs.result.failure")?
                }
            },
            ActivationJob::CancelWorkflow { reason } => bounded_text(reason, "$.jobs.reason")?,
            ActivationJob::RemoveFromCache { message, .. } => {
                bounded_text(message, "$.jobs.message")?
            }
            ActivationJob::FireTimer { .. } => {}
        }
    }
    if let Some(metadata) = &value.metadata {
        let parsed = metadata.history_size_bytes.parse::<u64>().map_err(|_| {
            ProtocolError::invalid(
                "$.metadata.history_size_bytes",
                "value is not canonical unsigned 64-bit decimal",
            )
        })?;
        if parsed.to_string() != metadata.history_size_bytes {
            return Err(ProtocolError::invalid(
                "$.metadata.history_size_bytes",
                "value is not canonical unsigned 64-bit decimal",
            ));
        }
        bounded_text(&metadata.last_sdk_version, "$.metadata.last_sdk_version")?;
        if let Some(deployment) = &metadata.deployment_version_for_current_task {
            bounded_text(&deployment.deployment_name, "$.metadata.deployment_name")?;
            identifier(&deployment.build_id, "$.metadata.build_id")?;
        }
    }
    Ok(())
}

/// Applies timeout and terminal-command invariants.
fn validate_completion(value: &Completion) -> Result<(), ProtocolError> {
    identifier(&value.run_id, "$.run_id")?;
    for (index, command) in value.commands.iter().enumerate() {
        let terminal = matches!(
            command,
            CompletionCommand::CompleteWorkflow { .. }
                | CompletionCommand::FailWorkflow { .. }
                | CompletionCommand::CancelWorkflow
        );
        if terminal && index + 1 != value.commands.len() {
            return Err(ProtocolError::invalid(
                "$.commands",
                "terminal workflow command must be last",
            ));
        }
        match command {
            CompletionCommand::ScheduleActivity {
                activity_id,
                activity_type,
                task_queue,
                schedule_to_close_timeout,
                schedule_to_start_timeout,
                start_to_close_timeout,
                heartbeat_timeout,
                ..
            } => {
                identifier(activity_id, "$.commands.activity_id")?;
                identifier(activity_type, "$.commands.activity_type")?;
                identifier(task_queue, "$.commands.task_queue")?;
                if schedule_to_close_timeout.is_none() && start_to_close_timeout.is_none() {
                    return Err(ProtocolError::invalid(
                        "$.commands",
                        "activity requires schedule-to-close or start-to-close timeout",
                    ));
                }
                for duration in [
                    schedule_to_close_timeout,
                    schedule_to_start_timeout,
                    start_to_close_timeout,
                    heartbeat_timeout,
                ]
                .into_iter()
                .flatten()
                {
                    validate_time(
                        duration.seconds,
                        duration.nanoseconds,
                        true,
                        "$.commands.timeout",
                    )?;
                }
            }
            CompletionCommand::StartChildWorkflow {
                workflow_id,
                workflow_type,
                ..
            } => {
                identifier(workflow_id, "$.commands.workflow_id")?;
                identifier(workflow_type, "$.commands.workflow_type")?;
            }
            CompletionCommand::StartTimer {
                start_to_fire_timeout,
                ..
            } => validate_time(
                start_to_fire_timeout.seconds,
                start_to_fire_timeout.nanoseconds,
                true,
                "$.commands.start_to_fire_timeout",
            )?,
            CompletionCommand::FailWorkflow { failure } => {
                validate_failure(failure, "$.commands.failure")?
            }
            _ => {}
        }
    }
    Ok(())
}

/// Strictly decodes one workflow activation.
pub fn decode_activation(input: &str) -> Result<Activation, ProtocolError> {
    let value = protocol::decode_payload_object(input)?;
    let activation: Activation = serde_json::from_value(to_serde(value))
        .map_err(|_| ProtocolError::invalid("$", "invalid workflow activation semantics"))?;
    validate_activation(&activation)?;
    Ok(activation)
}

/// Validates, normalizes, and reparses one outgoing workflow activation.
pub fn encode_activation(value: &Activation) -> Result<String, ProtocolError> {
    validate_activation(value)?;
    let json = serde_json::to_value(value)
        .map_err(|_| ProtocolError::invalid("$", "could not encode workflow activation"))?;
    let output = protocol::encode_payload_object(&from_serde(json)?)?;
    decode_activation(&output)?;
    Ok(output)
}

/// Strictly decodes one successful workflow completion.
pub fn decode_completion(input: &str) -> Result<Completion, ProtocolError> {
    let value = protocol::decode_payload_object(input)?;
    let completion: Completion = serde_json::from_value(to_serde(value))
        .map_err(|_| ProtocolError::invalid("$", "invalid workflow completion semantics"))?;
    validate_completion(&completion)?;
    Ok(completion)
}

/// Validates, normalizes, and reparses one outgoing workflow completion.
pub fn encode_completion(value: &Completion) -> Result<String, ProtocolError> {
    validate_completion(value)?;
    let json = serde_json::to_value(value)
        .map_err(|_| ProtocolError::invalid("$", "could not encode workflow completion"))?;
    let output = protocol::encode_payload_object(&from_serde(json)?)?;
    decode_completion(&output)?;
    Ok(output)
}

/// Why an official Core protobuf could not be represented losslessly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoreConversionErrorCode {
    /// The first protocol slice deliberately does not represent this variant or option.
    Unsupported,
    /// A required protobuf oneof or nested message was absent or malformed.
    InvalidCore,
}

/// Privacy-safe error returned at the Rust-only protobuf boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoreConversionError {
    /// Stable category suitable for branching in bridge code.
    pub code: CoreConversionErrorCode,
    /// Static diagnostic that never includes user payload data.
    pub message: &'static str,
}

/// Constructs an unsupported-conversion error without copying source values.
pub(crate) fn unsupported(message: &'static str) -> CoreConversionError {
    CoreConversionError {
        code: CoreConversionErrorCode::Unsupported,
        message,
    }
}

/// Constructs a malformed-Core error without copying source values.
pub(crate) fn invalid_core(message: &'static str) -> CoreConversionError {
    CoreConversionError {
        code: CoreConversionErrorCode::InvalidCore,
        message,
    }
}

use temporalio_protos::{
    coresdk::{
        activity_result as core_activity, workflow_activation as core_activation,
        workflow_commands as core_commands, workflow_completion as core_completion,
    },
    temporal::api::{common::v1 as api_common, enums::v1 as api_enums, failure::v1 as api_failure},
};

/// Copies one Core payload while rejecting external references this JSON slice cannot preserve.
pub(crate) fn payload_from_core(
    value: &api_common::Payload,
) -> Result<Payload, CoreConversionError> {
    if !value.external_payloads.is_empty() {
        return Err(unsupported("external payload references are not supported"));
    }
    if value.data.len() > MAX_PAYLOAD_BYTES
        || value
            .metadata
            .values()
            .any(|bytes| bytes.len() > MAX_PAYLOAD_BYTES)
    {
        return Err(invalid_core("Core payload exceeds the protocol byte limit"));
    }
    if value
        .metadata
        .keys()
        .any(|key| key.is_empty() || key.len() > MAX_STRING_BYTES)
    {
        return Err(invalid_core(
            "Core payload metadata key is outside protocol limits",
        ));
    }
    Ok(Payload {
        metadata: value
            .metadata
            .iter()
            .map(|(key, bytes)| (key.clone(), bytes.clone()))
            .collect(),
        data: value.data.clone(),
    })
}

/// Builds a Core payload whose allocation ownership is transferred into protobuf values.
pub(crate) fn payload_to_core(value: &Payload) -> Result<api_common::Payload, CoreConversionError> {
    if value.data.len() > MAX_PAYLOAD_BYTES
        || value
            .metadata
            .values()
            .any(|bytes| bytes.len() > MAX_PAYLOAD_BYTES)
    {
        return Err(invalid_core(
            "semantic payload exceeds the protocol byte limit",
        ));
    }
    if value
        .metadata
        .keys()
        .any(|key| key.is_empty() || key.len() > MAX_STRING_BYTES)
    {
        return Err(invalid_core(
            "semantic payload metadata key is outside protocol limits",
        ));
    }
    Ok(api_common::Payload {
        metadata: value
            .metadata
            .iter()
            .map(|(key, bytes)| (key.clone(), bytes.clone()))
            .collect(),
        data: value.data.clone(),
        external_payloads: Vec::new(),
    })
}

/// Converts optional Core payload collections while preserving order.
fn payloads_from_core(
    value: Option<&api_common::Payloads>,
) -> Result<Vec<Payload>, CoreConversionError> {
    value
        .map(|payloads| payloads.payloads.iter().map(payload_from_core).collect())
        .unwrap_or_else(|| Ok(Vec::new()))
}

/// Converts semantic payload collections while preserving order.
fn payloads_to_core(values: &[Payload]) -> Result<api_common::Payloads, CoreConversionError> {
    Ok(api_common::Payloads {
        payloads: values
            .iter()
            .map(payload_to_core)
            .collect::<Result<_, _>>()?,
    })
}

/// Maps the official retry enum without accepting future unknown integer values.
fn retry_state_from_core(value: i32) -> Result<RetryState, CoreConversionError> {
    use api_enums::RetryState as Core;
    Ok(
        match Core::try_from(value).map_err(|_| invalid_core("unknown Core retry state"))? {
            Core::Unspecified => RetryState::Unspecified,
            Core::InProgress => RetryState::InProgress,
            Core::NonRetryableFailure => RetryState::NonRetryableFailure,
            Core::Timeout => RetryState::Timeout,
            Core::MaximumAttemptsReached => RetryState::MaximumAttemptsReached,
            Core::RetryPolicyNotSet => RetryState::RetryPolicyNotSet,
            Core::InternalServerError => RetryState::InternalServerError,
            Core::CancelRequested => RetryState::CancelRequested,
        },
    )
}

/// Maps a semantic retry state to the exact official protobuf number.
fn retry_state_to_core(value: RetryState) -> i32 {
    use api_enums::RetryState as Core;
    (match value {
        RetryState::Unspecified => Core::Unspecified,
        RetryState::InProgress => Core::InProgress,
        RetryState::NonRetryableFailure => Core::NonRetryableFailure,
        RetryState::Timeout => Core::Timeout,
        RetryState::MaximumAttemptsReached => Core::MaximumAttemptsReached,
        RetryState::RetryPolicyNotSet => Core::RetryPolicyNotSet,
        RetryState::InternalServerError => Core::InternalServerError,
        RetryState::CancelRequested => Core::CancelRequested,
    }) as i32
}

/// Converts the supported recursive official failure subset.
pub(crate) fn failure_from_core(
    value: &api_failure::Failure,
) -> Result<Failure, CoreConversionError> {
    use api_failure::failure::FailureInfo as Core;
    let info = match value
        .failure_info
        .as_ref()
        .ok_or_else(|| invalid_core("Core failure info is absent"))?
    {
        Core::ApplicationFailureInfo(info) => {
            if info.next_retry_delay.is_some() || info.category != 0 {
                return Err(unsupported("application failure options are not supported"));
            }
            FailureInfo::Application {
                type_name: info.r#type.clone(),
                non_retryable: info.non_retryable,
                details: payloads_from_core(info.details.as_ref())?,
            }
        }
        Core::CanceledFailureInfo(info) => FailureInfo::Canceled {
            details: payloads_from_core(info.details.as_ref())?,
            identity: info.identity.clone(),
        },
        Core::ActivityFailureInfo(info) => FailureInfo::Activity {
            scheduled_event_id: info.scheduled_event_id,
            started_event_id: info.started_event_id,
            identity: info.identity.clone(),
            activity_type: info
                .activity_type
                .as_ref()
                .ok_or_else(|| invalid_core("Core activity failure type is absent"))?
                .name
                .clone(),
            activity_id: info.activity_id.clone(),
            retry_state: retry_state_from_core(info.retry_state)?,
        },
        _ => return Err(unsupported("Core failure info kind is not supported")),
    };
    Ok(Failure {
        message: value.message.clone(),
        source: value.source.clone(),
        stack_trace: value.stack_trace.clone(),
        encoded_attributes: value
            .encoded_attributes
            .as_ref()
            .map(payload_from_core)
            .transpose()?,
        cause: value
            .cause
            .as_deref()
            .map(failure_from_core)
            .transpose()?
            .map(Box::new),
        info,
    })
}

/// Builds an official failure using only options represented in semantic JSON.
pub(crate) fn failure_to_core(
    value: &Failure,
) -> Result<api_failure::Failure, CoreConversionError> {
    use api_failure::failure::FailureInfo as Core;
    let failure_info = Some(match &value.info {
        FailureInfo::Application {
            type_name,
            non_retryable,
            details,
        } => Core::ApplicationFailureInfo(api_failure::ApplicationFailureInfo {
            r#type: type_name.clone(),
            non_retryable: *non_retryable,
            details: Some(payloads_to_core(details)?),
            next_retry_delay: None,
            category: 0,
        }),
        FailureInfo::Canceled { details, identity } => {
            Core::CanceledFailureInfo(api_failure::CanceledFailureInfo {
                details: Some(payloads_to_core(details)?),
                identity: identity.clone(),
            })
        }
        FailureInfo::Activity {
            scheduled_event_id,
            started_event_id,
            identity,
            activity_type,
            activity_id,
            retry_state,
        } => Core::ActivityFailureInfo(api_failure::ActivityFailureInfo {
            scheduled_event_id: *scheduled_event_id,
            started_event_id: *started_event_id,
            identity: identity.clone(),
            activity_type: Some(api_common::ActivityType {
                name: activity_type.clone(),
            }),
            activity_id: activity_id.clone(),
            retry_state: retry_state_to_core(*retry_state),
        }),
    });
    Ok(api_failure::Failure {
        message: value.message.clone(),
        source: value.source.clone(),
        stack_trace: value.stack_trace.clone(),
        encoded_attributes: value
            .encoded_attributes
            .as_ref()
            .map(payload_to_core)
            .transpose()?,
        cause: value
            .cause
            .as_deref()
            .map(failure_to_core)
            .transpose()?
            .map(Box::new),
        failure_info,
    })
}

/// Converts one supported Core activity resolution.
fn activity_resolution_from_core(
    value: &core_activity::ActivityResolution,
) -> Result<ActivityResolution, CoreConversionError> {
    use core_activity::activity_resolution::Status;
    match value
        .status
        .as_ref()
        .ok_or_else(|| invalid_core("Core activity resolution status is absent"))?
    {
        Status::Completed(value) => Ok(ActivityResolution::Completed {
            payload: value.result.as_ref().map(payload_from_core).transpose()?,
        }),
        Status::Failed(value) => Ok(ActivityResolution::Failed {
            failure: failure_from_core(
                value
                    .failure
                    .as_ref()
                    .ok_or_else(|| invalid_core("Core failed activity has no failure"))?,
            )?,
        }),
        Status::Cancelled(value) => Ok(ActivityResolution::Cancelled {
            failure: failure_from_core(
                value
                    .failure
                    .as_ref()
                    .ok_or_else(|| invalid_core("Core cancelled activity has no failure"))?,
            )?,
        }),
        Status::Backoff(_) => Err(unsupported("local activity backoff is not supported")),
    }
}

/// Maps every official cache-eviction enum in the pinned Core revision.
fn eviction_reason_from_core(value: i32) -> Result<EvictionReason, CoreConversionError> {
    use core_activation::remove_from_cache::EvictionReason as Core;
    Ok(
        match Core::try_from(value).map_err(|_| invalid_core("unknown Core eviction reason"))? {
            Core::Unspecified => EvictionReason::Unspecified,
            Core::CacheFull => EvictionReason::CacheFull,
            Core::CacheMiss => EvictionReason::CacheMiss,
            Core::Nondeterminism => EvictionReason::Nondeterminism,
            Core::LangFail => EvictionReason::LangFail,
            Core::LangRequested => EvictionReason::LangRequested,
            Core::TaskNotFound => EvictionReason::TaskNotFound,
            Core::UnhandledCommand => EvictionReason::UnhandledCommand,
            Core::Fatal => EvictionReason::Fatal,
            Core::PaginationOrHistoryFetch => EvictionReason::PaginationOrHistoryFetch,
            Core::WorkflowExecutionEnding => EvictionReason::WorkflowExecutionEnding,
        },
    )
}

/// Checks that initialize fields omitted from this first slice are all defaulted.
fn validate_initialize_subset(
    value: &core_activation::InitializeWorkflow,
) -> Result<(), CoreConversionError> {
    if !value.continued_from_execution_run_id.is_empty()
        || value.continued_initiator != 0
        || value.continued_failure.is_some()
        || value.last_completion_result.is_some()
        || value.retry_policy.is_some()
        || !value.cron_schedule.is_empty()
        || value.workflow_execution_expiration_time.is_some()
        || value.cron_schedule_to_schedule_interval.is_some()
        || value.memo.is_some()
        || value.search_attributes.is_some()
    {
        return Err(unsupported(
            "initialize workflow contains fields not represented by this protocol slice",
        ));
    }
    Ok(())
}

/// Maps one pinned Core suggestion reason and rejects future enum numbers.
fn suggestion_from_core(value: i32) -> Result<SuggestContinueAsNewReason, CoreConversionError> {
    use api_enums::SuggestContinueAsNewReason as Core;
    Ok(
        match Core::try_from(value)
            .map_err(|_| invalid_core("unknown Core continue-as-new suggestion reason"))?
        {
            Core::Unspecified => SuggestContinueAsNewReason::Unspecified,
            Core::HistorySizeTooLarge => SuggestContinueAsNewReason::HistorySizeTooLarge,
            Core::TooManyHistoryEvents => SuggestContinueAsNewReason::TooManyHistoryEvents,
            Core::TooManyUpdates => SuggestContinueAsNewReason::TooManyUpdates,
        },
    )
}

/// Converts an official pinned-Core activation without silently dropping data.
pub fn activation_from_core(
    value: &core_activation::WorkflowActivation,
) -> Result<Activation, CoreConversionError> {
    use core_activation::workflow_activation_job::Variant;
    let jobs = value
        .jobs
        .iter()
        .map(|job| {
            match job
                .variant
                .as_ref()
                .ok_or_else(|| invalid_core("Core activation job variant is absent"))?
            {
                Variant::InitializeWorkflow(value) => {
                    validate_initialize_subset(value)?;
                    Ok(ActivationJob::InitializeWorkflow {
                        workflow_id: value.workflow_id.clone(),
                        workflow_type: value.workflow_type.clone(),
                        arguments: value
                            .arguments
                            .iter()
                            .map(payload_from_core)
                            .collect::<Result<_, _>>()?,
                        randomness_seed: value.randomness_seed.to_string(),
                        attempt: value.attempt,
                        context: Some(InitializeContext {
                            headers: value
                                .headers
                                .iter()
                                .map(|(key, payload)| {
                                    Ok((key.clone(), payload_from_core(payload)?))
                                })
                                .collect::<Result<_, CoreConversionError>>()?,
                            identity: value.identity.clone(),
                            parent_workflow: value.parent_workflow_info.as_ref().map(|parent| {
                                NamespacedWorkflowExecution {
                                    namespace: parent.namespace.clone(),
                                    workflow_id: parent.workflow_id.clone(),
                                    run_id: parent.run_id.clone(),
                                }
                            }),
                            workflow_execution_timeout: value
                                .workflow_execution_timeout
                                .as_ref()
                                .map(duration_from_core)
                                .transpose()?,
                            workflow_run_timeout: value
                                .workflow_run_timeout
                                .as_ref()
                                .map(duration_from_core)
                                .transpose()?,
                            workflow_task_timeout: value
                                .workflow_task_timeout
                                .as_ref()
                                .map(duration_from_core)
                                .transpose()?,
                            first_execution_run_id: value.first_execution_run_id.clone(),
                            start_time: value.start_time.as_ref().map(|time| Timestamp {
                                seconds: time.seconds,
                                nanoseconds: time.nanos,
                            }),
                            root_workflow: value.root_workflow.as_ref().map(|root| {
                                WorkflowExecution {
                                    workflow_id: root.workflow_id.clone(),
                                    run_id: root.run_id.clone(),
                                }
                            }),
                            priority: value.priority.as_ref().map(|priority| WorkflowPriority {
                                priority_key: priority.priority_key,
                                fairness_key: priority.fairness_key.clone(),
                                fairness_weight_bits: priority.fairness_weight.to_bits(),
                            }),
                        }),
                    })
                }
                Variant::ResolveActivity(value) => {
                    // Core explicitly documents `is_local` as internal information the
                    // language SDK need not preserve. The result semantics are
                    // otherwise identical, so this is the sole intentionally ignored
                    // activation field in this slice.
                    Ok(ActivationJob::ResolveActivity {
                        seq: value.seq,
                        result: activity_resolution_from_core(
                            value
                                .result
                                .as_ref()
                                .ok_or_else(|| invalid_core("Core activity result is absent"))?,
                        )?,
                    })
                }
                Variant::FireTimer(value) => Ok(ActivationJob::FireTimer { seq: value.seq }),
                Variant::CancelWorkflow(value) => Ok(ActivationJob::CancelWorkflow {
                    reason: value.reason.clone(),
                }),
                Variant::RemoveFromCache(value) => Ok(ActivationJob::RemoveFromCache {
                    message: value.message.clone(),
                    reason: eviction_reason_from_core(value.reason)?,
                }),
                _ => Err(unsupported("Core activation job kind is not supported")),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let activation = Activation {
        run_id: value.run_id.clone(),
        timestamp: value.timestamp.as_ref().map(|timestamp| Timestamp {
            seconds: timestamp.seconds,
            nanoseconds: timestamp.nanos,
        }),
        is_replaying: value.is_replaying,
        history_length: value.history_length,
        jobs,
        metadata: Some(ActivationMetadata {
            available_internal_flags: value.available_internal_flags.clone(),
            history_size_bytes: value.history_size_bytes.to_string(),
            continue_as_new_suggested: value.continue_as_new_suggested,
            deployment_version_for_current_task: value
                .deployment_version_for_current_task
                .as_ref()
                .map(|version| WorkerDeploymentVersion {
                    deployment_name: version.deployment_name.clone(),
                    build_id: version.build_id.clone(),
                }),
            last_sdk_version: value.last_sdk_version.clone(),
            suggest_continue_as_new_reasons: value
                .suggest_continue_as_new_reasons
                .iter()
                .copied()
                .map(suggestion_from_core)
                .collect::<Result<_, _>>()?,
            target_worker_deployment_version_changed: value
                .target_worker_deployment_version_changed,
        }),
    };
    validate_activation(&activation)
        .map_err(|_| invalid_core("Core activation violates semantic protocol invariants"))?;
    Ok(activation)
}

/// Converts one semantic duration to the protobuf representation.
pub(crate) fn duration_to_core(value: Duration) -> prost_wkt_types::Duration {
    prost_wkt_types::Duration {
        seconds: value.seconds,
        nanos: value.nanoseconds,
    }
}

/// Converts and validates one protobuf duration.
pub(crate) fn duration_from_core(
    value: &prost_wkt_types::Duration,
) -> Result<Duration, CoreConversionError> {
    let duration = Duration {
        seconds: value.seconds,
        nanoseconds: value.nanos,
    };
    validate_time(duration.seconds, duration.nanoseconds, true, "$")
        .map_err(|_| invalid_core("Core duration is not normalized"))?;
    Ok(duration)
}

/// Maps an OCaml-facing cancellation choice to the pinned Core enum.
fn cancellation_to_core(value: ActivityCancellationType) -> i32 {
    use core_commands::ActivityCancellationType as Core;
    (match value {
        ActivityCancellationType::TryCancel => Core::TryCancel,
        ActivityCancellationType::WaitCancellationCompleted => Core::WaitCancellationCompleted,
        ActivityCancellationType::Abandon => Core::Abandon,
    }) as i32
}

/// Maps a pinned Core cancellation number without accepting unknown values.
fn cancellation_from_core(value: i32) -> Result<ActivityCancellationType, CoreConversionError> {
    use core_commands::ActivityCancellationType as Core;
    Ok(
        match Core::try_from(value).map_err(|_| invalid_core("unknown Core cancellation type"))? {
            Core::TryCancel => ActivityCancellationType::TryCancel,
            Core::WaitCancellationCompleted => ActivityCancellationType::WaitCancellationCompleted,
            Core::Abandon => ActivityCancellationType::Abandon,
        },
    )
}

/// Builds one official Core command with unsupported optional fields defaulted explicitly.
fn command_to_core(
    value: &CompletionCommand,
) -> Result<core_commands::WorkflowCommand, CoreConversionError> {
    use core_commands::workflow_command::Variant;
    let variant = match value {
        CompletionCommand::ScheduleActivity {
            seq,
            activity_id,
            activity_type,
            task_queue,
            arguments,
            schedule_to_close_timeout,
            schedule_to_start_timeout,
            start_to_close_timeout,
            heartbeat_timeout,
            cancellation_type,
            do_not_eagerly_execute,
        } => Variant::ScheduleActivity(core_commands::ScheduleActivity {
            seq: *seq,
            activity_id: activity_id.clone(),
            activity_type: activity_type.clone(),
            task_queue: task_queue.clone(),
            headers: Default::default(),
            arguments: arguments
                .iter()
                .map(payload_to_core)
                .collect::<Result<_, _>>()?,
            schedule_to_close_timeout: schedule_to_close_timeout.map(duration_to_core),
            schedule_to_start_timeout: schedule_to_start_timeout.map(duration_to_core),
            start_to_close_timeout: start_to_close_timeout.map(duration_to_core),
            heartbeat_timeout: heartbeat_timeout.map(duration_to_core),
            retry_policy: None,
            cancellation_type: cancellation_to_core(*cancellation_type),
            do_not_eagerly_execute: *do_not_eagerly_execute,
            versioning_intent: 0,
            priority: None,
        }),
        CompletionCommand::RequestCancelActivity { seq } => {
            Variant::RequestCancelActivity(core_commands::RequestCancelActivity { seq: *seq })
        }
        CompletionCommand::StartChildWorkflow {
            seq,
            workflow_id,
            workflow_type,
            input,
        } => Variant::StartChildWorkflowExecution(core_commands::StartChildWorkflowExecution {
            seq: *seq,
            workflow_id: workflow_id.clone(),
            workflow_type: workflow_type.clone(),
            input: input
                .iter()
                .map(payload_to_core)
                .collect::<Result<_, _>>()?,
            // The current OCaml command carries no task queue, namespace, or
            // child policy options. Defaulting the remaining Core fields is
            // deliberate and documented by the semantic schema.
            ..Default::default()
        }),
        CompletionCommand::StartTimer {
            seq,
            start_to_fire_timeout,
        } => Variant::StartTimer(core_commands::StartTimer {
            seq: *seq,
            start_to_fire_timeout: Some(duration_to_core(*start_to_fire_timeout)),
        }),
        CompletionCommand::CancelTimer { seq } => {
            Variant::CancelTimer(core_commands::CancelTimer { seq: *seq })
        }
        CompletionCommand::CompleteWorkflow { result } => {
            Variant::CompleteWorkflowExecution(core_commands::CompleteWorkflowExecution {
                result: result.as_ref().map(payload_to_core).transpose()?,
            })
        }
        CompletionCommand::FailWorkflow { failure } => {
            Variant::FailWorkflowExecution(core_commands::FailWorkflowExecution {
                failure: Some(failure_to_core(failure)?),
            })
        }
        CompletionCommand::CancelWorkflow => {
            Variant::CancelWorkflowExecution(core_commands::CancelWorkflowExecution {})
        }
    };
    Ok(core_commands::WorkflowCommand {
        variant: Some(variant),
        user_metadata: None,
    })
}

/// Converts one supported Core command while rejecting every lossy option.
fn command_from_core(
    value: &core_commands::WorkflowCommand,
) -> Result<CompletionCommand, CoreConversionError> {
    use core_commands::workflow_command::Variant;
    if value.user_metadata.is_some() {
        return Err(unsupported("Core command user metadata is not supported"));
    }
    match value
        .variant
        .as_ref()
        .ok_or_else(|| invalid_core("Core command variant is absent"))?
    {
        Variant::ScheduleActivity(value) => {
            if !value.headers.is_empty()
                || value.retry_policy.is_some()
                || value.versioning_intent != 0
                || value.priority.is_some()
            {
                return Err(unsupported(
                    "Core schedule activity options are not supported",
                ));
            }
            Ok(CompletionCommand::ScheduleActivity {
                seq: value.seq,
                activity_id: value.activity_id.clone(),
                activity_type: value.activity_type.clone(),
                task_queue: value.task_queue.clone(),
                arguments: value
                    .arguments
                    .iter()
                    .map(payload_from_core)
                    .collect::<Result<_, _>>()?,
                schedule_to_close_timeout: value
                    .schedule_to_close_timeout
                    .as_ref()
                    .map(duration_from_core)
                    .transpose()?,
                schedule_to_start_timeout: value
                    .schedule_to_start_timeout
                    .as_ref()
                    .map(duration_from_core)
                    .transpose()?,
                start_to_close_timeout: value
                    .start_to_close_timeout
                    .as_ref()
                    .map(duration_from_core)
                    .transpose()?,
                heartbeat_timeout: value
                    .heartbeat_timeout
                    .as_ref()
                    .map(duration_from_core)
                    .transpose()?,
                cancellation_type: cancellation_from_core(value.cancellation_type)?,
                do_not_eagerly_execute: value.do_not_eagerly_execute,
            })
        }
        Variant::StartChildWorkflowExecution(value) => {
            if !value.namespace.is_empty()
                || !value.task_queue.is_empty()
                || value.workflow_execution_timeout.is_some()
                || value.workflow_run_timeout.is_some()
                || value.workflow_task_timeout.is_some()
                || value.parent_close_policy != 0
                || value.workflow_id_reuse_policy != 0
                || value.retry_policy.is_some()
                || !value.cron_schedule.is_empty()
                || !value.headers.is_empty()
                || !value.memo.is_empty()
                || value.search_attributes.is_some()
                || value.cancellation_type != 0
                || value.versioning_intent != 0
                || value.priority.is_some()
            {
                return Err(unsupported(
                    "Core child workflow options are not represented by this protocol",
                ));
            }
            Ok(CompletionCommand::StartChildWorkflow {
                seq: value.seq,
                workflow_id: value.workflow_id.clone(),
                workflow_type: value.workflow_type.clone(),
                input: value
                    .input
                    .iter()
                    .map(payload_from_core)
                    .collect::<Result<_, _>>()?,
            })
        }
        Variant::RequestCancelActivity(value) => {
            Ok(CompletionCommand::RequestCancelActivity { seq: value.seq })
        }
        Variant::StartTimer(value) => Ok(CompletionCommand::StartTimer {
            seq: value.seq,
            start_to_fire_timeout: duration_from_core(
                value
                    .start_to_fire_timeout
                    .as_ref()
                    .ok_or_else(|| invalid_core("Core timer timeout is absent"))?,
            )?,
        }),
        Variant::CancelTimer(value) => Ok(CompletionCommand::CancelTimer { seq: value.seq }),
        Variant::CompleteWorkflowExecution(value) => Ok(CompletionCommand::CompleteWorkflow {
            result: value.result.as_ref().map(payload_from_core).transpose()?,
        }),
        Variant::FailWorkflowExecution(value) => Ok(CompletionCommand::FailWorkflow {
            failure: failure_from_core(
                value
                    .failure
                    .as_ref()
                    .ok_or_else(|| invalid_core("Core fail command has no failure"))?,
            )?,
        }),
        Variant::CancelWorkflowExecution(_) => Ok(CompletionCommand::CancelWorkflow),
        _ => Err(unsupported("Core workflow command kind is not supported")),
    }
}

/// Converts semantic commands to the official successful activation completion.
pub fn completion_to_core(
    value: &Completion,
) -> Result<core_completion::WorkflowActivationCompletion, CoreConversionError> {
    validate_completion(value)
        .map_err(|_| invalid_core("semantic completion violates protocol invariants"))?;
    Ok(core_completion::WorkflowActivationCompletion {
        run_id: value.run_id.clone(),
        status: Some(
            core_completion::workflow_activation_completion::Status::Successful(
                core_completion::Success {
                    commands: value
                        .commands
                        .iter()
                        .map(command_to_core)
                        .collect::<Result<_, _>>()?,
                    used_internal_flags: Vec::new(),
                    versioning_behavior: 0,
                },
            ),
        ),
    })
}

/// Converts a completion while enforcing activation-dependent invariants.
///
/// Core guarantees eviction activations contain only `RemoveFromCache`; the
/// language side must acknowledge those activations with no workflow commands.
/// Keeping this check beside conversion prevents accidental workflow execution
/// during cache teardown.
pub fn completion_to_core_for_activation(
    activation: &Activation,
    completion: &Completion,
) -> Result<core_completion::WorkflowActivationCompletion, CoreConversionError> {
    if activation.run_id != completion.run_id {
        return Err(invalid_core(
            "completion run id does not match its activation",
        ));
    }
    let eviction = matches!(
        activation.jobs.as_slice(),
        [ActivationJob::RemoveFromCache { .. }]
    );
    if eviction && !completion.commands.is_empty() {
        return Err(invalid_core(
            "cache eviction activation must have an empty completion",
        ));
    }
    completion_to_core(completion)
}

/// Converts a successful official completion without silently dropping flags or status.
pub fn completion_from_core(
    value: &core_completion::WorkflowActivationCompletion,
) -> Result<Completion, CoreConversionError> {
    use core_completion::workflow_activation_completion::Status;
    let success = match value
        .status
        .as_ref()
        .ok_or_else(|| invalid_core("Core completion status is absent"))?
    {
        Status::Successful(success) => success,
        Status::Failed(_) => {
            return Err(unsupported(
                "failed activation completion is not represented by this command protocol",
            ));
        }
    };
    if !success.used_internal_flags.is_empty() || success.versioning_behavior != 0 {
        return Err(unsupported(
            "Core completion metadata is not represented by this protocol slice",
        ));
    }
    let completion = Completion {
        run_id: value.run_id.clone(),
        commands: success
            .commands
            .iter()
            .map(command_from_core)
            .collect::<Result<_, _>>()?,
    };
    validate_completion(&completion)
        .map_err(|_| invalid_core("Core completion violates semantic protocol invariants"))?;
    Ok(completion)
}
