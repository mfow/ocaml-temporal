//! Closed semantic JSON protocol for remote activity tasks and completions.
//!
//! Task tokens remain opaque canonical base64. Rust alone converts the pinned
//! Core protobufs; OCaml receives ordinary records and variants with every
//! execution field needed by an activity implementation.

use std::collections::BTreeMap;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};

use crate::protocol::{self, MAX_PAYLOAD_BYTES, ProtocolError};
use crate::workflow_protocol::{
    self, CoreConversionError, Duration, Failure, Payload, Timestamp, WorkflowExecution,
    WorkflowPriority,
};

/// One remote activity task delivered by Core.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivityTask {
    /// Opaque canonical-base64 task token used for completion correlation.
    pub task_token: String,
    /// Start or cancellation work associated with the token.
    pub variant: ActivityTaskVariant,
}

/// Closed set of remote activity task variants enabled by this worker.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ActivityTaskVariant {
    /// Begin one remote activity attempt.
    Start(Box<ActivityStart>),
    /// Request cancellation of an already running attempt.
    Cancel(ActivityCancel),
}

/// Complete execution context for a remote activity attempt.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivityStart {
    /// Namespace containing the requesting workflow.
    pub workflow_namespace: String,
    /// Type of the requesting workflow.
    pub workflow_type: String,
    /// Concrete workflow execution that scheduled the activity.
    pub workflow_execution: WorkflowExecution,
    /// Workflow-assigned activity identifier.
    pub activity_id: String,
    /// Registered activity type to invoke.
    pub activity_type: String,
    /// User headers delivered to activity interceptors.
    pub header_fields: BTreeMap<String, Payload>,
    /// Ordered activity arguments.
    pub input: Vec<Payload>,
    /// Details retained from the preceding heartbeat, if any.
    pub heartbeat_details: Vec<Payload>,
    /// Time the activity was first scheduled.
    #[serde(deserialize_with = "workflow_protocol::required_nullable")]
    pub scheduled_time: Option<Timestamp>,
    /// Time the current retry attempt was scheduled.
    #[serde(deserialize_with = "workflow_protocol::required_nullable")]
    pub current_attempt_scheduled_time: Option<Timestamp>,
    /// Time Core received this attempt from matching.
    #[serde(deserialize_with = "workflow_protocol::required_nullable")]
    pub started_time: Option<Timestamp>,
    /// One-based execution attempt number.
    pub attempt: u32,
    /// Deadline measured from the first schedule to final completion.
    #[serde(deserialize_with = "workflow_protocol::required_nullable")]
    pub schedule_to_close_timeout: Option<Duration>,
    /// Deadline measured from this attempt's start.
    #[serde(deserialize_with = "workflow_protocol::required_nullable")]
    pub start_to_close_timeout: Option<Duration>,
    /// Maximum interval permitted between activity heartbeats.
    #[serde(deserialize_with = "workflow_protocol::required_nullable")]
    pub heartbeat_timeout: Option<Duration>,
    /// Effective retry policy after Server-side normalization.
    #[serde(deserialize_with = "workflow_protocol::required_nullable")]
    pub retry_policy: Option<RetryPolicy>,
    /// Matching priority attached to the activity task.
    #[serde(deserialize_with = "workflow_protocol::required_nullable")]
    pub priority: Option<WorkflowPriority>,
    /// Run ID used only by standalone activity execution, otherwise empty.
    pub standalone_run_id: String,
}

/// Exact retry policy effective for this activity attempt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicy {
    /// Delay before the first retry.
    #[serde(deserialize_with = "workflow_protocol::required_nullable")]
    pub initial_interval: Option<Duration>,
    /// Raw IEEE-754 bits avoid fractional JSON and preserve Core exactly.
    pub backoff_coefficient_bits: String,
    /// Maximum delay permitted between attempts.
    #[serde(deserialize_with = "workflow_protocol::required_nullable")]
    pub maximum_interval: Option<Duration>,
    /// Maximum attempts, where zero means unlimited and one disables retries.
    pub maximum_attempts: i32,
    /// Exact application failure types that stop retrying.
    pub non_retryable_error_types: Vec<String>,
}

/// Activity cancellation context supplied by Core.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivityCancel {
    /// Primary Core cancellation reason.
    pub reason: ActivityCancelReason,
    /// Independent cancellation facts supplied by newer Core paths.
    #[serde(deserialize_with = "workflow_protocol::required_nullable")]
    pub details: Option<ActivityCancellationDetails>,
}

/// Stable semantic names for Core activity cancellation reasons.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityCancelReason {
    NotFound,
    Cancelled,
    TimedOut,
    WorkerShutdown,
    Paused,
    Reset,
}

/// Independent cancellation facts retained from Core.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivityCancellationDetails {
    /// Server no longer recognizes the activity.
    pub is_not_found: bool,
    /// Cancellation was explicitly requested.
    pub is_cancelled: bool,
    /// Activity execution was paused.
    pub is_paused: bool,
    /// A configured activity timeout elapsed.
    pub is_timed_out: bool,
    /// Worker graceful shutdown expired.
    pub is_worker_shutdown: bool,
    /// Activity was reset by an administrative operation.
    pub is_reset: bool,
}

/// One terminal response to an activity task token.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivityCompletion {
    /// Opaque token copied exactly from the leased activity task.
    pub task_token: String,
    /// Terminal result Core should apply to the activity attempt.
    pub result: ActivityCompletionResult,
}

/// Supported activity completion outcomes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ActivityCompletionResult {
    Completed {
        /// Optional Temporal payload returned by the activity.
        #[serde(deserialize_with = "workflow_protocol::required_nullable")]
        result: Option<Payload>,
    },
    /// Activity code failed with structured Temporal failure information.
    Failed {
        /// Failure reported to Core for retry-policy evaluation.
        failure: Failure,
    },
    /// Activity cooperatively accepted a cancellation request.
    Cancelled {
        /// Cancellation failure carrying any application details.
        failure: Failure,
    },
    /// External code retained the token and will complete through a client API.
    WillCompleteAsync,
}

/// Decodes and validates one strict activity-task document.
pub fn decode_task(input: &str) -> Result<ActivityTask, ProtocolError> {
    let value = protocol::decode_payload_object(input)?;
    let task: ActivityTask = serde_json::from_value(to_serde(value))
        .map_err(|_| ProtocolError::invalid("$", "invalid activity task semantics"))?;
    validate_task(&task)?;
    Ok(task)
}

/// Validates, normalizes, and reparses one outgoing activity task.
pub fn encode_task(value: &ActivityTask) -> Result<String, ProtocolError> {
    validate_task(value)?;
    let json = serde_json::to_value(value)
        .map_err(|_| ProtocolError::invalid("$", "could not encode activity task"))?;
    let output = protocol::encode_payload_object(&from_serde(json)?)?;
    decode_task(&output)?;
    Ok(output)
}

/// Decodes and validates one strict activity-completion document.
pub fn decode_completion(input: &str) -> Result<ActivityCompletion, ProtocolError> {
    let value = protocol::decode_payload_object(input)?;
    let completion: ActivityCompletion = serde_json::from_value(to_serde(value))
        .map_err(|_| ProtocolError::invalid("$", "invalid activity completion semantics"))?;
    validate_completion(&completion)?;
    Ok(completion)
}

/// Validates, normalizes, and reparses one outgoing activity completion.
pub fn encode_completion(value: &ActivityCompletion) -> Result<String, ProtocolError> {
    validate_completion(value)?;
    let json = serde_json::to_value(value)
        .map_err(|_| ProtocolError::invalid("$", "could not encode activity completion"))?;
    let output = protocol::encode_payload_object(&from_serde(json)?)?;
    decode_completion(&output)?;
    Ok(output)
}

/// Enforces token, identifier, time, and failure invariants.
fn validate_task(value: &ActivityTask) -> Result<(), ProtocolError> {
    decode_token(&value.task_token)?;
    if let ActivityTaskVariant::Start(start) = &value.variant {
        for (field, path) in [
            (&start.workflow_namespace, "$.variant.workflow_namespace"),
            (&start.workflow_type, "$.variant.workflow_type"),
            (
                &start.workflow_execution.workflow_id,
                "$.variant.workflow_execution.workflow_id",
            ),
            (
                &start.workflow_execution.run_id,
                "$.variant.workflow_execution.run_id",
            ),
            (&start.activity_id, "$.variant.activity_id"),
            (&start.activity_type, "$.variant.activity_type"),
        ] {
            workflow_protocol::identifier(field, path)?;
        }
        for time in [
            start.scheduled_time,
            start.current_attempt_scheduled_time,
            start.started_time,
        ]
        .into_iter()
        .flatten()
        {
            workflow_protocol::validate_time(
                time.seconds,
                time.nanoseconds,
                false,
                "$.variant.time",
            )?;
        }
        for duration in [
            start.schedule_to_close_timeout,
            start.start_to_close_timeout,
            start.heartbeat_timeout,
        ]
        .into_iter()
        .flatten()
        {
            workflow_protocol::validate_time(
                duration.seconds,
                duration.nanoseconds,
                true,
                "$.variant.timeout",
            )?;
        }
    }
    Ok(())
}

/// Enforces token and structured failure invariants before Core conversion.
fn validate_completion(value: &ActivityCompletion) -> Result<(), ProtocolError> {
    decode_token(&value.task_token)?;
    match &value.result {
        ActivityCompletionResult::Failed { failure }
        | ActivityCompletionResult::Cancelled { failure } => {
            workflow_protocol::validate_failure(failure, "$.result.failure")?
        }
        _ => {}
    }
    Ok(())
}

/// Strictly decodes one opaque canonical task token.
fn decode_token(value: &str) -> Result<Vec<u8>, ProtocolError> {
    let bytes = STANDARD.decode(value.as_bytes()).map_err(|_| {
        ProtocolError::invalid("$.task_token", "task token is not canonical base64")
    })?;
    if bytes.is_empty() || bytes.len() > MAX_PAYLOAD_BYTES || STANDARD.encode(&bytes) != value {
        return Err(ProtocolError::invalid(
            "$.task_token",
            "task token is not canonical base64",
        ));
    }
    Ok(bytes)
}

use temporalio_protos::{
    coresdk::{
        ActivityTaskCompletion as CoreCompletion, activity_result, activity_task as core_task,
    },
    temporal::api::common::v1 as api_common,
};

/// Converts an official remote activity task without exposing protobuf.
pub fn task_from_core(
    value: &core_task::ActivityTask,
) -> Result<ActivityTask, CoreConversionError> {
    use core_task::activity_task::Variant;
    let variant = match value
        .variant
        .as_ref()
        .ok_or_else(|| workflow_protocol::invalid_core("Core activity variant is absent"))?
    {
        Variant::Start(start) => {
            if start.is_local {
                return Err(workflow_protocol::unsupported(
                    "local activity task is not enabled",
                ));
            }
            let execution = start.workflow_execution.as_ref().ok_or_else(|| {
                workflow_protocol::invalid_core("activity workflow execution is absent")
            })?;
            ActivityTaskVariant::Start(Box::new(ActivityStart {
                workflow_namespace: start.workflow_namespace.clone(),
                workflow_type: start.workflow_type.clone(),
                workflow_execution: WorkflowExecution {
                    workflow_id: execution.workflow_id.clone(),
                    run_id: execution.run_id.clone(),
                },
                activity_id: start.activity_id.clone(),
                activity_type: start.activity_type.clone(),
                header_fields: start
                    .header_fields
                    .iter()
                    .map(|(k, v)| Ok((k.clone(), workflow_protocol::payload_from_core(v)?)))
                    .collect::<Result<_, CoreConversionError>>()?,
                input: start
                    .input
                    .iter()
                    .map(workflow_protocol::payload_from_core)
                    .collect::<Result<_, _>>()?,
                heartbeat_details: start
                    .heartbeat_details
                    .iter()
                    .map(workflow_protocol::payload_from_core)
                    .collect::<Result<_, _>>()?,
                scheduled_time: start.scheduled_time.as_ref().map(timestamp_from_core),
                current_attempt_scheduled_time: start
                    .current_attempt_scheduled_time
                    .as_ref()
                    .map(timestamp_from_core),
                started_time: start.started_time.as_ref().map(timestamp_from_core),
                attempt: start.attempt,
                schedule_to_close_timeout: start
                    .schedule_to_close_timeout
                    .as_ref()
                    .map(workflow_protocol::duration_from_core)
                    .transpose()?,
                start_to_close_timeout: start
                    .start_to_close_timeout
                    .as_ref()
                    .map(workflow_protocol::duration_from_core)
                    .transpose()?,
                heartbeat_timeout: start
                    .heartbeat_timeout
                    .as_ref()
                    .map(workflow_protocol::duration_from_core)
                    .transpose()?,
                retry_policy: start
                    .retry_policy
                    .as_ref()
                    .map(retry_policy_from_core)
                    .transpose()?,
                priority: start.priority.as_ref().map(|p| WorkflowPriority {
                    priority_key: p.priority_key,
                    fairness_key: p.fairness_key.clone(),
                    fairness_weight_bits: p.fairness_weight.to_bits(),
                }),
                standalone_run_id: start.run_id.clone(),
            }))
        }
        Variant::Cancel(cancel) => ActivityTaskVariant::Cancel(ActivityCancel {
            reason: cancel_reason_from_core(cancel.reason)?,
            details: cancel
                .details
                .as_ref()
                .map(|d| ActivityCancellationDetails {
                    is_not_found: d.is_not_found,
                    is_cancelled: d.is_cancelled,
                    is_paused: d.is_paused,
                    is_timed_out: d.is_timed_out,
                    is_worker_shutdown: d.is_worker_shutdown,
                    is_reset: d.is_reset,
                }),
        }),
    };
    let task = ActivityTask {
        task_token: STANDARD.encode(&value.task_token),
        variant,
    };
    validate_task(&task).map_err(|_| {
        workflow_protocol::invalid_core("Core activity task violates semantic protocol")
    })?;
    Ok(task)
}

/// Converts a validated semantic activity completion into Core protobuf.
pub fn completion_to_core(
    value: &ActivityCompletion,
) -> Result<CoreCompletion, CoreConversionError> {
    use activity_result::activity_execution_result::Status;
    validate_completion(value).map_err(|_| {
        workflow_protocol::invalid_core("activity completion violates semantic protocol")
    })?;
    let status = match &value.result {
        ActivityCompletionResult::Completed { result } => {
            Status::Completed(activity_result::Success {
                result: result
                    .as_ref()
                    .map(workflow_protocol::payload_to_core)
                    .transpose()?,
            })
        }
        ActivityCompletionResult::Failed { failure } => Status::Failed(activity_result::Failure {
            failure: Some(workflow_protocol::failure_to_core(failure)?),
        }),
        ActivityCompletionResult::Cancelled { failure } => {
            Status::Cancelled(activity_result::Cancellation {
                failure: Some(workflow_protocol::failure_to_core(failure)?),
            })
        }
        ActivityCompletionResult::WillCompleteAsync => {
            Status::WillCompleteAsync(activity_result::WillCompleteAsync {})
        }
    };
    Ok(CoreCompletion {
        task_token: decode_token(&value.task_token)
            .map_err(|_| workflow_protocol::invalid_core("activity task token is invalid"))?,
        result: Some(activity_result::ActivityExecutionResult {
            status: Some(status),
        }),
    })
}

/// Copies protobuf timestamp components exactly.
fn timestamp_from_core(value: &prost_wkt_types::Timestamp) -> Timestamp {
    Timestamp {
        seconds: value.seconds,
        nanoseconds: value.nanos,
    }
}

/// Copies effective Core retry policy without floating-point JSON.
fn retry_policy_from_core(
    value: &api_common::RetryPolicy,
) -> Result<RetryPolicy, CoreConversionError> {
    Ok(RetryPolicy {
        initial_interval: value
            .initial_interval
            .as_ref()
            .map(workflow_protocol::duration_from_core)
            .transpose()?,
        backoff_coefficient_bits: value.backoff_coefficient.to_bits().to_string(),
        maximum_interval: value
            .maximum_interval
            .as_ref()
            .map(workflow_protocol::duration_from_core)
            .transpose()?,
        maximum_attempts: value.maximum_attempts,
        non_retryable_error_types: value.non_retryable_error_types.clone(),
    })
}

/// Converts Core's numeric cancellation reason without accepting unknown values.
fn cancel_reason_from_core(value: i32) -> Result<ActivityCancelReason, CoreConversionError> {
    use core_task::ActivityCancelReason as Core;
    Ok(
        match Core::try_from(value)
            .map_err(|_| workflow_protocol::unsupported("unknown activity cancellation reason"))?
        {
            Core::NotFound => ActivityCancelReason::NotFound,
            Core::Cancelled => ActivityCancelReason::Cancelled,
            Core::TimedOut => ActivityCancelReason::TimedOut,
            Core::WorkerShutdown => ActivityCancelReason::WorkerShutdown,
            Core::Paused => ActivityCancelReason::Paused,
            Core::Reset => ActivityCancelReason::Reset,
        },
    )
}

/// Converts the duplicate-aware protocol tree into Serde's value tree.
fn to_serde(value: protocol::JsonValue) -> serde_json::Value {
    match value {
        protocol::JsonValue::Null => serde_json::Value::Null,
        protocol::JsonValue::Bool(value) => serde_json::Value::Bool(value),
        protocol::JsonValue::Signed(value) => value.into(),
        protocol::JsonValue::Unsigned(value) => value.into(),
        protocol::JsonValue::String(value) => serde_json::Value::String(value),
        protocol::JsonValue::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(to_serde).collect())
        }
        protocol::JsonValue::Object(fields) => {
            serde_json::Value::Object(fields.into_iter().map(|(k, v)| (k, to_serde(v))).collect())
        }
    }
}

/// Converts a Serde value into the normalized protocol tree.
fn from_serde(value: serde_json::Value) -> Result<protocol::JsonValue, ProtocolError> {
    Ok(match value {
        serde_json::Value::Null => protocol::JsonValue::Null,
        serde_json::Value::Bool(value) => protocol::JsonValue::Bool(value),
        serde_json::Value::Number(value) => value
            .as_i64()
            .map(protocol::JsonValue::Signed)
            .or_else(|| value.as_u64().map(protocol::JsonValue::Unsigned))
            .ok_or_else(|| ProtocolError::invalid("$", "number is not integral"))?,
        serde_json::Value::String(value) => protocol::JsonValue::String(value),
        serde_json::Value::Array(values) => protocol::JsonValue::Array(
            values
                .into_iter()
                .map(from_serde)
                .collect::<Result<_, _>>()?,
        ),
        serde_json::Value::Object(fields) => protocol::JsonValue::Object(
            fields
                .into_iter()
                .map(|(k, v)| Ok((k, from_serde(v)?)))
                .collect::<Result<_, ProtocolError>>()?,
        ),
    })
}
