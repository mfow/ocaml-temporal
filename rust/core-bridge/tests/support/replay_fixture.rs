//! Shared replay history fixture for ABI integration tests.
//!
//! The fixture is deliberately built from Temporal's generated protobuf types
//! rather than copied base64. That keeps the test readable and makes a change
//! to the history shape visible in the event declarations below.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use prost::Message;
use prost_wkt_types::{Duration, Timestamp};
use temporalio_common::protos::temporal::api::{
    common::v1::WorkflowType,
    enums::v1::EventType,
    history::v1::{
        History, HistoryEvent, WorkflowExecutionCompletedEventAttributes,
        WorkflowExecutionStartedEventAttributes, WorkflowTaskCompletedEventAttributes,
        WorkflowTaskScheduledEventAttributes, WorkflowTaskStartedEventAttributes, history_event,
    },
    taskqueue::v1::TaskQueue,
};

/// Gives each synthetic history event a deterministic, monotonically
/// increasing timestamp. Core uses these timestamps when constructing replay
/// activations and otherwise turns an incomplete fixture into an eviction.
fn event_time(event_id: i64) -> Option<Timestamp> {
    Some(Timestamp {
        seconds: event_id,
        nanos: 0,
    })
}

/// Supplies the ten-second timeout metadata that Core expects for a normal
/// workflow task. Keeping it explicit avoids protobuf's empty-message
/// encoding being mistaken for an omitted timeout during replay.
fn task_timeout() -> Option<Duration> {
    Some(Duration {
        seconds: 10,
        nanos: 0,
    })
}

/// Builds a small terminal history accepted by Core's replay invariants.
fn complete_history() -> History {
    let started = HistoryEvent {
        event_id: 1,
        event_time: event_time(1),
        event_type: EventType::WorkflowExecutionStarted as i32,
        attributes: Some(
            history_event::Attributes::WorkflowExecutionStartedEventAttributes(
                WorkflowExecutionStartedEventAttributes {
                    workflow_type: Some(WorkflowType {
                        name: "replay-test".to_owned(),
                    }),
                    task_queue: Some(TaskQueue {
                        name: "replay-test".to_owned(),
                        ..Default::default()
                    }),
                    workflow_task_timeout: task_timeout(),
                    original_execution_run_id: "run-replay-test".to_owned(),
                    first_execution_run_id: "run-replay-test".to_owned(),
                    attempt: 1,
                    first_workflow_task_backoff: Some(Duration::default()),
                    ..Default::default()
                },
            ),
        ),
        ..Default::default()
    };
    let scheduled = HistoryEvent {
        event_id: 2,
        event_time: event_time(2),
        event_type: EventType::WorkflowTaskScheduled as i32,
        attributes: Some(
            history_event::Attributes::WorkflowTaskScheduledEventAttributes(
                WorkflowTaskScheduledEventAttributes {
                    task_queue: Some(TaskQueue {
                        name: "replay-test".to_owned(),
                        ..Default::default()
                    }),
                    start_to_close_timeout: task_timeout(),
                    attempt: 1,
                },
            ),
        ),
        ..Default::default()
    };
    let task_started = HistoryEvent {
        event_id: 3,
        event_time: event_time(3),
        event_type: EventType::WorkflowTaskStarted as i32,
        attributes: Some(
            history_event::Attributes::WorkflowTaskStartedEventAttributes(
                WorkflowTaskStartedEventAttributes {
                    identity: "replay-test-worker".to_owned(),
                    scheduled_event_id: 2,
                    ..Default::default()
                },
            ),
        ),
        ..Default::default()
    };
    let task_completed = HistoryEvent {
        event_id: 4,
        event_time: event_time(4),
        event_type: EventType::WorkflowTaskCompleted as i32,
        attributes: Some(
            history_event::Attributes::WorkflowTaskCompletedEventAttributes(
                WorkflowTaskCompletedEventAttributes {
                    scheduled_event_id: 2,
                    ..Default::default()
                },
            ),
        ),
        ..Default::default()
    };
    let completed = HistoryEvent {
        event_id: 5,
        event_time: event_time(5),
        event_type: EventType::WorkflowExecutionCompleted as i32,
        attributes: Some(
            history_event::Attributes::WorkflowExecutionCompletedEventAttributes(
                WorkflowExecutionCompletedEventAttributes {
                    workflow_task_completed_event_id: 4,
                    ..Default::default()
                },
            ),
        ),
        ..Default::default()
    };
    History {
        events: vec![started, scheduled, task_started, task_completed, completed],
    }
}

/// Builds a history whose first workflow task is still open.
///
/// A replay activation from this history still represents an active Core
/// workflow task, so a rejection completion is a valid exercise of the
/// bridge's semantic lease path. The terminal fixture above intentionally
/// remains separate because it is used by lifecycle tests that acknowledge a
/// completed replay with an empty completion.
fn open_workflow_task_history() -> History {
    let mut history = complete_history();
    history.events.truncate(3);
    history
}

/// Encodes the terminal fixture in the exact JSON envelope accepted by the
/// private replay ABI. The workflow ID is JSON-escaped so callers can use a
/// normal string without weakening the fixture's document validation.
pub fn complete_history_document(workflow_id: &str) -> String {
    let history = STANDARD.encode(complete_history().encode_to_vec());
    let workflow_id = serde_json::to_string(workflow_id)
        .expect("fixture workflow ID should always be JSON encodable");
    format!(
        "{{\"workflow_id\":{workflow_id},\"history\":{{\"encoding\":\"base64\",\"data\":\"{history}\"}}}}"
    )
}

/// Encodes the non-terminal fixture used when testing semantic rejection of
/// an activation that Core still expects the bridge to complete.
pub fn open_workflow_task_document(workflow_id: &str) -> String {
    let history = STANDARD.encode(open_workflow_task_history().encode_to_vec());
    let workflow_id = serde_json::to_string(workflow_id)
        .expect("fixture workflow ID should always be JSON encodable");
    format!(
        "{{\"workflow_id\":{workflow_id},\"history\":{{\"encoding\":\"base64\",\"data\":\"{history}\"}}}}"
    )
}
