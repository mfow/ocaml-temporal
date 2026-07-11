//! Closed JSON protocol and Core adapter for the first native client slice.
//!
//! This module deliberately exposes neither `temporalio_client::Client` nor
//! protobuf values to OCaml.  The bridge accepts a small, lossless request
//! document for starting a workflow and a request naming one exact run to
//! observe.  A wait never follows a continued-as-new successor: callers can
//! inspect the successor metadata and decide what to do in OCaml.

use serde::{Deserialize, Serialize};
use temporalio_client::Connection;
use temporalio_client::tonic::{Code, IntoRequest, Status};
use temporalio_common::protos::temporal::api::{
    common::v1::{Payloads, WorkflowExecution, WorkflowType},
    enums::v1::HistoryEventFilterType,
    history::v1::{HistoryEvent, history_event::Attributes},
    taskqueue::v1::TaskQueue,
    workflowservice::v1::{GetWorkflowExecutionHistoryRequest, StartWorkflowExecutionRequest},
};
use uuid::Uuid;

use crate::{protocol, workflow_protocol};

/// One exact execution identity carried by client responses.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionRef {
    /// Namespace containing the execution.
    pub namespace: String,
    /// Stable workflow identifier.
    pub workflow_id: String,
    /// Concrete run identifier.
    pub run_id: String,
}

/// Request to start one workflow execution with raw Temporal payloads.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartWorkflowRequest {
    /// Namespace in which Core should start the execution.
    pub namespace: String,
    /// User-supplied workflow identifier.
    pub workflow_id: String,
    /// Registered workflow type name.
    pub workflow_type: String,
    /// Normal task queue receiving the first workflow task.
    pub task_queue: String,
    /// Ordered input payloads; an empty vector means no workflow arguments.
    pub input: Vec<workflow_protocol::Payload>,
}

/// Successful result returned by the start operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartWorkflowResponse {
    /// Execution allocated by Temporal Server.
    pub execution: ExecutionRef,
}

/// Request to wait for one exact workflow run.
///
/// There is intentionally no `follow_runs` field.  This ABI operation has
/// fixed `follow_runs = false` semantics, so a continued-as-new event is a
/// terminal result for the requested run and cannot silently switch identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitWorkflowRequest {
    /// Namespace containing the execution.
    pub namespace: String,
    /// Stable workflow identifier.
    pub workflow_id: String,
    /// Concrete run identifier to observe without following successors.
    pub run_id: String,
}

/// Metadata for a successor run created by continued-as-new.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuccessorRef {
    /// Namespace inherited by the successor run.
    pub namespace: String,
    /// Workflow identifier inherited by the successor run.
    pub workflow_id: String,
    /// New concrete run identifier.
    pub run_id: String,
}

/// Terminal outcome of one exact workflow run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkflowOutcome {
    /// The run completed and returned zero or more payloads.
    Completed {
        /// Result payloads in the order returned by Temporal.
        result: Vec<workflow_protocol::Payload>,
        /// Successor metadata, if the completion event links to a successor.
        successor: Option<SuccessorRef>,
    },
    /// The run failed with a structured Temporal failure.
    Failed {
        /// Failure tree supplied by Temporal.
        failure: workflow_protocol::Failure,
        /// Successor metadata, if the failed event links to a successor.
        successor: Option<SuccessorRef>,
    },
    /// The run was cancelled by a request or workflow code.
    Cancelled {
        /// Cancellation details supplied by the workflow.
        details: Vec<workflow_protocol::Payload>,
    },
    /// The run was terminated by an operator or policy.
    Terminated {
        /// Termination details supplied by the operator.
        details: Vec<workflow_protocol::Payload>,
    },
    /// The run timed out.
    TimedOut {
        /// Successor metadata, if the timeout event links to a successor.
        successor: Option<SuccessorRef>,
    },
    /// The run continued as new and linked to the successor shown here.
    ContinuedAsNew {
        /// The next run in the execution chain.
        successor: SuccessorRef,
    },
}

/// Response containing the exact run identity and its terminal outcome.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitWorkflowResponse {
    /// Run whose close event was observed.
    pub execution: ExecutionRef,
    /// Terminal outcome of that run; no successor is followed automatically.
    pub outcome: WorkflowOutcome,
}

/// Structured native client operation failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientOperationError {
    /// Temporal rejected the requested workflow ID as already running or
    /// closed according to its conflict/reuse policy.
    AlreadyStarted {
        /// ID supplied in the start request.
        workflow_id: String,
        /// Existing run ID when Temporal supplied one in status details.
        existing_run_id: Option<String>,
    },
    /// A gRPC call failed.  Only the stable status code crosses the bridge;
    /// server text may contain user payloads and is intentionally discarded.
    Rpc {
        /// Lowercase tonic status code name.
        code: String,
    },
    /// Core returned an event or payload outside this closed semantic slice.
    Core(workflow_protocol::CoreConversionError),
}

/// Closed JSON body used when a start call reports `AlreadyStarted`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ClientErrorDocument {
    /// Existing workflow identity returned by Temporal status details.
    AlreadyStarted {
        /// Workflow ID that could not be started.
        workflow_id: String,
        /// Existing run ID, when status details contained one.
        existing_run_id: Option<String>,
    },
    /// Stable gRPC status code for non-AlreadyStarted failures.
    Rpc {
        /// Lowercase tonic status code name.
        code: String,
    },
    /// Stable category for a Core event that cannot cross the semantic bridge.
    Protocol {
        /// Closed Core conversion category.
        code: String,
    },
}

impl ClientOperationError {
    /// Encodes a privacy-safe structured error body for the ABI error buffer.
    pub(crate) fn to_json(&self) -> String {
        let document = match self {
            Self::AlreadyStarted {
                workflow_id,
                existing_run_id,
            } => ClientErrorDocument::AlreadyStarted {
                workflow_id: workflow_id.clone(),
                existing_run_id: existing_run_id.clone(),
            },
            Self::Rpc { code } => ClientErrorDocument::Rpc { code: code.clone() },
            Self::Core(error) => ClientErrorDocument::Protocol {
                code: match error.code {
                    workflow_protocol::CoreConversionErrorCode::Unsupported => {
                        "core_unsupported".to_owned()
                    }
                    workflow_protocol::CoreConversionErrorCode::InvalidCore => {
                        "core_invalid".to_owned()
                    }
                },
            },
        };
        // All variants contain validated identifiers or a bounded tonic code;
        // serialization cannot fail.  Keep a defensive fallback that cannot
        // expose a Rust panic through the ABI if a future variant changes.
        serde_json::to_string(&document)
            .unwrap_or_else(|_| "{\"kind\":\"rpc\",\"code\":\"internal\"}".to_owned())
    }
}

/// Strictly parses one start request after duplicate-key and byte-limit checks.
pub fn decode_start_request(input: &str) -> Result<StartWorkflowRequest, protocol::ProtocolError> {
    protocol::decode_payload_object(input)?;
    let request = serde_json::from_str(input)
        .map_err(|_| protocol::ProtocolError::invalid("$", "invalid client start request"))?;
    validate_start_request(&request)?;
    Ok(request)
}

/// Strictly parses one exact-run wait request.
pub fn decode_wait_request(input: &str) -> Result<WaitWorkflowRequest, protocol::ProtocolError> {
    protocol::decode_object(input)?;
    let request = serde_json::from_str(input)
        .map_err(|_| protocol::ProtocolError::invalid("$", "invalid client wait request"))?;
    validate_wait_request(&request)?;
    Ok(request)
}

/// Encodes and reparses a start response before ownership leaves Rust.
pub fn encode_start_response(
    response: &StartWorkflowResponse,
) -> Result<String, protocol::ProtocolError> {
    validate_execution(&response.execution, "$.execution")?;
    encode_document(response)
}

/// Encodes and reparses an exact-run wait response before ownership leaves Rust.
pub fn encode_wait_response(
    response: &WaitWorkflowResponse,
) -> Result<String, protocol::ProtocolError> {
    validate_execution(&response.execution, "$.execution")?;
    validate_outcome(&response.outcome)?;
    encode_document(response)
}

/// Starts one workflow through Core's raw workflow service trait.
pub async fn start_workflow(
    connection: Connection,
    request: StartWorkflowRequest,
) -> Result<StartWorkflowResponse, ClientOperationError> {
    let payloads = payloads_to_core(&request.input).map_err(ClientOperationError::Core)?;
    let workflow_id = request.workflow_id.clone();
    let mut service = connection.workflow_service();
    let response = service
        .start_workflow_execution(
            StartWorkflowExecutionRequest {
                namespace: request.namespace.clone(),
                input: Some(payloads),
                workflow_id: request.workflow_id,
                workflow_type: Some(WorkflowType {
                    name: request.workflow_type,
                }),
                task_queue: Some(TaskQueue {
                    name: request.task_queue,
                    kind: 0,
                    normal_name: String::new(),
                }),
                identity: connection.identity().to_owned(),
                request_id: Uuid::new_v4().to_string(),
                // The first slice intentionally uses server defaults for all
                // optional start policies; adding them is a later protocol
                // extension, not an implicit semantic default here.
                ..Default::default()
            }
            .into_request(),
        )
        .await
        .map_err(|status| map_status(&workflow_id, status))?
        .into_inner();

    let run_id = response.run_id;
    if run_id.is_empty() {
        return Err(ClientOperationError::Core(workflow_protocol::invalid_core(
            "Temporal start response omitted run ID",
        )));
    }
    let execution = ExecutionRef {
        namespace: request.namespace,
        workflow_id,
        run_id,
    };
    validate_execution(&execution, "$.execution").map_err(|_| {
        ClientOperationError::Core(workflow_protocol::invalid_core(
            "Temporal start response had invalid execution identity",
        ))
    })?;
    Ok(StartWorkflowResponse { execution })
}

/// Waits for the exact run named by `request`, never following successors.
pub async fn wait_workflow(
    connection: Connection,
    request: WaitWorkflowRequest,
) -> Result<WaitWorkflowResponse, ClientOperationError> {
    let execution = ExecutionRef {
        namespace: request.namespace.clone(),
        workflow_id: request.workflow_id.clone(),
        run_id: request.run_id.clone(),
    };
    let mut next_page_token = Vec::new();
    let mut service = connection.workflow_service();

    loop {
        let response = service
            .get_workflow_execution_history(
                GetWorkflowExecutionHistoryRequest {
                    namespace: request.namespace.clone(),
                    execution: Some(WorkflowExecution {
                        workflow_id: request.workflow_id.clone(),
                        run_id: request.run_id.clone(),
                    }),
                    next_page_token: std::mem::take(&mut next_page_token),
                    skip_archival: true,
                    wait_new_event: true,
                    history_event_filter_type: HistoryEventFilterType::CloseEvent as i32,
                    ..Default::default()
                }
                .into_request(),
            )
            .await
            .map_err(|status| map_status(&request.workflow_id, status))?
            .into_inner();

        if let Some(history) = response.history
            && let Some(event) = history.events.into_iter().last()
        {
            let outcome = outcome_from_event(event, &request.namespace, &request.workflow_id)?;
            let result = WaitWorkflowResponse { execution, outcome };
            validate_wait_response(&result).map_err(|_| {
                ClientOperationError::Core(workflow_protocol::invalid_core(
                    "Temporal close event violated client result invariants",
                ))
            })?;
            return Ok(result);
        }

        // Pagination tokens are returned for already-closed histories.  If a
        // long poll returns no close event, retry with an empty token and keep
        // the same run identity; this is the exact-run equivalent of Core's
        // `follow_runs = false` option.
        next_page_token = response.next_page_token;
    }
}

/// Converts a terminal history event without following a successor run.
fn outcome_from_event(
    event: HistoryEvent,
    namespace: &str,
    workflow_id: &str,
) -> Result<WorkflowOutcome, ClientOperationError> {
    let attributes = event.attributes.ok_or_else(|| {
        ClientOperationError::Core(workflow_protocol::invalid_core(
            "Temporal close event omitted attributes",
        ))
    })?;
    let successor = |run_id: &str| successor_ref(namespace, workflow_id, run_id);
    let outcome = match attributes {
        Attributes::WorkflowExecutionCompletedEventAttributes(attributes) => {
            WorkflowOutcome::Completed {
                result: payloads_from_core(attributes.result.as_ref())
                    .map_err(ClientOperationError::Core)?,
                successor: successor(&attributes.new_execution_run_id),
            }
        }
        Attributes::WorkflowExecutionFailedEventAttributes(attributes) => {
            let failure = attributes
                .failure
                .as_ref()
                .ok_or_else(|| {
                    ClientOperationError::Core(workflow_protocol::invalid_core(
                        "Temporal failed event omitted failure details",
                    ))
                })
                .and_then(|failure| {
                    workflow_protocol::failure_from_core(failure)
                        .map_err(ClientOperationError::Core)
                })?;
            WorkflowOutcome::Failed {
                failure,
                successor: successor(&attributes.new_execution_run_id),
            }
        }
        Attributes::WorkflowExecutionCanceledEventAttributes(attributes) => {
            WorkflowOutcome::Cancelled {
                details: payloads_from_core(attributes.details.as_ref())
                    .map_err(ClientOperationError::Core)?,
            }
        }
        Attributes::WorkflowExecutionTerminatedEventAttributes(attributes) => {
            WorkflowOutcome::Terminated {
                details: payloads_from_core(attributes.details.as_ref())
                    .map_err(ClientOperationError::Core)?,
            }
        }
        Attributes::WorkflowExecutionTimedOutEventAttributes(attributes) => {
            WorkflowOutcome::TimedOut {
                successor: successor(&attributes.new_execution_run_id),
            }
        }
        Attributes::WorkflowExecutionContinuedAsNewEventAttributes(attributes) => {
            let successor = successor(&attributes.new_execution_run_id).ok_or_else(|| {
                ClientOperationError::Core(workflow_protocol::invalid_core(
                    "Temporal continued-as-new event omitted successor run ID",
                ))
            })?;
            WorkflowOutcome::ContinuedAsNew { successor }
        }
        _ => {
            return Err(ClientOperationError::Core(workflow_protocol::unsupported(
                "Temporal returned an unsupported close event",
            )));
        }
    };
    Ok(outcome)
}

/// Converts Core payloads without dropping order or metadata.
fn payloads_from_core(
    value: Option<&Payloads>,
) -> Result<Vec<workflow_protocol::Payload>, workflow_protocol::CoreConversionError> {
    value
        .map(|payloads| {
            payloads
                .payloads
                .iter()
                .map(workflow_protocol::payload_from_core)
                .collect()
        })
        .unwrap_or_else(|| Ok(Vec::new()))
}

/// Converts semantic payloads into the protobuf payload container.
fn payloads_to_core(
    values: &[workflow_protocol::Payload],
) -> Result<Payloads, workflow_protocol::CoreConversionError> {
    Ok(Payloads {
        payloads: values
            .iter()
            .map(workflow_protocol::payload_to_core)
            .collect::<Result<_, _>>()?,
    })
}

/// Builds successor metadata only for a nonempty Core run ID.
fn successor_ref(namespace: &str, workflow_id: &str, run_id: &str) -> Option<SuccessorRef> {
    (!run_id.is_empty()).then(|| SuccessorRef {
        namespace: namespace.to_owned(),
        workflow_id: workflow_id.to_owned(),
        run_id: run_id.to_owned(),
    })
}

/// Maps a tonic status into the closed privacy-safe client error set.
fn map_status(workflow_id: &str, status: Status) -> ClientOperationError {
    if status.code() == Code::AlreadyExists {
        let existing_run_id = temporalio_common::protos::utilities::decode_status_detail::<
            temporalio_common::protos::temporal::api::errordetails::v1::WorkflowExecutionAlreadyStartedFailure,
        >(status.details())
        .map(|detail| detail.run_id)
        // Status details are server input too.  Do not reflect an empty or
        // oversized run ID into the ABI's bounded JSON error document.
        .filter(|run_id| !run_id.is_empty() && run_id.len() <= protocol::MAX_STRING_BYTES);
        ClientOperationError::AlreadyStarted {
            workflow_id: workflow_id.to_owned(),
            existing_run_id,
        }
    } else {
        ClientOperationError::Rpc {
            code: format!("{:?}", status.code()).to_ascii_lowercase(),
        }
    }
}

/// Validates one start request's identifiers and payload ownership bounds.
fn validate_start_request(value: &StartWorkflowRequest) -> Result<(), protocol::ProtocolError> {
    validate_identifier(&value.namespace, "$.namespace")?;
    validate_identifier(&value.workflow_id, "$.workflow_id")?;
    validate_identifier(&value.workflow_type, "$.workflow_type")?;
    validate_identifier(&value.task_queue, "$.task_queue")?;
    for payload in &value.input {
        workflow_protocol::payload_to_core(payload)
            .map_err(|_| protocol::ProtocolError::invalid("$.input", "invalid Temporal payload"))?;
    }
    Ok(())
}

/// Validates one exact-run wait request.
fn validate_wait_request(value: &WaitWorkflowRequest) -> Result<(), protocol::ProtocolError> {
    validate_identifier(&value.namespace, "$.namespace")?;
    validate_identifier(&value.workflow_id, "$.workflow_id")?;
    validate_identifier(&value.run_id, "$.run_id")?;
    Ok(())
}

/// Validates one execution reference.
fn validate_execution(value: &ExecutionRef, path: &str) -> Result<(), protocol::ProtocolError> {
    validate_identifier(&value.namespace, &format!("{path}.namespace"))?;
    validate_identifier(&value.workflow_id, &format!("{path}.workflow_id"))?;
    validate_identifier(&value.run_id, &format!("{path}.run_id"))?;
    Ok(())
}

/// Validates one outcome, including required continued-as-new successor data.
fn validate_outcome(value: &WorkflowOutcome) -> Result<(), protocol::ProtocolError> {
    let validate_successor = |successor: &SuccessorRef| {
        validate_identifier(&successor.namespace, "$.outcome.successor.namespace")?;
        validate_identifier(&successor.workflow_id, "$.outcome.successor.workflow_id")?;
        validate_identifier(&successor.run_id, "$.outcome.successor.run_id")
    };
    match value {
        WorkflowOutcome::Completed { result, successor } => {
            for payload in result {
                workflow_protocol::payload_to_core(payload).map_err(|_| {
                    protocol::ProtocolError::invalid("$.outcome.result", "invalid Temporal payload")
                })?;
            }
            if let Some(successor) = successor {
                validate_successor(successor)?;
            }
        }
        WorkflowOutcome::Failed { failure, successor } => {
            workflow_protocol::validate_failure(failure, "$.outcome.failure")?;
            if let Some(successor) = successor {
                validate_successor(successor)?;
            }
        }
        WorkflowOutcome::Cancelled { details } | WorkflowOutcome::Terminated { details } => {
            for payload in details {
                workflow_protocol::payload_to_core(payload).map_err(|_| {
                    protocol::ProtocolError::invalid(
                        "$.outcome.details",
                        "invalid Temporal payload",
                    )
                })?;
            }
        }
        WorkflowOutcome::TimedOut { successor } => {
            if let Some(successor) = successor {
                validate_successor(successor)?;
            }
        }
        WorkflowOutcome::ContinuedAsNew { successor } => validate_successor(successor)?,
    }
    Ok(())
}

/// Validates a complete wait response.
fn validate_wait_response(value: &WaitWorkflowResponse) -> Result<(), protocol::ProtocolError> {
    validate_execution(&value.execution, "$.execution")?;
    validate_outcome(&value.outcome)
}

/// Serializes, strictly reparses, and validates a client response.
fn encode_document<T: Serialize + for<'de> Deserialize<'de> + Clone + PartialEq>(
    value: &T,
) -> Result<String, protocol::ProtocolError> {
    let output = serde_json::to_string(value)
        .map_err(|_| protocol::ProtocolError::invalid("$", "could not encode client JSON"))?;
    protocol::decode_payload_object(&output)?;
    let reparsed: T = serde_json::from_str(&output)
        .map_err(|_| protocol::ProtocolError::invalid("$", "client JSON did not round trip"))?;
    // A second typed parse proves serde's closed-shape checks ran on output;
    // semantic validators are called by the operation-specific caller.
    if reparsed != *value {
        return Err(protocol::ProtocolError::invalid(
            "$",
            "client JSON did not preserve its typed value",
        ));
    }
    Ok(output)
}

/// Validates a bounded nonempty UTF-8 identifier.
fn validate_identifier(value: &str, path: &str) -> Result<(), protocol::ProtocolError> {
    if value.is_empty() || value.len() > protocol::MAX_STRING_BYTES {
        Err(protocol::ProtocolError::invalid(
            path,
            "identifier is empty or exceeds the protocol string safety limit",
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::STANDARD};

    /// Builds the smallest valid start document used by strict-parser tests.
    fn start_json() -> String {
        serde_json::json!({
            "namespace":"default",
            "workflow_id":"workflow-1",
            "workflow_type":"smoke",
            "task_queue":"queue",
            "input":[{"metadata":{},"data":{"encoding":"base64","data":STANDARD.encode(b"input")}}]
        })
        .to_string()
    }

    #[test]
    /// Rejects a syntactically valid request containing an unknown member.
    fn start_request_requires_closed_payload_shape() {
        let mut json = start_json();
        // Insert the unknown member before the existing closing brace so the
        // test exercises strict-field validation rather than JSON syntax.
        json.insert_str(json.len() - 1, ",\"unexpected\":true");
        assert!(decode_start_request(&json).is_err());
    }

    #[test]
    /// Rejects duplicate members before serde's map conversion can erase one.
    fn duplicate_start_member_is_rejected_before_serde_map_conversion() {
        let json = r#"{"namespace":"default","namespace":"other","workflow_id":"id","workflow_type":"type","task_queue":"queue","input":[]}"#;
        assert!(decode_start_request(json).is_err());
    }

    #[test]
    /// Rejects an attempted opt-in to successor following on exact-run waits.
    fn exact_wait_request_has_no_follow_runs_escape_hatch() {
        let json =
            r#"{"namespace":"default","workflow_id":"id","run_id":"run","follow_runs":true}"#;
        assert!(decode_wait_request(json).is_err());
    }

    #[test]
    /// Fails closed instead of polling forever when Core returns no attributes.
    fn close_event_without_attributes_is_rejected() {
        assert!(outcome_from_event(HistoryEvent::default(), "default", "id").is_err());
    }

    #[test]
    /// Converts every supported Core close event without following successors.
    fn terminal_history_events_map_to_closed_outcomes() {
        let cases = [
            (
                Attributes::WorkflowExecutionCompletedEventAttributes(Default::default()),
                WorkflowOutcome::Completed {
                    result: Vec::new(),
                    successor: None,
                },
            ),
            (
                Attributes::WorkflowExecutionCanceledEventAttributes(Default::default()),
                WorkflowOutcome::Cancelled {
                    details: Vec::new(),
                },
            ),
            (
                Attributes::WorkflowExecutionTerminatedEventAttributes(Default::default()),
                WorkflowOutcome::Terminated {
                    details: Vec::new(),
                },
            ),
            (
                Attributes::WorkflowExecutionTimedOutEventAttributes(Default::default()),
                WorkflowOutcome::TimedOut { successor: None },
            ),
            (
                Attributes::WorkflowExecutionContinuedAsNewEventAttributes(
                    temporalio_common::protos::temporal::api::history::v1::
                        WorkflowExecutionContinuedAsNewEventAttributes {
                        new_execution_run_id: "run-2".to_owned(),
                        ..Default::default()
                    },
                ),
                WorkflowOutcome::ContinuedAsNew {
                    successor: SuccessorRef {
                        namespace: "default".to_owned(),
                        workflow_id: "id".to_owned(),
                        run_id: "run-2".to_owned(),
                    },
                },
            ),
        ];
        for (attributes, expected) in cases {
            let event = HistoryEvent {
                attributes: Some(attributes),
                ..Default::default()
            };
            assert_eq!(
                outcome_from_event(event, "default", "id").unwrap(),
                expected
            );
        }
    }

    #[test]
    /// Rejects terminal Core events that omit required failure/successor data.
    fn malformed_terminal_history_events_fail_closed() {
        let failed = HistoryEvent {
            attributes: Some(Attributes::WorkflowExecutionFailedEventAttributes(
                Default::default(),
            )),
            ..Default::default()
        };
        assert!(outcome_from_event(failed, "default", "id").is_err());

        let continued = HistoryEvent {
            attributes: Some(Attributes::WorkflowExecutionContinuedAsNewEventAttributes(
                Default::default(),
            )),
            ..Default::default()
        };
        assert!(outcome_from_event(continued, "default", "id").is_err());
    }

    #[test]
    /// Confirms continued-as-new output retains the successor run identity.
    fn continued_as_new_response_preserves_successor_metadata() {
        let response = WaitWorkflowResponse {
            execution: ExecutionRef {
                namespace: "default".to_owned(),
                workflow_id: "id".to_owned(),
                run_id: "run-1".to_owned(),
            },
            outcome: WorkflowOutcome::ContinuedAsNew {
                successor: SuccessorRef {
                    namespace: "default".to_owned(),
                    workflow_id: "id".to_owned(),
                    run_id: "run-2".to_owned(),
                },
            },
        };
        let encoded = encode_wait_response(&response).unwrap();
        let decoded: WaitWorkflowResponse = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, response);
        assert!(encoded.contains("run-2"));
    }

    #[test]
    /// Keeps AlreadyStarted diagnostics closed and free of raw RPC text.
    fn already_started_error_is_closed_and_does_not_include_rpc_text() {
        let error = ClientOperationError::AlreadyStarted {
            workflow_id: "id".to_owned(),
            existing_run_id: Some("run".to_owned()),
        };
        let value: ClientErrorDocument = serde_json::from_str(&error.to_json()).unwrap();
        assert_eq!(
            value,
            ClientErrorDocument::AlreadyStarted {
                workflow_id: "id".to_owned(),
                existing_run_id: Some("run".to_owned())
            }
        );
        assert!(!error.to_json().contains("server"));
    }

    #[test]
    /// Maps Core conversion categories to stable, closed error codes.
    fn core_conversion_errors_use_stable_codes() {
        let error =
            ClientOperationError::Core(workflow_protocol::unsupported("unsupported client option"));
        assert_eq!(
            error.to_json(),
            r#"{"kind":"protocol","code":"core_unsupported"}"#
        );
    }

    #[test]
    /// Accepts a workflow start with no argument payloads.
    fn empty_payload_vector_is_valid_for_no_argument_workflow() {
        let json = serde_json::json!({
            "namespace":"default",
            "workflow_id":"workflow-1",
            "workflow_type":"smoke",
            "task_queue":"queue",
            "input":[]
        })
        .to_string();
        assert_eq!(decode_start_request(&json).unwrap().input, Vec::new());
    }
}
