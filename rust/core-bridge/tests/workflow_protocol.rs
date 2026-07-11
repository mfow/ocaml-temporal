use std::{collections::BTreeMap, fs, path::PathBuf};

use ocaml_temporal_core_bridge::workflow_protocol;
use temporalio_protos::coresdk::{
    common as core_common, workflow_activation as core_activation,
    workflow_commands as core_commands, workflow_completion as core_completion,
};

/// Locates one language-neutral semantic fixture in the repository.
fn fixture_path(parts: &[&str]) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.extend([
        "..",
        "..",
        "test",
        "bridge",
        "fixtures",
        "workflow-protocol",
    ]);
    path.extend(parts);
    path
}

/// Reads a shared fixture without embedding malformed source in diagnostics.
fn fixture(parts: &[&str]) -> String {
    fs::read_to_string(fixture_path(parts)).expect("workflow fixture must be readable")
}

/// Proves all first-slice activation jobs normalize identically to OCaml.
#[test]
fn accepts_and_normalizes_workflow_activations() {
    for name in [
        "activation",
        "eviction",
        "realistic-initialize",
        "child-initialize",
    ] {
        let input = fixture(&["valid", &format!("{name}.input.json")]);
        let expected = fixture(&["valid", &format!("{name}.normalized.json")]);
        let value = workflow_protocol::decode_activation(&input).unwrap();
        assert_eq!(
            workflow_protocol::encode_activation(&value).unwrap(),
            expected.trim()
        );
        workflow_protocol::decode_activation(expected.trim()).unwrap();
    }
}

/// Proves all first-slice completion commands retain their source order.
#[test]
fn accepts_and_normalizes_workflow_completion() {
    let input = fixture(&["valid", "completion.input.json"]);
    let expected = fixture(&["valid", "completion.normalized.json"]);
    let value = workflow_protocol::decode_completion(&input).unwrap();
    assert_eq!(
        workflow_protocol::encode_completion(&value).unwrap(),
        expected.trim()
    );
    workflow_protocol::decode_completion(expected.trim()).unwrap();
}

/// Proves semantic exactness and range rules independently of JSON structure.
#[test]
fn rejects_malformed_workflow_documents() {
    for name in [
        "activation-duplicate-field",
        "activation-unknown-job",
        "activation-seq-negative",
        "activation-seq-too-large",
        "activation-invalid-base64",
        "activation-missing-field",
        "activation-eviction-mixed",
    ] {
        assert!(
            workflow_protocol::decode_activation(&fixture(&["invalid", &format!("{name}.json")]))
                .is_err(),
            "{name} was accepted"
        );
    }
    for name in [
        "completion-unknown-command",
        "completion-terminal-not-last",
        "completion-invalid-duration",
        "completion-no-activity-timeout",
        "completion-unknown-nested",
        "completion-duplicate-field",
    ] {
        assert!(
            workflow_protocol::decode_completion(&fixture(&["invalid", &format!("{name}.json")]))
                .is_err(),
            "{name} was accepted"
        );
    }
}

/// Proves the pinned Core boundary preserves supported values and rejects
/// omitted metadata instead of silently dropping it.
#[test]
fn converts_pinned_core_values_losslessly() {
    use core_activation::workflow_activation_job::Variant;
    let activation = core_activation::WorkflowActivation {
        run_id: "run-1".to_owned(),
        timestamp: Some(prost_wkt_types::Timestamp {
            seconds: 12,
            nanos: 34,
        }),
        jobs: vec![core_activation::WorkflowActivationJob {
            variant: Some(Variant::FireTimer(core_activation::FireTimer { seq: 7 })),
        }],
        ..Default::default()
    };
    let semantic = workflow_protocol::activation_from_core(&activation).unwrap();
    assert!(matches!(
        semantic.jobs.as_slice(),
        [workflow_protocol::ActivationJob::FireTimer { seq: 7 }]
    ));

    let completion =
        workflow_protocol::decode_completion(&fixture(&["valid", "completion.input.json"]))
            .unwrap();
    let core = workflow_protocol::completion_to_core(&completion).unwrap();
    assert_eq!(
        workflow_protocol::completion_from_core(&core).unwrap(),
        completion
    );

    let mut unsupported_activation = activation.clone();
    unsupported_activation.jobs[0].variant = Some(Variant::InitializeWorkflow(
        core_activation::InitializeWorkflow {
            workflow_type: "workflow".to_owned(),
            workflow_id: "workflow-1".to_owned(),
            randomness_seed: 1,
            attempt: 1,
            continued_from_execution_run_id: "previous-run".to_owned(),
            ..Default::default()
        },
    ));
    assert_eq!(
        workflow_protocol::activation_from_core(&unsupported_activation)
            .unwrap_err()
            .code,
        workflow_protocol::CoreConversionErrorCode::Unsupported
    );

    let mut malformed_payload_activation = activation;
    malformed_payload_activation.jobs[0].variant = Some(Variant::InitializeWorkflow(
        core_activation::InitializeWorkflow {
            workflow_type: "workflow".to_owned(),
            workflow_id: "workflow-1".to_owned(),
            arguments: vec![temporalio_protos::temporal::api::common::v1::Payload {
                metadata: [(String::new(), Vec::new())].into(),
                ..Default::default()
            }],
            randomness_seed: 1,
            first_execution_run_id: "first-run".to_owned(),
            attempt: 1,
            ..Default::default()
        },
    ));
    assert_eq!(
        workflow_protocol::activation_from_core(&malformed_payload_activation)
            .unwrap_err()
            .code,
        workflow_protocol::CoreConversionErrorCode::InvalidCore
    );

    let mut unsupported_completion = core;
    let Some(core_completion::workflow_activation_completion::Status::Successful(success)) =
        unsupported_completion.status.as_mut()
    else {
        panic!("test completion must be successful");
    };
    success.commands[0].user_metadata = Some(Default::default());
    assert_eq!(
        workflow_protocol::completion_from_core(&unsupported_completion)
            .unwrap_err()
            .code,
        workflow_protocol::CoreConversionErrorCode::Unsupported
    );
}

/// Proves ordinary first-task metadata survives Core conversion and semantic
/// JSON instead of being rejected as an unsupported default-only initializer.
#[test]
fn preserves_realistic_first_workflow_activation() {
    use core_activation::workflow_activation_job::Variant;
    use temporalio_protos::coresdk::workflow_activation::remove_from_cache::EvictionReason;

    let initialize = core_activation::InitializeWorkflow {
        workflow_type: "order_workflow".to_owned(),
        workflow_id: "order-42".to_owned(),
        arguments: vec![Default::default()],
        randomness_seed: u64::MAX,
        headers: [("trace".to_owned(), Default::default())].into(),
        identity: "starter".to_owned(),
        workflow_execution_timeout: Some(prost_wkt_types::Duration {
            seconds: 3_600,
            nanos: 0,
        }),
        workflow_run_timeout: Some(prost_wkt_types::Duration {
            seconds: 600,
            nanos: 0,
        }),
        workflow_task_timeout: Some(prost_wkt_types::Duration {
            seconds: 10,
            nanos: 0,
        }),
        first_execution_run_id: "first-run".to_owned(),
        attempt: 1,
        start_time: Some(prost_wkt_types::Timestamp {
            seconds: 1_720_000_000,
            nanos: 123,
        }),
        priority: Some(temporalio_protos::temporal::api::common::v1::Priority {
            priority_key: 2,
            fairness_key: "tenant-a".to_owned(),
            fairness_weight: 1.5,
        }),
        ..Default::default()
    };
    let core = core_activation::WorkflowActivation {
        run_id: "run-1".to_owned(),
        timestamp: Some(prost_wkt_types::Timestamp {
            seconds: 1_720_000_001,
            nanos: 456,
        }),
        is_replaying: false,
        history_length: 3,
        jobs: vec![core_activation::WorkflowActivationJob {
            variant: Some(Variant::InitializeWorkflow(initialize)),
        }],
        available_internal_flags: vec![1, 2],
        history_size_bytes: 9_999,
        continue_as_new_suggested: true,
        deployment_version_for_current_task: Some(core_common::WorkerDeploymentVersion {
            deployment_name: "prod".to_owned(),
            build_id: "build-1".to_owned(),
        }),
        last_sdk_version: "1.2.3".to_owned(),
        suggest_continue_as_new_reasons: vec![1],
        target_worker_deployment_version_changed: true,
    };
    let semantic = workflow_protocol::activation_from_core(&core).unwrap();
    let context = match &semantic.jobs[0] {
        workflow_protocol::ActivationJob::InitializeWorkflow {
            context: Some(context),
            ..
        } => context,
        _ => panic!("realistic activation must retain initialization context"),
    };
    assert_eq!(context.identity, "starter");
    assert_eq!(
        context.priority.as_ref().unwrap().fairness_weight_bits,
        1.5_f32.to_bits()
    );
    assert_eq!(
        semantic.metadata.as_ref().unwrap().history_size_bytes,
        "9999"
    );
    let json = workflow_protocol::encode_activation(&semantic).unwrap();
    assert_eq!(
        workflow_protocol::decode_activation(&json).unwrap(),
        semantic
    );

    let mut unknown_suggestion = core;
    unknown_suggestion.suggest_continue_as_new_reasons = vec![999];
    assert_eq!(
        workflow_protocol::activation_from_core(&unknown_suggestion)
            .unwrap_err()
            .code,
        workflow_protocol::CoreConversionErrorCode::InvalidCore
    );

    // Keep the imported pinned enum exercised so an upstream name drift is a
    // compile-time failure in this boundary test.
    assert_eq!(EvictionReason::CacheFull as i32, 1);
}

/// Proves a child activation retains both its namespaced parent and the root
/// workflow execution instead of treating normal child metadata as unsupported.
#[test]
fn preserves_child_workflow_identity() {
    use core_activation::workflow_activation_job::Variant;
    use temporalio_protos::{
        coresdk::common::NamespacedWorkflowExecution, temporal::api::common::v1::WorkflowExecution,
    };

    let child = core_activation::InitializeWorkflow {
        workflow_type: "child_workflow".to_owned(),
        workflow_id: "child-1".to_owned(),
        randomness_seed: 42,
        parent_workflow_info: Some(NamespacedWorkflowExecution {
            namespace: "default".to_owned(),
            workflow_id: "parent-1".to_owned(),
            run_id: "parent-run".to_owned(),
        }),
        first_execution_run_id: "child-first-run".to_owned(),
        attempt: 1,
        root_workflow: Some(WorkflowExecution {
            workflow_id: "root-1".to_owned(),
            run_id: "root-run".to_owned(),
        }),
        ..Default::default()
    };
    let core = core_activation::WorkflowActivation {
        run_id: "child-run".to_owned(),
        timestamp: Some(prost_wkt_types::Timestamp::default()),
        jobs: vec![core_activation::WorkflowActivationJob {
            variant: Some(Variant::InitializeWorkflow(child)),
        }],
        ..Default::default()
    };
    let semantic = workflow_protocol::activation_from_core(&core).unwrap();
    let context = match &semantic.jobs[0] {
        workflow_protocol::ActivationJob::InitializeWorkflow {
            context: Some(context),
            ..
        } => context,
        _ => panic!("child activation must retain initialization context"),
    };
    assert_eq!(
        context.parent_workflow.as_ref().unwrap().namespace,
        "default"
    );
    assert_eq!(
        context.root_workflow.as_ref().unwrap().workflow_id,
        "root-1"
    );
    let encoded = workflow_protocol::encode_activation(&semantic).unwrap();
    assert_eq!(
        workflow_protocol::decode_activation(&encoded).unwrap(),
        semantic
    );
}

/// Proves absent oneofs and the eviction acknowledgement rule fail closed.
#[test]
fn enforces_core_absence_and_eviction_invariants() {
    let invalid = core_activation::WorkflowActivation {
        run_id: "run-1".to_owned(),
        timestamp: Some(prost_wkt_types::Timestamp::default()),
        jobs: vec![core_activation::WorkflowActivationJob { variant: None }],
        ..Default::default()
    };
    assert_eq!(
        workflow_protocol::activation_from_core(&invalid)
            .unwrap_err()
            .code,
        workflow_protocol::CoreConversionErrorCode::InvalidCore
    );

    let eviction =
        workflow_protocol::decode_activation(&fixture(&["valid", "eviction.input.json"])).unwrap();
    let nonempty = workflow_protocol::Completion {
        run_id: eviction.run_id.clone(),
        commands: vec![workflow_protocol::CompletionCommand::CancelTimer { seq: 1 }],
    };
    assert_eq!(
        workflow_protocol::completion_to_core_for_activation(&eviction, &nonempty)
            .unwrap_err()
            .code,
        workflow_protocol::CoreConversionErrorCode::InvalidCore
    );

    let absent_command = core_commands::WorkflowCommand {
        user_metadata: None,
        variant: None,
    };
    let core = core_completion::WorkflowActivationCompletion {
        run_id: "run-1".to_owned(),
        status: Some(
            core_completion::workflow_activation_completion::Status::Successful(
                core_completion::Success {
                    commands: vec![absent_command],
                    used_internal_flags: Vec::new(),
                    versioning_behavior: 0,
                },
            ),
        ),
    };
    assert_eq!(
        workflow_protocol::completion_from_core(&core)
            .unwrap_err()
            .code,
        workflow_protocol::CoreConversionErrorCode::InvalidCore
    );
}

/// Proves the official pinned-Core eviction constructor, including its absent
/// timestamp, crosses the semantic boundary without fabrication or rejection.
#[test]
fn converts_official_core_eviction_activation() {
    use temporalio_protos::coresdk::workflow_activation::{
        create_evict_activation, remove_from_cache::EvictionReason,
    };

    let core = create_evict_activation(
        "evicted-run".to_owned(),
        "cache is full".to_owned(),
        EvictionReason::CacheFull,
    );
    assert!(core.timestamp.is_none());
    let semantic = workflow_protocol::activation_from_core(&core).unwrap();
    assert!(semantic.timestamp.is_none());
    assert!(matches!(
        semantic.jobs.as_slice(),
        [workflow_protocol::ActivationJob::RemoveFromCache { .. }]
    ));
    let encoded = workflow_protocol::encode_activation(&semantic).unwrap();
    assert_eq!(
        workflow_protocol::decode_activation(&encoded).unwrap(),
        semantic
    );
}

/// Proves maximum payload representation does not weaken ordinary text limits.
#[test]
fn accepts_large_nested_payload_but_rejects_large_text() {
    let completion = workflow_protocol::Completion {
        run_id: "run-large".to_owned(),
        commands: vec![workflow_protocol::CompletionCommand::CompleteWorkflow {
            result: Some(workflow_protocol::Payload {
                metadata: BTreeMap::new(),
                data: vec![b'x'; 50_000],
            }),
        }],
    };
    let encoded = workflow_protocol::encode_completion(&completion).unwrap();
    assert_eq!(
        workflow_protocol::decode_completion(&encoded).unwrap(),
        completion
    );

    let oversized = "x".repeat(65_537);
    let activation = serde_json::json!({
        "run_id": "r",
        "timestamp": {"seconds": 0, "nanoseconds": 0},
        "is_replaying": false,
        "history_length": 0,
        "jobs": [{"kind": "cancel_workflow", "reason": oversized}]
    });
    assert!(workflow_protocol::decode_activation(&activation.to_string()).is_err());
}

/// Proves unordered input maps normalize identically in both implementations.
#[test]
fn canonicalizes_payload_metadata_keys() {
    let input = r#"{"run_id":"run-map","commands":[{"kind":"complete_workflow","result":{"metadata":{"z-key":{"encoding":"base64","data":"eg=="},"a-key":{"encoding":"base64","data":"YQ=="}},"data":{"encoding":"base64","data":""}}}]}"#;
    let value = workflow_protocol::decode_completion(input).unwrap();
    let output = workflow_protocol::encode_completion(&value).unwrap();
    assert!(output.find("\"a-key\"").unwrap() < output.find("\"z-key\"").unwrap());
}

/// Proves configurable Temporal identifiers can exceed the server's default
/// limit while still obeying the bridge's bounded-string safety policy.
#[test]
fn preserves_identifiers_above_255_bytes() {
    let long_id = "i".repeat(300);
    let completion = workflow_protocol::Completion {
        run_id: long_id.clone(),
        commands: Vec::new(),
    };
    let encoded = workflow_protocol::encode_completion(&completion).unwrap();
    assert_eq!(
        workflow_protocol::decode_completion(&encoded)
            .unwrap()
            .run_id,
        long_id
    );

    let oversized = workflow_protocol::Completion {
        run_id: "i".repeat(65_537),
        commands: Vec::new(),
    };
    assert!(workflow_protocol::encode_completion(&oversized).is_err());
}

/// Proves initialization is unique and first, and a missing timestamp cannot
/// escape the single synthetic-eviction exception.
#[test]
fn enforces_activation_cross_field_invariants() {
    let mut initialized = workflow_protocol::decode_activation(&fixture(&[
        "valid",
        "realistic-initialize.input.json",
    ]))
    .unwrap();
    let initialize = initialized.jobs[0].clone();
    initialized.jobs.push(initialize.clone());
    assert!(workflow_protocol::encode_activation(&initialized).is_err());

    initialized.jobs = vec![
        workflow_protocol::ActivationJob::FireTimer { seq: 0 },
        initialize,
    ];
    assert!(workflow_protocol::encode_activation(&initialized).is_err());

    initialized.jobs.truncate(1);
    initialized.timestamp = None;
    assert!(workflow_protocol::encode_activation(&initialized).is_err());

    initialized.timestamp = Some(workflow_protocol::Timestamp {
        seconds: 0,
        nanoseconds: 0,
    });
    initialized.jobs = vec![workflow_protocol::ActivationJob::FireTimer { seq: 0 }];
    let encoded = workflow_protocol::encode_activation(&initialized).unwrap();
    assert_eq!(
        workflow_protocol::decode_activation(&encoded).unwrap().jobs,
        initialized.jobs
    );
}

/// Proves collection accounting does not impose a smaller workflow-job limit
/// than the aggregate document boundary itself.
#[test]
fn accepts_more_than_256_small_activation_jobs() {
    let mut activation = workflow_protocol::decode_activation(&fixture(&[
        "valid",
        "realistic-initialize.input.json",
    ]))
    .unwrap();
    activation.jobs = (0..300)
        .map(|seq| workflow_protocol::ActivationJob::FireTimer { seq })
        .collect();
    let encoded = workflow_protocol::encode_activation(&activation).unwrap();
    assert_eq!(
        workflow_protocol::decode_activation(&encoded).unwrap(),
        activation
    );
}

/// Builds a failure whose nested info can be varied without obscuring the
/// boundary invariant under test.
fn failure_with_info(info: workflow_protocol::FailureInfo) -> workflow_protocol::Failure {
    workflow_protocol::Failure {
        message: String::new(),
        source: String::new(),
        stack_trace: String::new(),
        encoded_attributes: None,
        cause: None,
        info,
    }
}

/// Builds a recursive application-failure chain used to exercise the shared
/// parser's stack-safety depth boundary.
fn nested_application_failure(cause_count: usize) -> workflow_protocol::Failure {
    (0..cause_count).fold(
        failure_with_info(workflow_protocol::FailureInfo::Application {
            type_name: String::new(),
            non_retryable: false,
            details: Vec::new(),
        }),
        |cause, _| workflow_protocol::Failure {
            cause: Some(Box::new(cause)),
            ..failure_with_info(workflow_protocol::FailureInfo::Application {
                type_name: String::new(),
                non_retryable: false,
                details: Vec::new(),
            })
        },
    )
}

/// Proves recursive failures can exceed the former 16-level limit while the
/// serde_json-aligned stack-safety boundary still rejects hostile depth.
#[test]
fn enforces_recursive_failure_depth_safely() {
    let completion = |cause_count| workflow_protocol::Completion {
        run_id: "run-nested-failure".to_owned(),
        commands: vec![workflow_protocol::CompletionCommand::FailWorkflow {
            failure: nested_application_failure(cause_count),
        }],
    };
    workflow_protocol::encode_completion(&completion(32))
        .expect("32 recursive causes must fit the parser safety boundary");
    assert!(workflow_protocol::encode_completion(&completion(130)).is_err());
}

/// Proves application failure type follows the schema's bounded-text contract
/// and is not incorrectly treated as a nonempty Temporal identifier.
#[test]
fn accepts_application_failure_type_as_bounded_text() {
    let application = workflow_protocol::Completion {
        run_id: "run-failure".to_owned(),
        commands: vec![workflow_protocol::CompletionCommand::FailWorkflow {
            failure: failure_with_info(workflow_protocol::FailureInfo::Application {
                type_name: String::new(),
                non_retryable: false,
                details: Vec::new(),
            }),
        }],
    };
    workflow_protocol::encode_completion(&application)
        .expect("an empty application failure type is valid bounded text");
}

/// Proves activity failure event IDs and worker identity are validated after
/// payload-aware parsing has admitted the larger base64 string ceiling.
#[test]
fn rejects_invalid_activity_failure_fields() {
    let invalid_activity =
        |scheduled_event_id, started_event_id, identity: String| workflow_protocol::Completion {
            run_id: "run-failure".to_owned(),
            commands: vec![workflow_protocol::CompletionCommand::FailWorkflow {
                failure: failure_with_info(workflow_protocol::FailureInfo::Activity {
                    scheduled_event_id,
                    started_event_id,
                    identity,
                    activity_type: "activity".to_owned(),
                    activity_id: "activity-1".to_owned(),
                    retry_state: workflow_protocol::RetryState::Unspecified,
                }),
            }],
        };
    assert!(workflow_protocol::encode_completion(&invalid_activity(-1, 0, String::new())).is_err());
    assert!(workflow_protocol::encode_completion(&invalid_activity(0, -1, String::new())).is_err());
    assert!(
        workflow_protocol::encode_completion(&invalid_activity(0, 0, "i".repeat(65_537))).is_err()
    );
}

/// Proves relaxed payload parsing cannot bypass the normal header-key limit.
#[test]
fn validates_initialize_header_keys() {
    let mut activation = workflow_protocol::decode_activation(&fixture(&[
        "valid",
        "realistic-initialize.input.json",
    ]))
    .unwrap();
    let workflow_protocol::ActivationJob::InitializeWorkflow {
        context: Some(context),
        ..
    } = &mut activation.jobs[0]
    else {
        panic!("fixture must contain initialization context");
    };
    context.headers.insert(
        String::new(),
        workflow_protocol::Payload {
            metadata: BTreeMap::new(),
            data: Vec::new(),
        },
    );
    assert!(workflow_protocol::encode_activation(&activation).is_err());
}

/// Proves two server-default-sized payload byte fields, and their canonical
/// base64 expansion, fit in one validated semantic document.
#[test]
fn accepts_batched_default_temporal_payloads() {
    const DEFAULT_TEMPORAL_BLOB_BYTES: usize = 2 * 1024 * 1024;
    let bytes = vec![b'x'; DEFAULT_TEMPORAL_BLOB_BYTES];
    let completion = workflow_protocol::Completion {
        run_id: "run-batched-payloads".to_owned(),
        commands: vec![workflow_protocol::CompletionCommand::CompleteWorkflow {
            result: Some(workflow_protocol::Payload {
                metadata: [("second".to_owned(), bytes.clone())].into(),
                data: bytes,
            }),
        }],
    };
    let encoded = workflow_protocol::encode_completion(&completion).unwrap();
    assert_eq!(
        workflow_protocol::decode_completion(&encoded).unwrap(),
        completion
    );
}
