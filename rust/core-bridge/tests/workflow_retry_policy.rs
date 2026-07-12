use std::collections::BTreeMap;

use ocaml_temporal_core_bridge::workflow_protocol::{
    self, ActivityCancellationType, Completion, CompletionCommand, Duration, Payload, RetryPolicy,
};
use temporalio_protos::coresdk::workflow_completion;

/// Builds the smallest activity command that exercises an explicit retry
/// policy while retaining the same timeout and payload requirements as a real
/// workflow completion.
fn completion(policy: Option<RetryPolicy>) -> Completion {
    Completion {
        run_id: "run-1".to_owned(),
        commands: vec![CompletionCommand::ScheduleActivity {
            seq: 1,
            activity_id: "activity-1".to_owned(),
            activity_type: "example.activity".to_owned(),
            task_queue: "activities".to_owned(),
            arguments: vec![Payload {
                metadata: BTreeMap::new(),
                data: b"input".to_vec(),
            }],
            schedule_to_close_timeout: Some(Duration {
                seconds: 60,
                nanoseconds: 0,
            }),
            schedule_to_start_timeout: None,
            start_to_close_timeout: Some(Duration {
                seconds: 30,
                nanoseconds: 0,
            }),
            heartbeat_timeout: None,
            retry_policy: policy,
            cancellation_type: ActivityCancellationType::TryCancel,
            do_not_eagerly_execute: false,
        }],
    }
}

/// Confirms explicit retry policy values survive JSON and the official Core
/// protobuf boundary without converting the coefficient through a decimal
/// float representation.
#[test]
fn explicit_policy_round_trips_losslessly() {
    let policy = RetryPolicy {
        initial_interval: Duration {
            seconds: 2,
            nanoseconds: 0,
        },
        backoff_coefficient_bits: 1.5f64.to_bits().to_string(),
        maximum_interval: Duration {
            seconds: 60,
            nanoseconds: 0,
        },
        maximum_attempts: 3,
        non_retryable_error_types: vec!["InvalidInput".to_owned()],
    };
    let value = completion(Some(policy.clone()));
    let encoded = workflow_protocol::encode_completion(&value).unwrap();
    assert!(encoded.contains("\"backoff_coefficient_bits\""));
    assert!(!encoded.contains("\"backoff_coefficient\":"));
    assert_eq!(
        workflow_protocol::decode_completion(&encoded).unwrap(),
        value
    );

    let core = workflow_protocol::completion_to_core(&value).unwrap();
    let Some(workflow_completion::workflow_activation_completion::Status::Successful(success)) =
        core.status.as_ref()
    else {
        panic!("completion should be successful");
    };
    let Some(
        temporalio_protos::coresdk::workflow_commands::workflow_command::Variant::ScheduleActivity(
            activity,
        ),
    ) = success.commands[0].variant.as_ref()
    else {
        panic!("completion should contain an activity command");
    };
    let core_policy = activity.retry_policy.as_ref().expect("policy is explicit");
    assert_eq!(core_policy.backoff_coefficient.to_bits(), 1.5f64.to_bits());
    assert_eq!(core_policy.maximum_attempts, 3);
    assert_eq!(
        workflow_protocol::completion_from_core(&core).unwrap(),
        value
    );
}

/// Invalid policies are rejected before either semantic encoding or Core
/// conversion can observe them.
#[test]
fn policy_validation_is_strict_and_bilateral() {
    let policy = RetryPolicy {
        initial_interval: Duration {
            seconds: 1,
            nanoseconds: 0,
        },
        backoff_coefficient_bits: 1.5f64.to_bits().to_string(),
        maximum_interval: Duration {
            seconds: 2,
            nanoseconds: 0,
        },
        maximum_attempts: 2,
        non_retryable_error_types: Vec::new(),
    };
    let valid = workflow_protocol::encode_completion(&completion(Some(policy))).unwrap();
    for (from, to) in [
        (
            "\"backoff_coefficient_bits\":\"4609434218613702656\"",
            "\"backoff_coefficient_bits\":\"0\"",
        ),
        ("\"maximum_attempts\":2", "\"maximum_attempts\":-1"),
        (
            "\"initial_interval\":{\"nanoseconds\":0,\"seconds\":1}",
            "\"initial_interval\":{\"nanoseconds\":0,\"seconds\":0}",
        ),
        (
            "\"backoff_coefficient_bits\":\"4609434218613702656\"",
            "\"backoff_coefficient_bits\":\"18446744073709551615\"",
        ),
    ] {
        let invalid = valid.replace(from, to);
        assert!(
            workflow_protocol::decode_completion(&invalid).is_err(),
            "malformed policy was accepted: {to}"
        );
    }
}

/// A missing retry field is different from an explicit null field.  This
/// prevents an older or malformed producer from silently changing Temporal's
/// service-default policy.
#[test]
fn retry_policy_field_is_required_even_when_unset() {
    let value = completion(None);
    let encoded = workflow_protocol::encode_completion(&value).unwrap();
    assert!(encoded.contains("\"retry_policy\":null"));
    let omitted = encoded.replace(",\"retry_policy\":null", "");
    assert!(workflow_protocol::decode_completion(&omitted).is_err());
}
