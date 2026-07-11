use super::*;
use crate::activity_protocol::{
    ActivityCancel, ActivityCancelReason, ActivityTask, ActivityTaskVariant,
};
use crate::workflow_protocol::{Activation, Timestamp};

/// Builds the smallest ordinary activation needed to test retained identity
/// without involving Temporal Core or a network worker.
fn activation(run_id: &str, history_length: u32) -> Activation {
    Activation {
        run_id: run_id.to_owned(),
        timestamp: Some(Timestamp {
            seconds: 1,
            nanoseconds: 0,
        }),
        is_replaying: false,
        history_length,
        jobs: Vec::new(),
        metadata: None,
    }
}

/// Exact workflow JSON proves both correlation identity and immutable semantic
/// content; altering either must leave the pending value intact.
#[test]
fn workflow_rejection_requires_the_complete_retained_document() {
    let retained = activation("run-1", 1);
    let mut pending = HashMap::new();
    retain_workflow_activation(&mut pending, retained.clone()).expect("first activation");

    let changed_id = workflow_protocol::encode_activation(&activation("run-2", 1))
        .expect("changed identifier encodes");
    assert_eq!(
        workflow_rejection_run_id(&pending, changed_id.as_bytes())
            .expect_err("changed run ID must not correlate")
            .status,
        STATUS_PROTOCOL
    );
    let changed_content = workflow_protocol::encode_activation(&activation("run-1", 2))
        .expect("changed semantic content encodes");
    assert_eq!(
        workflow_rejection_run_id(&pending, changed_content.as_bytes())
            .expect_err("changed activation must not correlate")
            .status,
        STATUS_PROTOCOL
    );
    assert_eq!(pending.get("run-1"), Some(&retained));

    let exact = workflow_protocol::encode_activation(&retained).expect("retained value encodes");
    assert_eq!(
        workflow_rejection_run_id(&pending, exact.as_bytes()).expect("exact value correlates"),
        "run-1"
    );
}

/// A duplicate poll reports an invariant failure but cannot overwrite the
/// activation document against which completion or rejection is checked.
#[test]
fn duplicate_workflow_poll_preserves_the_original_activation() {
    let original = activation("run-1", 1);
    let mut pending = HashMap::new();
    retain_workflow_activation(&mut pending, original.clone()).expect("first activation");

    assert_eq!(
        retain_workflow_activation(&mut pending, activation("run-1", 2))
            .expect_err("duplicate must fail")
            .status,
        STATUS_INTERNAL
    );
    assert_eq!(pending.get("run-1"), Some(&original));
}

/// Activity rejection requires the full retained document, so neither a
/// changed token nor changed cancellation content can reach the ledger.
#[test]
fn activity_rejection_requires_the_complete_retained_document() {
    let task = ActivityTask {
        task_token: "AAEC".to_owned(),
        variant: ActivityTaskVariant::Cancel(ActivityCancel {
            reason: ActivityCancelReason::WorkerShutdown,
            details: None,
        }),
    };
    let mut pending = HashMap::new();
    retain_activity_task(&mut pending, vec![0, 1, 2], task.clone());
    let encoded = activity_protocol::encode_task(&task).expect("activity task encodes");

    assert_eq!(
        activity_rejection_token(&pending, encoded.as_bytes()).expect("exact task correlates"),
        vec![0, 1, 2]
    );

    let changed_token = ActivityTask {
        task_token: "AwQF".to_owned(),
        ..task.clone()
    };
    let changed_token =
        activity_protocol::encode_task(&changed_token).expect("changed token encodes");
    assert_eq!(
        activity_rejection_token(&pending, changed_token.as_bytes())
            .expect_err("changed token must not correlate")
            .status,
        STATUS_PROTOCOL
    );

    let changed_content = ActivityTask {
        variant: ActivityTaskVariant::Cancel(ActivityCancel {
            reason: ActivityCancelReason::Cancelled,
            details: None,
        }),
        ..task
    };
    let changed_content =
        activity_protocol::encode_task(&changed_content).expect("changed task encodes");
    assert_eq!(
        activity_rejection_token(&pending, changed_content.as_bytes())
            .expect_err("changed task content must not correlate")
            .status,
        STATUS_PROTOCOL
    );
    assert_eq!(pending.get(&vec![0, 1, 2]).map(Vec::len), Some(1));

    let cancellation_update = ActivityTask {
        task_token: "AAEC".to_owned(),
        variant: ActivityTaskVariant::Cancel(ActivityCancel {
            reason: ActivityCancelReason::Cancelled,
            details: None,
        }),
    };
    retain_activity_task(&mut pending, vec![0, 1, 2], cancellation_update.clone());
    let cancellation_update =
        activity_protocol::encode_task(&cancellation_update).expect("cancellation update encodes");
    assert_eq!(
        activity_rejection_token(&pending, cancellation_update.as_bytes())
            .expect("retained update correlates"),
        vec![0, 1, 2]
    );
    assert_eq!(pending.get(&vec![0, 1, 2]).map(Vec::len), Some(2));

    // Cancellation updates share one Temporal completion debt with the start
    // task, so retirement must clear every semantic document for that token.
    retire_activity_semantics(&mut pending, &[0, 1, 2]);
    assert!(pending.is_empty());
}
