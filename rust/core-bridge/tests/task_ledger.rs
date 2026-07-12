use ocaml_temporal_core_bridge::worker_bridge::{
    ActivityAdmission, Admission, CompleteError, TaskLedger, bridge_task_types,
};

/// A workflow activation is admitted once and remains outstanding until its
/// matching run identifier is completed.
#[test]
fn workflow_run_ids_are_unique_and_completion_is_exact() {
    let mut ledger = TaskLedger::new();

    assert_eq!(ledger.admit_workflow("run-1"), Ok(Admission::New));
    assert_eq!(ledger.admit_workflow("run-1"), Ok(Admission::Duplicate));
    assert_eq!(
        ledger.complete_workflow("unknown"),
        Err(CompleteError::UnknownWorkflow)
    );
    assert_eq!(ledger.outstanding_workflows(), 1);
    assert_eq!(ledger.lease_workflow("run-1"), Ok(()));
    assert_eq!(ledger.complete_workflow("run-1"), Ok(()));
    assert_eq!(ledger.outstanding_workflows(), 0);
}

/// A cancellation polled before the owner leases the start reuses the original
/// task token and therefore still creates only one completion obligation.
#[test]
fn cancellation_before_start_lease_reuses_one_completion_debt() {
    let mut ledger = TaskLedger::new();
    let token = b"opaque-task-token";

    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Start),
        Ok(Admission::New)
    );
    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Cancel),
        Ok(Admission::ExistingCancellation)
    );
    assert_eq!(ledger.outstanding_activities(), 1);
    assert_eq!(ledger.lease_activity(token), Ok(()));
    assert_eq!(ledger.complete_activity(token), Ok(()));
    assert_eq!(ledger.outstanding_activities(), 0);
}

/// A cancellation can already be queued when the owner completes its start.
/// Leasing the stale update must fail without creating a second completion
/// debt, which is the state the poll-lane handoff treats as advisory.
#[test]
fn cancellation_after_start_completion_has_no_second_completion_debt() {
    let mut ledger = TaskLedger::new();
    let token = b"opaque-task-token";

    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Start),
        Ok(Admission::New)
    );
    assert_eq!(ledger.lease_activity(token), Ok(()));
    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Cancel),
        Ok(Admission::ExistingCancellation)
    );
    assert_eq!(ledger.complete_activity(token), Ok(()));
    assert_eq!(ledger.outstanding_activities(), 0);
    assert_eq!(
        ledger.lease_activity(token),
        Err(CompleteError::UnknownActivity)
    );
    assert_eq!(ledger.outstanding_activities(), 0);
}

/// A cancellation observed while the start is still leased cannot be leased
/// independently. The start remains the sole completion owner until it ends.
#[test]
fn cancellation_during_start_lease_does_not_create_second_completion_debt() {
    let mut ledger = TaskLedger::new();
    let token = b"opaque-task-token";

    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Start),
        Ok(Admission::New)
    );
    assert_eq!(ledger.lease_activity(token), Ok(()));
    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Cancel),
        Ok(Admission::ExistingCancellation)
    );
    assert_eq!(
        ledger.lease_activity(token),
        Err(CompleteError::AlreadyLeased)
    );
    assert_eq!(ledger.outstanding_activities(), 1);
    assert_eq!(ledger.complete_activity(token), Ok(()));
    assert_eq!(ledger.outstanding_activities(), 0);
}

/// Shutdown closes poll admission immediately, while preserving the ability to
/// finish work already leased to OCaml before final Core worker destruction.
#[test]
fn draining_rejects_new_tasks_until_existing_tasks_complete() {
    let mut ledger = TaskLedger::new();
    assert_eq!(ledger.admit_workflow("run-1"), Ok(Admission::New));

    ledger.begin_draining();
    assert!(ledger.admit_workflow("run-2").is_err());
    assert!(!ledger.can_finalize());
    assert_eq!(ledger.lease_workflow("run-1"), Ok(()));
    assert_eq!(ledger.complete_workflow("run-1"), Ok(()));
    assert!(ledger.can_finalize());
}

/// Core may issue cancellation for an already admitted activity while worker
/// shutdown is draining; that update must remain deliverable without admitting
/// any new completion obligation.
#[test]
fn draining_accepts_cancellation_for_an_existing_activity() {
    let mut ledger = TaskLedger::new();
    let token = b"activity";
    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Start),
        Ok(Admission::New)
    );
    ledger.begin_draining();

    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Cancel),
        Ok(Admission::ExistingCancellation)
    );
    assert!(
        ledger
            .admit_activity(b"new", ActivityAdmission::Start)
            .is_err()
    );
}

/// Dispose cleanup must be able to harvest every outstanding identity once so
/// force-fail completions cannot miss a leased or unleased residual entry.
#[test]
fn take_all_outstanding_drains_workflow_and_activity_debt() {
    let mut ledger = TaskLedger::new();
    assert_eq!(ledger.admit_workflow("run-1"), Ok(Admission::New));
    assert_eq!(ledger.lease_workflow("run-1"), Ok(()));
    let token = b"activity-token";
    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Start),
        Ok(Admission::New)
    );
    let (workflows, activities) = ledger.take_all_outstanding();
    assert_eq!(workflows, vec!["run-1".to_owned()]);
    assert_eq!(activities, vec![token.to_vec()]);
    assert_eq!(ledger.outstanding(), 0);
    assert!(!ledger.can_finalize()); // still Open, not Draining
    ledger.begin_draining();
    assert!(ledger.can_finalize());
}

/// A second lease for the same identity is rejected rather than silently
/// overwriting ownership.
#[test]
fn double_lease_is_rejected() {
    let mut ledger = TaskLedger::new();
    assert_eq!(ledger.admit_workflow("run-1"), Ok(Admission::New));
    assert_eq!(ledger.lease_workflow("run-1"), Ok(()));
    assert_eq!(
        ledger.lease_workflow("run-1"),
        Err(CompleteError::AlreadyLeased)
    );
    let token = b"activity-token";
    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Start),
        Ok(Admission::New)
    );
    assert_eq!(ledger.lease_activity(token), Ok(()));
    assert_eq!(
        ledger.lease_activity(token),
        Err(CompleteError::AlreadyLeased)
    );
}

/// A dequeued activation that fails lease handoff must be removable from the
/// ledger while still unleased so force-fail cleanup cannot leave phantom debt.
#[test]
fn abandon_admission_removes_only_unleased_entries() {
    let mut ledger = TaskLedger::new();
    assert_eq!(ledger.admit_workflow("run-1"), Ok(Admission::New));
    ledger.abandon_workflow_admission("run-1");
    assert_eq!(ledger.outstanding_workflows(), 0);

    assert_eq!(ledger.admit_workflow("run-2"), Ok(Admission::New));
    assert_eq!(ledger.lease_workflow("run-2"), Ok(()));
    ledger.abandon_workflow_admission("run-2");
    assert_eq!(ledger.outstanding_workflows(), 1);
    assert_eq!(ledger.complete_workflow("run-2"), Ok(()));

    let token = b"activity-token";
    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Start),
        Ok(Admission::New)
    );
    ledger.abandon_activity_admission(token);
    assert_eq!(ledger.outstanding_activities(), 0);
}

/// Empty task identities are rejected before they can become keys whose
/// ownership or completion cannot be explained at the native boundary.
#[test]
fn task_identity_validation_rejects_empty_values() {
    let mut ledger = TaskLedger::new();

    assert!(ledger.admit_workflow("").is_err());
    assert!(
        ledger
            .admit_activity(&[], ActivityAdmission::Start)
            .is_err()
    );
    assert_eq!(ledger.outstanding(), 0);
}

/// A completion cannot consume a task that remains in Rust's ready queue and
/// has never crossed the lease handoff to the OCaml supervisor.
#[test]
fn completion_requires_a_prior_lease_handoff() {
    let mut ledger = TaskLedger::new();
    assert_eq!(ledger.admit_workflow("run-1"), Ok(Admission::New));

    assert_eq!(
        ledger.complete_workflow("run-1"),
        Err(CompleteError::NotLeased)
    );
    assert_eq!(ledger.outstanding(), 1);
}

/// A duplicate workflow poll cannot revoke the existing OCaml lease and make
/// a legitimate completion appear forged.
#[test]
fn duplicate_workflow_admission_preserves_lease_state() {
    let mut ledger = TaskLedger::new();
    assert_eq!(ledger.admit_workflow("run-1"), Ok(Admission::New));
    assert_eq!(ledger.lease_workflow("run-1"), Ok(()));

    assert_eq!(ledger.admit_workflow("run-1"), Ok(Admission::Duplicate));
    assert_eq!(ledger.complete_workflow("run-1"), Ok(()));
}

/// A workflow activation rejected during Rust-to-semantic conversion was never
/// visible to OCaml. Retiring that exact lease prevents shutdown from waiting
/// for a completion the language runtime cannot possibly construct.
#[test]
fn rejected_workflow_conversion_retires_the_inaccessible_lease() {
    let mut ledger = TaskLedger::new();
    assert_eq!(
        ledger.admit_workflow("unrepresentable-run"),
        Ok(Admission::New)
    );
    assert_eq!(ledger.lease_workflow("unrepresentable-run"), Ok(()));

    assert_eq!(
        ledger.retire_rejected_workflow("unrepresentable-run"),
        Ok(())
    );
    ledger.begin_draining();
    assert!(ledger.can_finalize());
    assert_eq!(
        ledger.retire_rejected_workflow("unrepresentable-run"),
        Err(CompleteError::UnknownWorkflow)
    );
}

/// Remote activity conversion failure follows the same one-shot ownership
/// rule, using the opaque Core token rather than a workflow run identifier.
#[test]
fn rejected_activity_conversion_retires_the_inaccessible_token() {
    let mut ledger = TaskLedger::new();
    let token = b"unrepresentable-activity";
    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Start),
        Ok(Admission::New)
    );
    assert_eq!(ledger.lease_activity(token), Ok(()));

    assert_eq!(ledger.retire_rejected_activity(token), Ok(()));
    ledger.begin_draining();
    assert!(ledger.can_finalize());
    assert_eq!(
        ledger.retire_rejected_activity(token),
        Err(CompleteError::UnknownActivity)
    );
}

/// Decode drift may reject workflow and activity deliveries concurrently with
/// shutdown. Retiring both exact leased identities must make the draining
/// ledger finalizable without fabricating a completion from OCaml.
#[test]
fn rejected_language_deliveries_unblock_shutdown_drainage() {
    let mut ledger = TaskLedger::new();
    let token = b"decode-drift-activity";
    assert_eq!(
        ledger.admit_workflow("decode-drift-run"),
        Ok(Admission::New)
    );
    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Start),
        Ok(Admission::New)
    );
    assert_eq!(ledger.lease_workflow("decode-drift-run"), Ok(()));
    assert_eq!(ledger.lease_activity(token), Ok(()));
    ledger.begin_draining();
    assert!(!ledger.can_finalize());

    assert_eq!(ledger.retire_rejected_workflow("decode-drift-run"), Ok(()));
    assert_eq!(ledger.retire_rejected_activity(token), Ok(()));
    assert!(ledger.can_finalize());
}

/// Rejection is correlated by exact native identity. A changed run ID or task
/// token must not consume the genuine lease that remains owed during shutdown.
#[test]
fn changed_rejection_identities_do_not_retire_real_leases() {
    let mut ledger = TaskLedger::new();
    let token = b"real-activity-token";
    assert_eq!(ledger.admit_workflow("real-run"), Ok(Admission::New));
    assert_eq!(
        ledger.admit_activity(token, ActivityAdmission::Start),
        Ok(Admission::New)
    );
    assert_eq!(ledger.lease_workflow("real-run"), Ok(()));
    assert_eq!(ledger.lease_activity(token), Ok(()));

    assert_eq!(
        ledger.retire_rejected_workflow("changed-run"),
        Err(CompleteError::UnknownWorkflow)
    );
    assert_eq!(
        ledger.retire_rejected_activity(b"changed-activity-token"),
        Err(CompleteError::UnknownActivity)
    );
    assert_eq!(ledger.outstanding(), 2);

    assert_eq!(ledger.retire_rejected_workflow("real-run"), Ok(()));
    assert_eq!(ledger.retire_rejected_activity(token), Ok(()));
    ledger.begin_draining();
    assert!(ledger.can_finalize());
}

/// The acceptance worker polls only workflows and remote activities; enabling
/// local activities or Nexus would create unimplemented completion paths.
#[test]
fn bridge_enables_only_supported_core_task_types() {
    let task_types = bridge_task_types();

    assert!(task_types.enable_workflows);
    assert!(task_types.enable_remote_activities);
    assert!(!task_types.enable_local_activities);
    assert!(!task_types.enable_nexus);
}
