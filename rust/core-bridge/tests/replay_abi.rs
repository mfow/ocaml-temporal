use std::ptr;

use ocaml_temporal_core_bridge::{
    ABI_VERSION, Result as AbiResult, STATUS_INVALID_ARGUMENT, STATUS_INVALID_STATE, STATUS_OK,
    STATUS_PROTOCOL, ocaml_temporal_core_v1_replay_worker_complete_workflow_json,
    ocaml_temporal_core_v1_replay_worker_dispose,
    ocaml_temporal_core_v1_replay_worker_feed_history_json,
    ocaml_temporal_core_v1_replay_worker_finalize,
    ocaml_temporal_core_v1_replay_worker_finish_input,
    ocaml_temporal_core_v1_replay_worker_reject_workflow_json,
    ocaml_temporal_core_v1_replay_worker_start_json,
    ocaml_temporal_core_v1_replay_worker_try_poll_workflow,
    ocaml_temporal_core_v1_replay_worker_wait_workflow, ocaml_temporal_core_v1_result_free,
    ocaml_temporal_core_v1_runtime_free, ocaml_temporal_core_v1_runtime_new,
};

/// Produces the initialized result storage required by every C ABI call.
fn empty_result() -> AbiResult {
    AbiResult::default()
}

/// Releases a result after asserting the operation's status. Keeping this in
/// one helper makes every branch exercise the same result-buffer ownership
/// contract rather than accidentally leaking an error allocation.
fn assert_status(result: &mut AbiResult, expected: i32) {
    assert_eq!(result.status, expected);
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_result_free(result) },
        STATUS_OK
    );
}

/// Uses the same bounded worker settings as a normal replay worker. The values
/// are intentionally independent of a Temporal endpoint because replay feeds
/// histories directly into Core and never opens a network client.
fn worker_config() -> &'static [u8] {
    br#"{"namespace":"default","task_queue":"replay","build_id":"abi-test","max_cached_workflows":0,"max_outstanding_workflow_tasks":1,"max_concurrent_workflow_task_polls":1,"graceful_shutdown_timeout_ms":1000}"#
}

#[test]
/// Every replay symbol rejects a null runtime before dereferencing input or
/// touching native state. This is the C boundary's minimum memory-safety gate.
fn replay_exports_reject_null_runtime_handles() {
    let mut result = empty_result();
    let history = br#"{}"#;
    let completion = br#"{}"#;
    macro_rules! check {
        ($call:expr) => {{
            let status = unsafe { $call };
            assert_eq!(status, STATUS_INVALID_ARGUMENT);
            assert_status(&mut result, STATUS_INVALID_ARGUMENT);
        }};
    }
    check!(ocaml_temporal_core_v1_replay_worker_start_json(
        ptr::null_mut(),
        worker_config().as_ptr(),
        worker_config().len(),
        &mut result
    ));
    check!(ocaml_temporal_core_v1_replay_worker_feed_history_json(
        ptr::null_mut(),
        history.as_ptr(),
        history.len(),
        &mut result
    ));
    check!(ocaml_temporal_core_v1_replay_worker_finish_input(
        ptr::null_mut(),
        &mut result
    ));
    check!(ocaml_temporal_core_v1_replay_worker_try_poll_workflow(
        ptr::null_mut(),
        &mut result
    ));
    check!(ocaml_temporal_core_v1_replay_worker_wait_workflow(
        ptr::null_mut(),
        &mut result
    ));
    check!(ocaml_temporal_core_v1_replay_worker_complete_workflow_json(
        ptr::null_mut(),
        completion.as_ptr(),
        completion.len(),
        &mut result
    ));
    check!(ocaml_temporal_core_v1_replay_worker_reject_workflow_json(
        ptr::null_mut(),
        completion.as_ptr(),
        completion.len(),
        &mut result
    ));
    check!(ocaml_temporal_core_v1_replay_worker_finalize(
        ptr::null_mut(),
        &mut result
    ));
    check!(ocaml_temporal_core_v1_replay_worker_dispose(
        ptr::null_mut(),
        &mut result
    ));
}

#[test]
/// Malformed history, completion, and rejection documents fail closed before
/// any replay worker is required. The same runtime can then be closed and
/// freed, proving rejected input did not leave a partial native graph behind.
fn malformed_replay_documents_are_rejected_without_state_leaks() {
    let mut runtime = ptr::null_mut();
    let mut result = empty_result();
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_runtime_new(&mut runtime, &mut result) },
        STATUS_OK
    );
    assert_status(&mut result, STATUS_OK);

    assert_eq!(
        unsafe {
            ocaml_temporal_core_v1_replay_worker_start_json(
                runtime,
                worker_config().as_ptr(),
                worker_config().len(),
                &mut result,
            )
        },
        STATUS_OK
    );
    assert_status(&mut result, STATUS_OK);

    let malformed_history =
        br#"{"workflow_id":"run","history":{"encoding":"base64","data":"not canonical"}}"#;
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v1_replay_worker_feed_history_json(
                runtime,
                malformed_history.as_ptr(),
                malformed_history.len(),
                &mut result,
            )
        },
        STATUS_PROTOCOL
    );
    assert_status(&mut result, STATUS_PROTOCOL);

    let malformed_completion = br#"{"run_id":"run","commands":[{"kind":"unknown"}]}"#;
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v1_replay_worker_complete_workflow_json(
                runtime,
                malformed_completion.as_ptr(),
                malformed_completion.len(),
                &mut result,
            )
        },
        STATUS_PROTOCOL
    );
    assert_status(&mut result, STATUS_PROTOCOL);

    assert_eq!(
        unsafe {
            ocaml_temporal_core_v1_replay_worker_reject_workflow_json(
                runtime,
                malformed_completion.as_ptr(),
                malformed_completion.len(),
                &mut result,
            )
        },
        STATUS_PROTOCOL
    );
    assert_status(&mut result, STATUS_PROTOCOL);

    assert_eq!(
        unsafe { ocaml_temporal_core_v1_replay_worker_dispose(runtime, &mut result) },
        STATUS_OK
    );
    assert_status(&mut result, STATUS_OK);

    assert_eq!(
        unsafe { ocaml_temporal_core_v1_runtime_free(&mut runtime) },
        STATUS_OK
    );
}

#[test]
/// Finishing, polling, waiting, and finalizing before a replay worker exists
/// all report the same closed lifecycle category and remain retry-safe.
fn replay_lifecycle_requires_a_started_worker() {
    let mut runtime = ptr::null_mut();
    let mut result = empty_result();
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_runtime_new(&mut runtime, &mut result) },
        STATUS_OK
    );
    assert_status(&mut result, STATUS_OK);

    for operation in ["try_poll", "wait", "finalize"] {
        let status = match operation {
            "try_poll" => unsafe {
                ocaml_temporal_core_v1_replay_worker_try_poll_workflow(runtime, &mut result)
            },
            "wait" => unsafe {
                ocaml_temporal_core_v1_replay_worker_wait_workflow(runtime, &mut result)
            },
            "finalize" => unsafe {
                ocaml_temporal_core_v1_replay_worker_finalize(runtime, &mut result)
            },
            _ => unreachable!(),
        };
        assert_eq!(status, STATUS_INVALID_STATE);
        assert_status(&mut result, STATUS_INVALID_STATE);
    }

    // Input closure is intentionally idempotent even when no worker exists;
    // this makes supervisor shutdown safe after a partially failed start.
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_replay_worker_finish_input(runtime, &mut result) },
        STATUS_OK
    );
    assert_status(&mut result, STATUS_OK);
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_replay_worker_dispose(runtime, &mut result) },
        STATUS_OK
    );
    assert_status(&mut result, STATUS_OK);

    assert_eq!(
        unsafe { ocaml_temporal_core_v1_runtime_free(&mut runtime) },
        STATUS_OK
    );
}

#[test]
/// Confirms the replay tests target the same ABI contract as the OCaml header.
fn replay_abi_tests_use_the_current_contract() {
    assert_eq!(ABI_VERSION, 1);
}
