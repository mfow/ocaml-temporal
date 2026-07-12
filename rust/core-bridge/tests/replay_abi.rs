use std::ptr;

use ocaml_temporal_core_bridge::{
    ABI_VERSION, Buffer, Result as AbiResult, Runtime, STATUS_INVALID_ARGUMENT,
    STATUS_INVALID_STATE, STATUS_NOT_READY, STATUS_OK, STATUS_OUTSTANDING_TASKS, STATUS_PROTOCOL,
    ocaml_temporal_core_v1_replay_worker_complete_workflow_json,
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

#[path = "support/replay_fixture.rs"]
mod replay_fixture;

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

/// Copies a Rust-owned ABI buffer before the containing result is released.
/// The helper also asserts the canonical representation of an empty value,
/// which catches accidental zero-length allocations at the ownership edge.
fn buffer_bytes(buffer: &Buffer) -> Vec<u8> {
    if buffer.ptr.is_null() {
        assert_eq!(buffer.len, 0);
        Vec::new()
    } else {
        // SAFETY: The result owns this buffer until `result_free` is called;
        // the test copies it before releasing that ownership.
        unsafe { std::slice::from_raw_parts(buffer.ptr, buffer.len).to_vec() }
    }
}

/// Creates a runtime and starts its workflow-only replay worker for an ABI
/// test. The returned pointer remains exclusively owned by the caller until
/// it is passed to `ocaml_temporal_core_v1_runtime_free`.
fn new_replay_runtime() -> *mut Runtime {
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
    runtime
}

/// Waits for one replay activation and copies its JSON value out of the ABI.
/// Readiness is only a hint, so the helper retries both bounded waits and
/// empty polls instead of assuming that a successful wait consumed work.
fn poll_replay_activation(runtime: *mut Runtime) -> Vec<u8> {
    for _ in 0..20 {
        let mut result = empty_result();
        let wait_status =
            unsafe { ocaml_temporal_core_v1_replay_worker_wait_workflow(runtime, &mut result) };
        match wait_status {
            STATUS_OK | STATUS_NOT_READY => assert_status(&mut result, wait_status),
            status => {
                assert_status(&mut result, status);
                panic!("replay readiness wait failed with status {status}");
            }
        }

        let status =
            unsafe { ocaml_temporal_core_v1_replay_worker_try_poll_workflow(runtime, &mut result) };
        match status {
            STATUS_OK => {
                let value = buffer_bytes(&result.value);
                assert_status(&mut result, STATUS_OK);
                return value;
            }
            STATUS_NOT_READY => assert_status(&mut result, STATUS_NOT_READY),
            status => {
                assert_status(&mut result, status);
                panic!("replay activation poll failed with status {status}");
            }
        }
    }
    panic!("replay activation did not become available within the bounded test wait");
}

/// Drives the replay worker through its terminal readiness observation and
/// retries finalization until the native lane graph can be joined. No pending
/// activation is expected when this helper is called; a remaining debt is
/// therefore reported as a test failure by the bounded retry limit.
fn finalize_after_natural_shutdown(runtime: *mut Runtime) {
    for _ in 0..20 {
        let mut wait_result = empty_result();
        let wait_status = unsafe {
            ocaml_temporal_core_v1_replay_worker_wait_workflow(runtime, &mut wait_result)
        };
        match wait_status {
            STATUS_OK | STATUS_NOT_READY => assert_status(&mut wait_result, wait_status),
            status => {
                assert_status(&mut wait_result, status);
                panic!("replay shutdown wait failed with status {status}");
            }
        }

        let mut finalize_result = empty_result();
        let finalize_status =
            unsafe { ocaml_temporal_core_v1_replay_worker_finalize(runtime, &mut finalize_result) };
        match finalize_status {
            STATUS_OK => {
                assert_status(&mut finalize_result, STATUS_OK);
                return;
            }
            STATUS_OUTSTANDING_TASKS => {
                assert_status(&mut finalize_result, STATUS_OUTSTANDING_TASKS)
            }
            status => {
                assert_status(&mut finalize_result, status);
                panic!("replay finalization failed with status {status}");
            }
        }
    }
    panic!("replay worker did not finalize within the bounded shutdown wait");
}

/// A rejected replay workflow task causes Core to emit a follow-up cache
/// eviction activation. The bridge must still acknowledge that activation
/// before the replay feeder can shut down; otherwise finalization correctly
/// remains blocked by Core's completion debt.
fn complete_follow_up_eviction(runtime: *mut Runtime) {
    let activation = poll_replay_activation(runtime);
    let semantic: serde_json::Value =
        serde_json::from_slice(&activation).expect("eviction activation should be JSON");
    let run_id = semantic
        .get("run_id")
        .and_then(serde_json::Value::as_str)
        .expect("eviction activation should retain its run ID");
    let completion = serde_json::json!({"run_id": run_id, "commands": []}).to_string();
    let mut result = empty_result();
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v1_replay_worker_complete_workflow_json(
                runtime,
                completion.as_ptr(),
                completion.len(),
                &mut result,
            )
        },
        STATUS_OK
    );
    assert_status(&mut result, STATUS_OK);
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
/// Invalid input is rejected before Core admission, while a valid history
/// cannot be fed after the owner closes the bounded feeder. Explicit dispose
/// then proves those failed calls did not strand the worker graph.
fn replay_abi_rejects_invalid_history_and_closed_feeder() {
    let mut runtime = new_replay_runtime();
    let mut result = empty_result();
    let malformed =
        br#"{"workflow_id":"run","history":{"encoding":"base64","data":"not canonical"}}"#;
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v1_replay_worker_feed_history_json(
                runtime,
                malformed.as_ptr(),
                malformed.len(),
                &mut result,
            )
        },
        STATUS_PROTOCOL
    );
    assert_status(&mut result, STATUS_PROTOCOL);

    let document = replay_fixture::complete_history_document("workflow-replay-test");
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v1_replay_worker_feed_history_json(
                runtime,
                document.as_ptr(),
                document.len(),
                &mut result,
            )
        },
        STATUS_OK
    );
    assert_status(&mut result, STATUS_OK);

    assert_eq!(
        unsafe { ocaml_temporal_core_v1_replay_worker_finish_input(runtime, &mut result) },
        STATUS_OK
    );
    assert_status(&mut result, STATUS_OK);
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v1_replay_worker_feed_history_json(
                runtime,
                document.as_ptr(),
                document.len(),
                &mut result,
            )
        },
        STATUS_INVALID_STATE
    );
    assert_status(&mut result, STATUS_INVALID_STATE);
    // Repeating finish is intentionally harmless even after the feeder has
    // already been closed, which keeps shutdown cleanup idempotent.
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
/// A closed feeder is not enough evidence that Core replay completed. The ABI
/// must return `STATUS_OUTSTANDING_TASKS` from premature finalization, retain
/// the graph, and then allow explicit disposal to release it safely.
fn replay_abi_requires_drain_before_finalize_and_disposes_owned_worker() {
    let mut runtime = new_replay_runtime();
    let mut result = empty_result();
    let document = replay_fixture::complete_history_document("workflow-replay-test");
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v1_replay_worker_feed_history_json(
                runtime,
                document.as_ptr(),
                document.len(),
                &mut result,
            )
        },
        STATUS_OK
    );
    assert_status(&mut result, STATUS_OK);
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_replay_worker_finish_input(runtime, &mut result) },
        STATUS_OK
    );
    assert_status(&mut result, STATUS_OK);

    assert_eq!(
        unsafe { ocaml_temporal_core_v1_replay_worker_finalize(runtime, &mut result) },
        STATUS_OUTSTANDING_TASKS
    );
    assert_status(&mut result, STATUS_OUTSTANDING_TASKS);
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
/// Explicit replay disposal must acknowledge a leased activation without
/// sending Core the live-worker failure completion. A replay activation can
/// represent an eviction after the history has reached a terminal state;
/// Core rejects a non-empty failure for that state and would otherwise leave
/// the ABI call blocked or panic inside its workflow state machine.
fn replay_abi_disposes_a_leased_activation_without_core_failure() {
    let mut runtime = new_replay_runtime();
    let mut result = empty_result();
    let document = replay_fixture::complete_history_document("workflow-replay-test");
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v1_replay_worker_feed_history_json(
                runtime,
                document.as_ptr(),
                document.len(),
                &mut result,
            )
        },
        STATUS_OK
    );
    assert_status(&mut result, STATUS_OK);

    // Polling before disposal makes the completion debt explicit rather than
    // relying on a scheduler race between Core and the shutdown request.
    let _activation = poll_replay_activation(runtime);
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
/// A replay rejection must retain its lease when the semantic activation is
/// changed, but accept equivalent JSON with different formatting. This tests
/// the ownership boundary without claiming that JSON bytes are identity.
fn replay_abi_rejects_only_semantically_matching_lease() {
    let mut runtime = new_replay_runtime();
    let mut result = empty_result();
    let document = replay_fixture::open_workflow_task_document("workflow-replay-test");
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v1_replay_worker_feed_history_json(
                runtime,
                document.as_ptr(),
                document.len(),
                &mut result,
            )
        },
        STATUS_OK
    );
    assert_status(&mut result, STATUS_OK);

    let activation = poll_replay_activation(runtime);
    let semantic: serde_json::Value =
        serde_json::from_slice(&activation).expect("replay poll should return JSON");
    let pretty = serde_json::to_string_pretty(&semantic).expect("activation should pretty-print");
    assert_ne!(pretty.as_bytes(), activation.as_slice());

    let mut mismatched = semantic.clone();
    mismatched
        .as_object_mut()
        .expect("activation should be a JSON object")
        .insert("run_id".to_owned(), serde_json::json!("different-run"));
    let mismatched =
        serde_json::to_string(&mismatched).expect("mismatched activation should encode");
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v1_replay_worker_reject_workflow_json(
                runtime,
                mismatched.as_ptr(),
                mismatched.len(),
                &mut result,
            )
        },
        STATUS_PROTOCOL
    );
    assert_status(&mut result, STATUS_PROTOCOL);

    // The failed mismatch above must not retire the lease. Whitespace-only
    // formatting changes preserve the same semantic activation and therefore
    // are accepted by Rust's retained-lease comparison.
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v1_replay_worker_reject_workflow_json(
                runtime,
                pretty.as_ptr(),
                pretty.len(),
                &mut result,
            )
        },
        STATUS_OK
    );
    assert_status(&mut result, STATUS_OK);
    complete_follow_up_eviction(runtime);
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_replay_worker_finish_input(runtime, &mut result) },
        STATUS_OK
    );
    assert_status(&mut result, STATUS_OK);
    finalize_after_natural_shutdown(runtime);
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_runtime_free(&mut runtime) },
        STATUS_OK
    );
}

#[test]
/// Closing an empty replay feeder eventually produces the successful wait
/// signal for natural shutdown, after which finalization is allowed.
fn replay_abi_waits_for_natural_shutdown_before_finalizing() {
    let mut runtime = new_replay_runtime();
    let mut result = empty_result();
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_replay_worker_finish_input(runtime, &mut result) },
        STATUS_OK
    );
    assert_status(&mut result, STATUS_OK);
    finalize_after_natural_shutdown(runtime);
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
