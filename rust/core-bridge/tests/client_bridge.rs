//! ABI-level tests for the private dynamic client start/wait slice.

use std::ptr;

use ocaml_temporal_core_bridge::{
    Buffer, Result as AbiResult, STATUS_INVALID_ARGUMENT, STATUS_INVALID_STATE, STATUS_OK,
    STATUS_PROTOCOL, ocaml_temporal_core_v1_client_start_workflow_json,
    ocaml_temporal_core_v1_client_wait_workflow_json, ocaml_temporal_core_v1_result_free,
    ocaml_temporal_core_v1_runtime_free, ocaml_temporal_core_v1_runtime_new,
};

/// Produces writable initialized storage matching the ABI result contract.
fn empty_result() -> AbiResult {
    AbiResult::default()
}

/// Copies the Rust-owned diagnostic before the result is freed.
fn error_bytes(result: &AbiResult) -> Vec<u8> {
    if result.error.ptr.is_null() {
        assert_eq!(result.error.len, 0);
        Vec::new()
    } else {
        // SAFETY: The test reads the result before invoking result_free.
        unsafe { std::slice::from_raw_parts(result.error.ptr, result.error.len).to_vec() }
    }
}

/// Minimal valid start document used when testing ABI state handling.
const START_REQUEST: &[u8] = br#"{"namespace":"default","workflow_id":"workflow-1","workflow_type":"smoke","task_queue":"queue","input":[]}"#;
/// Minimal valid exact-run wait document used when testing ABI state handling.
const WAIT_REQUEST: &[u8] =
    br#"{"namespace":"default","workflow_id":"workflow-1","run_id":"run-1"}"#;

#[test]
/// Null runtime handles fail without dereferencing either client operation.
fn client_operations_reject_null_runtime() {
    let mut result = empty_result();
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v1_client_start_workflow_json(
                ptr::null_mut(),
                START_REQUEST.as_ptr(),
                START_REQUEST.len(),
                &mut result,
            )
        },
        STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe {
            ocaml_temporal_core_v1_client_wait_workflow_json(
                ptr::null_mut(),
                WAIT_REQUEST.as_ptr(),
                WAIT_REQUEST.len(),
                &mut result,
            )
        },
        STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_result_free(&mut result) },
        STATUS_OK
    );
}

#[test]
/// A live runtime without a connection rejects start and wait as invalid
/// state, while still returning owned diagnostics that can be freed.
fn client_operations_require_connected_client() {
    let mut runtime = ptr::null_mut();
    let mut result = empty_result();
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_runtime_new(&mut runtime, &mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe {
            ocaml_temporal_core_v1_client_start_workflow_json(
                runtime,
                START_REQUEST.as_ptr(),
                START_REQUEST.len(),
                &mut result,
            )
        },
        STATUS_INVALID_STATE
    );
    assert!(!error_bytes(&result).is_empty());
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe {
            ocaml_temporal_core_v1_client_wait_workflow_json(
                runtime,
                WAIT_REQUEST.as_ptr(),
                WAIT_REQUEST.len(),
                &mut result,
            )
        },
        STATUS_INVALID_STATE
    );
    assert!(!error_bytes(&result).is_empty());
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_result_free(&mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe { ocaml_temporal_core_v1_runtime_free(&mut runtime) },
        STATUS_OK
    );
    assert!(runtime.is_null());
}

#[test]
/// Malformed JSON is rejected before attempting to access the client graph.
fn client_operations_validate_json_before_state_use() {
    let mut runtime = ptr::null_mut();
    let mut result = empty_result();
    let malformed = b"{}";
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_runtime_new(&mut runtime, &mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe {
            ocaml_temporal_core_v1_client_start_workflow_json(
                runtime,
                malformed.as_ptr(),
                malformed.len(),
                &mut result,
            )
        },
        STATUS_PROTOCOL
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_runtime_free(&mut runtime) },
        STATUS_OK
    );
}

// Keep Buffer referenced so this integration test also verifies the public
// result layout remains available to C callers without a private helper.
const _: Option<Buffer> = None;
