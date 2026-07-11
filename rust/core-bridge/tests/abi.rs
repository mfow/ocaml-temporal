use std::ptr;

use ocaml_temporal_core_bridge::{
    ABI_VERSION, Buffer, Result as AbiResult, STATUS_ABI_MISMATCH, STATUS_INVALID_ARGUMENT,
    STATUS_OK, STATUS_PANIC, ocaml_temporal_core_v1_check_abi_version, ocaml_temporal_core_v1_echo,
    ocaml_temporal_core_v1_result_free, test_invoke_panic,
};

fn empty_result() -> AbiResult {
    AbiResult::default()
}

fn bytes(buffer: &Buffer) -> Vec<u8> {
    if buffer.ptr.is_null() {
        assert_eq!(buffer.len, 0);
        Vec::new()
    } else {
        // SAFETY: Tests read buffers returned by the bridge before freeing the
        // containing result, so the allocation remains live for this copy.
        unsafe { std::slice::from_raw_parts(buffer.ptr, buffer.len).to_vec() }
    }
}

#[test]
fn negotiates_the_supported_abi_version() {
    let mut result = empty_result();

    let status = unsafe { ocaml_temporal_core_v1_check_abi_version(ABI_VERSION, &mut result) };

    assert_eq!(status, STATUS_OK);
    assert_eq!(result.status, STATUS_OK);
    assert!(result.value.ptr.is_null());
    assert!(result.error.ptr.is_null());
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_result_free(&mut result) },
        STATUS_OK
    );
}

#[test]
fn reports_an_owned_error_for_an_unsupported_version() {
    let mut result = empty_result();

    let status = unsafe { ocaml_temporal_core_v1_check_abi_version(ABI_VERSION + 1, &mut result) };

    assert_eq!(status, STATUS_ABI_MISMATCH);
    assert_eq!(result.status, STATUS_ABI_MISMATCH);
    assert!(result.value.ptr.is_null());
    assert!(!bytes(&result.error).is_empty());
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_result_free(&mut result) },
        STATUS_OK
    );
}

#[test]
fn owns_echoed_bytes_and_supports_zero_length_buffers() {
    let input = b"activation";
    let mut result = empty_result();

    let status = unsafe { ocaml_temporal_core_v1_echo(input.as_ptr(), input.len(), &mut result) };

    assert_eq!(status, STATUS_OK);
    assert_eq!(bytes(&result.value), input);
    assert!(result.error.ptr.is_null());
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_result_free(&mut result) },
        STATUS_OK
    );

    let status = unsafe { ocaml_temporal_core_v1_echo(ptr::null(), 0, &mut result) };
    assert_eq!(status, STATUS_OK);
    assert!(result.value.ptr.is_null());
    assert_eq!(result.value.len, 0);
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_result_free(&mut result) },
        STATUS_OK
    );
}

#[test]
fn rejects_null_required_pointers() {
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_check_abi_version(ABI_VERSION, ptr::null_mut()) },
        STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_echo(ptr::null(), 1, ptr::null_mut()) },
        STATUS_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_result_free(ptr::null_mut()) },
        STATUS_INVALID_ARGUMENT
    );
}

#[test]
fn result_free_is_idempotent_for_the_same_result_object() {
    let mut result = empty_result();
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_echo(b"owned".as_ptr(), 5, &mut result) },
        STATUS_OK
    );

    assert_eq!(
        unsafe { ocaml_temporal_core_v1_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(result, empty_result());
}

#[test]
fn contains_rust_panics_as_owned_errors() {
    let mut result = empty_result();

    let status = unsafe { test_invoke_panic(&mut result) };

    assert_eq!(status, STATUS_PANIC);
    assert_eq!(result.status, STATUS_PANIC);
    assert!(result.value.ptr.is_null());
    assert!(!bytes(&result.error).is_empty());
    assert_eq!(
        unsafe { ocaml_temporal_core_v1_result_free(&mut result) },
        STATUS_OK
    );
}
