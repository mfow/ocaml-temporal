use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;

/// Version of the native ABI implemented by this crate.
pub const ABI_VERSION: u32 = 1;

pub type Status = i32;

pub const STATUS_OK: Status = 0;
pub const STATUS_INVALID_ARGUMENT: Status = 1;
pub const STATUS_ABI_MISMATCH: Status = 2;
pub const STATUS_PANIC: Status = 3;
pub const STATUS_INTERNAL: Status = 4;

const _: () = assert!(size_of::<Status>() == 4);

/// Byte allocation owned by the Rust bridge.
///
/// Callers must treat this as an opaque field of [`Result`] and release it
/// only through [`ocaml_temporal_core_v1_result_free`].
#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub struct Buffer {
    pub ptr: *mut u8,
    pub len: usize,
}

impl Default for Buffer {
    fn default() -> Self {
        Self {
            ptr: ptr::null_mut(),
            len: 0,
        }
    }
}

impl Buffer {
    fn from_vec(value: Vec<u8>) -> Self {
        if value.is_empty() {
            return Self::default();
        }

        let len = value.len();
        let ptr = Box::into_raw(value.into_boxed_slice()).cast::<u8>();
        Self { ptr, len }
    }
}

/// Single result shape returned by every fallible ABI operation.
///
/// Exactly one of `value` or `error` can own bytes. A successful operation has
/// `status == STATUS_OK`; every other status may carry a UTF-8 diagnostic in
/// `error`.
#[repr(C)]
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Result {
    pub status: Status,
    pub value: Buffer,
    pub error: Buffer,
}

const _: () = {
    assert!(std::mem::offset_of!(Buffer, ptr) == 0);
    assert!(std::mem::offset_of!(Buffer, len) == size_of::<*mut u8>());
    assert!(std::mem::offset_of!(Result, status) == 0);
    assert!(std::mem::offset_of!(Result, value) == align_of::<Buffer>());
    assert!(
        std::mem::offset_of!(Result, error)
            == std::mem::offset_of!(Result, value) + size_of::<Buffer>()
    );
};

struct Failure {
    status: Status,
    message: String,
}

type Operation = std::result::Result<Vec<u8>, Failure>;

fn success(value: Vec<u8>) -> Result {
    Result {
        status: STATUS_OK,
        value: Buffer::from_vec(value),
        error: Buffer::default(),
    }
}

fn failure(status: Status, message: impl Into<String>) -> Result {
    Result {
        status,
        value: Buffer::default(),
        error: Buffer::from_vec(message.into().into_bytes()),
    }
}

unsafe fn invoke(output: *mut Result, operation: impl FnOnce() -> Operation) -> Status {
    if output.is_null() {
        return STATUS_INVALID_ARGUMENT;
    }

    // SAFETY: The non-null output pointer is required by the ABI contract to
    // be valid and writable. `ptr::write` also permits uninitialized storage.
    unsafe { ptr::write(output, Result::default()) };

    let result = match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(Ok(value)) => success(value),
        Ok(Err(error)) => failure(error.status, error.message),
        Err(_) => failure(
            STATUS_PANIC,
            "Rust panic contained at the native ABI boundary",
        ),
    };
    let status = result.status;

    // SAFETY: The pointer was validated above and no reference to its previous
    // (empty) value is retained.
    unsafe { ptr::write(output, result) };
    status
}

unsafe fn free_buffer(buffer: &mut Buffer) {
    if buffer.ptr.is_null() {
        buffer.len = 0;
        return;
    }

    let allocation = ptr::slice_from_raw_parts_mut(buffer.ptr, buffer.len);
    // SAFETY: Only buffers created by `Buffer::from_vec` may be passed to this
    // function. The exact pointer and length reconstruct its boxed slice.
    drop(unsafe { Box::from_raw(allocation) });
    *buffer = Buffer::default();
}

/// Negotiate ABI version 1.
///
/// # Safety
///
/// `output` must be null or point to writable storage for one [`Result`]. It
/// must not contain live bridge-owned allocations when this function starts.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_check_abi_version(
    requested_version: u32,
    output: *mut Result,
) -> Status {
    // SAFETY: `invoke` validates null and otherwise relies on the documented
    // output-pointer contract of this exported function.
    unsafe {
        invoke(output, || {
            if requested_version == ABI_VERSION {
                Ok(Vec::new())
            } else {
                Err(Failure {
                    status: STATUS_ABI_MISMATCH,
                    message: format!(
                        "unsupported ABI version {requested_version}; expected {ABI_VERSION}"
                    ),
                })
            }
        })
    }
}

/// Copy bytes into a Rust-owned result buffer.
///
/// This operation exercises the ownership contract used later for encoded
/// workflow activations and completions.
///
/// # Safety
///
/// When `input_len` is nonzero, `input` must point to that many readable bytes.
/// `output` follows the same contract as
/// [`ocaml_temporal_core_v1_check_abi_version`] and must not overlap `input`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_echo(
    input: *const u8,
    input_len: usize,
    output: *mut Result,
) -> Status {
    // SAFETY: `invoke` validates the output pointer. Input is checked for null
    // before constructing the slice; remaining validity is the caller's ABI
    // obligation documented above.
    unsafe {
        invoke(output, || {
            if input_len == 0 {
                return Ok(Vec::new());
            }
            if input.is_null() {
                return Err(Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "input is null but input_len is nonzero".to_owned(),
                });
            }

            // SAFETY: The caller guarantees a readable input allocation of
            // `input_len` bytes and the null case was rejected above.
            Ok(std::slice::from_raw_parts(input, input_len).to_vec())
        })
    }
}

/// Release both owned buffers in a result and reset it to the empty state.
///
/// Repeated calls with the same result object are safe. Copying a live result
/// and freeing both copies is forbidden by the ownership contract.
///
/// # Safety
///
/// `result` must be null or point to a result initialized by this ABI. The
/// caller must not mutate its pointer or length fields.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_result_free(result: *mut Result) -> Status {
    if result.is_null() {
        return STATUS_INVALID_ARGUMENT;
    }

    let outcome = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: The caller promises this pointer refers to a bridge result
        // and has not altered either owned buffer.
        let result = unsafe { &mut *result };
        // SAFETY: Both buffers originate from this bridge or are empty.
        unsafe {
            free_buffer(&mut result.value);
            free_buffer(&mut result.error);
        }
        result.status = STATUS_OK;
    }));

    if outcome.is_ok() {
        STATUS_OK
    } else {
        STATUS_PANIC
    }
}

/// Rust-only probe proving that the shared ABI wrapper contains panics.
///
/// This symbol is not exported through the C header or assigned a stable ABI
/// name. It exists solely for the integration test crate.
///
/// # Safety
///
/// `output` follows the same contract as
/// [`ocaml_temporal_core_v1_check_abi_version`].
#[doc(hidden)]
pub unsafe fn test_invoke_panic(output: *mut Result) -> Status {
    // SAFETY: The pointer contract is forwarded unchanged to `invoke`.
    unsafe { invoke(output, || panic!("intentional ABI containment probe")) }
}
