use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, channel, sync_channel};
use std::time::Duration;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, TokioRuntimeBuilder};

/// Version of the native ABI implemented by this crate.
pub const ABI_VERSION: u32 = 1;

/// Fixed-width status type shared with the C header.
pub type Status = i32;

/// Operation completed and `value` may own bytes.
pub const STATUS_OK: Status = 0;
/// A pointer, length, range, or other caller argument violated the ABI.
pub const STATUS_INVALID_ARGUMENT: Status = 1;
/// The caller requested an ABI version this bridge does not implement.
pub const STATUS_ABI_MISMATCH: Status = 2;
/// A Rust panic was contained before it crossed the C boundary.
pub const STATUS_PANIC: Status = 3;
/// Reserved non-panic bridge implementation failure.
pub const STATUS_INTERNAL: Status = 4;

const _: () = assert!(size_of::<Status>() == 4);

/// Monotonic test instrumentation for successfully exposed runtime owners.
static RUNTIMES_CREATED: AtomicU64 = AtomicU64::new(0);
/// Monotonic test instrumentation for Core instances whose destructor ran.
static RUNTIMES_CLEANED: AtomicU64 = AtomicU64::new(0);

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
    /// Returns the canonical empty buffer with no allocation ownership.
    fn default() -> Self {
        Self {
            ptr: ptr::null_mut(),
            len: 0,
        }
    }
}

impl Buffer {
    /// Transfers a vector allocation into the ABI, canonicalizing empty input.
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

/// Internal structured failure converted into an owned ABI diagnostic.
struct Failure {
    status: Status,
    message: String,
}

/// Owns the Tokio executor and shared Temporal Core runtime for one SDK instance.
///
/// The type is opaque to C. Higher-level client and worker handles will retain
/// the same runtime owner rather than creating independent executors.
pub struct Runtime {
    core: Option<CoreRuntime>,
    cleanup: std::sync::mpsc::Sender<RuntimeCleanup>,
}

/// Ownership transfer consumed by the runtime's dedicated cleanup thread.
struct RuntimeCleanup {
    core: CoreRuntime,
    completed: Option<SyncSender<Status>>,
}

impl Runtime {
    /// Starts the cleanup thread before exposing a handle, so every successful
    /// runtime allocation already has a non-blocking GC fallback path.
    fn new(core: CoreRuntime) -> std::result::Result<Self, Failure> {
        let (cleanup, receiver) = channel();
        std::thread::Builder::new()
            .name("ocaml-temporal-runtime-cleanup".to_owned())
            .spawn(move || run_runtime_cleanup(receiver))
            .map_err(|error| Failure {
                status: STATUS_INTERNAL,
                message: format!("could not start Temporal runtime cleanup thread: {error}"),
            })?;
        RUNTIMES_CREATED.fetch_add(1, Ordering::Relaxed);
        Ok(Self {
            core: Some(core),
            cleanup,
        })
    }

    /// Transfers Core to its cleanup thread and optionally waits for disposal.
    ///
    /// Explicit close waits while the OCaml lock is released. GC fallback does
    /// not wait, so a custom-block finalizer never stalls the collector.
    fn close(mut self, wait: bool) -> Status {
        let Some(core) = self.core.take() else {
            return STATUS_OK;
        };
        let (completed, receiver) = if wait {
            let (sender, receiver) = sync_channel(1);
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        };
        let message = RuntimeCleanup { core, completed };

        if let Err(error) = self.cleanup.send(message) {
            // The receiver only exits after a message, so this indicates a
            // defect in the cleanup thread itself. Reclaim on this thread to
            // preserve the no-leak guarantee even on that defensive path.
            drop(error.0.core);
            RUNTIMES_CLEANED.fetch_add(1, Ordering::Release);
            return STATUS_INTERNAL;
        }

        match receiver {
            Some(receiver) => receiver.recv().unwrap_or(STATUS_INTERNAL),
            None => STATUS_OK,
        }
    }
}

/// Drops Core away from OCaml's collector and reports completion when asked.
fn run_runtime_cleanup(receiver: Receiver<RuntimeCleanup>) {
    let Ok(message) = receiver.recv() else {
        return;
    };
    let RuntimeCleanup { core, completed } = message;
    let status = if catch_unwind(AssertUnwindSafe(|| drop(core))).is_ok() {
        STATUS_OK
    } else {
        STATUS_PANIC
    };
    // Release publishes completion after Core's destructor has returned. The
    // matching Acquire load is used only by the isolated ownership test.
    RUNTIMES_CLEANED.fetch_add(1, Ordering::Release);
    if let Some(completed) = completed {
        let _ = completed.send(status);
    }
}

/// Byte-producing operation accepted by the shared panic/ownership wrapper.
type Operation = std::result::Result<Vec<u8>, Failure>;

/// Constructs the only valid successful result shape.
fn success(value: Vec<u8>) -> Result {
    Result {
        status: STATUS_OK,
        value: Buffer::from_vec(value),
        error: Buffer::default(),
    }
}

/// Constructs the only valid failed result shape with UTF-8 diagnostic bytes.
fn failure(status: Status, message: impl Into<String>) -> Result {
    Result {
        status,
        value: Buffer::default(),
        error: Buffer::from_vec(message.into().into_bytes()),
    }
}

/// Initializes output, contains panics, and commits one fully formed result.
///
/// Writing the empty result before executing user logic ensures every non-null
/// output is safe to free even when the operation panics.
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

/// Reclaims one bridge allocation and resets the buffer to canonical empty.
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

/// Wait on the native side for bridge conformance testing.
///
/// This bounded operation exists to prove that language bindings release
/// their runtime lock around blocking ABI calls. It is not a workflow timer.
///
/// # Safety
///
/// `output` follows the same contract as
/// [`ocaml_temporal_core_v1_check_abi_version`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_conformance_wait_ms(
    milliseconds: u32,
    output: *mut Result,
) -> Status {
    // SAFETY: The output-pointer contract is forwarded unchanged to `invoke`.
    unsafe {
        invoke(output, || {
            if milliseconds > 1_000 {
                return Err(Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "conformance wait cannot exceed 1000 ms".to_owned(),
                });
            }
            std::thread::sleep(Duration::from_millis(u64::from(milliseconds)));
            Ok(Vec::new())
        })
    }
}

/// Create the native runtime that will own later Core clients and workers.
///
/// On success, `runtime` receives one owned opaque handle. The caller must
/// eventually pass that same slot to [`ocaml_temporal_core_v1_runtime_free`].
///
/// # Safety
///
/// `runtime` must be null or point to writable storage for one runtime pointer.
/// `output` follows the result contract of
/// [`ocaml_temporal_core_v1_check_abi_version`]. A non-null runtime slot must
/// not already contain a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_runtime_new(
    runtime: *mut *mut Runtime,
    output: *mut Result,
) -> Status {
    // Canonicalize any writable handle slot even when the independent result
    // pointer is invalid. A caller can therefore inspect a known null value on
    // every failing return instead of retaining indeterminate ownership state.
    if !runtime.is_null() {
        // SAFETY: A non-null runtime argument promises writable pointer storage.
        unsafe { ptr::write(runtime, ptr::null_mut()) };
    }
    if output.is_null() {
        return STATUS_INVALID_ARGUMENT;
    }
    if runtime.is_null() {
        // SAFETY: The result pointer was checked above and the closure does
        // not inspect the missing runtime slot.
        return unsafe {
            invoke(output, || {
                Err(Failure {
                    status: STATUS_INVALID_ARGUMENT,
                    message: "runtime output pointer is null".to_owned(),
                })
            })
        };
    }

    // SAFETY: Both output locations were validated above.
    unsafe {
        invoke(output, || {
            let options = RuntimeOptions::builder()
                .build()
                .map_err(|message| Failure {
                    status: STATUS_INTERNAL,
                    message: format!("could not configure Temporal Core runtime: {message}"),
                })?;
            let core =
                CoreRuntime::new(options, TokioRuntimeBuilder::default()).map_err(|error| {
                    Failure {
                        status: STATUS_INTERNAL,
                        message: format!("could not create Temporal Core runtime: {error}"),
                    }
                })?;
            let owned = Box::into_raw(Box::new(Runtime::new(core)?));

            // SAFETY: The runtime slot remains exclusively owned by this call
            // until it returns and was validated before invoking the closure.
            ptr::write(runtime, owned);
            Ok(Vec::new())
        })
    }
}

/// Destroy one native runtime and clear the caller's slot.
///
/// Passing the same slot again after a successful call is safe because the
/// first call stores null before dropping the owner.
///
/// # Safety
///
/// `runtime` must be null or point to a slot initialized by
/// [`ocaml_temporal_core_v1_runtime_new`]. The slot must not be accessed
/// concurrently, and all future child handles must be closed first.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_runtime_free(runtime: *mut *mut Runtime) -> Status {
    if runtime.is_null() {
        return STATUS_INVALID_ARGUMENT;
    }

    let outcome = catch_unwind(AssertUnwindSafe(|| {
        // Clear first so even a defensive panic during destruction cannot
        // invite a second attempt to free the same allocation.
        // SAFETY: The caller guarantees exclusive writable access to the slot.
        let owned = unsafe { ptr::replace(runtime, ptr::null_mut()) };
        if owned.is_null() {
            STATUS_OK
        } else {
            // SAFETY: Non-null values in this slot originate from `Box::into_raw`
            // in `runtime_new` and have not been reclaimed previously.
            let runtime = unsafe { Box::from_raw(owned) };
            runtime.close(true)
        }
    }));

    match outcome {
        Ok(status) => status,
        Err(_) => STATUS_PANIC,
    }
}

/// Transfer a runtime to its cleanup thread without waiting for destruction.
///
/// This is reserved for the OCaml custom-block finalizer. Normal supervisor
/// shutdown uses [`ocaml_temporal_core_v1_runtime_free`] and waits while the
/// OCaml runtime lock is released.
///
/// # Safety
///
/// `runtime` has the same exclusive slot contract as
/// [`ocaml_temporal_core_v1_runtime_free`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ocaml_temporal_core_v1_runtime_dispose(
    runtime: *mut *mut Runtime,
) -> Status {
    if runtime.is_null() {
        return STATUS_INVALID_ARGUMENT;
    }

    let outcome = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: The caller guarantees exclusive writable access to the slot.
        let owned = unsafe { ptr::replace(runtime, ptr::null_mut()) };
        if owned.is_null() {
            STATUS_OK
        } else {
            // SAFETY: The pointer was created by `runtime_new` and is consumed
            // exactly once by this pointer-to-pointer operation.
            let runtime = unsafe { Box::from_raw(owned) };
            runtime.close(false)
        }
    }));

    match outcome {
        Ok(status) => status,
        Err(_) => STATUS_PANIC,
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

/// Returns process-local lifecycle counts for the isolated cleanup test.
///
/// This is intentionally not part of the C ABI. Keeping the counters monotonic
/// lets the test wait for asynchronous destruction without reaching into the
/// runtime owner or depending on timing alone.
#[doc(hidden)]
pub fn test_runtime_cleanup_counts() -> (u64, u64) {
    (
        RUNTIMES_CREATED.load(Ordering::Acquire),
        RUNTIMES_CLEANED.load(Ordering::Acquire),
    )
}
