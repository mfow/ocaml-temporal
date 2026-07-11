# Native Core Bridge ABI

The product is an OCaml Temporal SDK, not only a Temporal service client. It
implements workflow workers and the deterministic workflow runtime as well as
client operations that start and observe workflows. The final worker process
is owned and launched by OCaml. The public OCaml library calls private C stubs,
which call the versioned Rust ABI documented here. Rust links the official
Temporal Core library and never invokes arbitrary OCaml closures.

## Version and symbols

ABI version 1 uses only symbols beginning with `ocaml_temporal_core_v1_`.
Callers must negotiate `OCAML_TEMPORAL_CORE_ABI_VERSION` before creating future
runtime, internal Core connection-client, or worker handles. The client handle
is one implementation component of the larger SDK, not the scope of the public
library. The header reserves opaque declarations for those three handle types
without exposing their Rust layout.

The canonical header is
`rust/core-bridge/include/ocaml_temporal_core.h`. Both Rust and C compile-time
assertions protect the status width and field ordering of the documented
`repr(C)` structures.

## Result and buffer ownership

Every fallible operation accepts a writable result pointer and returns the same
status stored in that result. Status zero is success. Nonzero statuses describe
invalid arguments, ABI mismatch, a contained Rust panic, or an internal bridge
failure.

A result has one success buffer and one error buffer. At most one owns memory:

- success may place arbitrary binary bytes in `value`;
- failure may place a UTF-8 diagnostic in `error`;
- an empty allocation is always represented as `{ NULL, 0 }`.

Rust owns both allocations. The caller may copy their bytes but must never
mutate or directly free their fields. It must call
`ocaml_temporal_core_v1_result_free` exactly once after consuming an initialized
result. That function clears the object, so accidentally calling it again on
the same object is safe. Copying a live result structure creates no new
ownership; freeing both copies is invalid.

An output object may be uninitialized, but it must not contain a live owned
result when passed to another operation. Free the previous result first.

### OCaml ownership guard

The private C stubs allocate an OCaml custom block before entering Rust. That
block is the sole owner of the ABI result and has a finalizer which calls
`ocaml_temporal_core_v1_result_free`. The OCaml wrapper also uses
`Fun.protect` to release the result deterministically after copying its bytes.
This gives every path two compatible safeguards: normal operation frees
immediately, while an OCaml allocation failure or other exception leaves a
rooted/finalizable owner rather than orphaning Rust memory. Disposal is
idempotent, so a later finalizer after deterministic disposal is harmless.

Returned bytes are copied once, directly from the live Rust buffer into the
OCaml string/bytes allocation. Inputs that must survive a blocking call are
copied to temporary C storage before the runtime lock is released, then freed
immediately after the lock is reacquired. Neither side directly frees an
allocation made by the other side.

## Pointer and panic contract

Null output/result pointers return `INVALID_ARGUMENT` without being
dereferenced. A null input pointer is valid only when its length is zero.
As with any C byte-span API, a non-null input pointer must identify a readable
allocation of the stated length and must not overlap the output object.

Every fallible exported operation contains Rust panics before they can unwind
through C. A contained panic becomes `STATUS_PANIC` and an owned diagnostic.
The Rust integration suite invokes the common wrapper with a deliberate panic;
the panic test hook is not exported in the C header and is not part of the
stable ABI.

## Stateful handle ownership

The reserved runtime, client, and worker handles are opaque references to
Rust-owned SDK state, not OS handles and not public OCaml values:

- a runtime owns Tokio and shared Core infrastructure;
- a client owns one cluster connection and its authentication/configuration;
- a worker owns polling and completion state for a task queue configuration.

A normal process is expected to have one runtime, usually one client, and one
or a small number of workers. The intended OCaml design is therefore one
supervisor actor per SDK instance, not one actor per handle. A dedicated OCaml
Domain owns the entire runtime/client/worker graph. Calls from other Domains
enter a synchronized MPSC mailbox and receive typed one-shot `result` replies.
The supervisor serializes lifecycle transitions and destroys workers before
clients and the runtime. Rust retains internal Tokio concurrency; workflow
executions retain their separate deterministic effect schedulers.

Notification is event-driven without a foreign-thread OCaml callback. Rust
queues readiness and signals a native condition/event primitive; the
supervisor's dedicated Domain/OS thread waits in a C stub with its OCaml
runtime lock released. No workflow effect continuation or general cooperative
scheduler performs this blocking wait. The stub returns normally when
signaled, after which the actor drains ready Core work. Shutdown signals the
same wait path. This avoids polling timers while also avoiding runtime
registration, reentrancy, and teardown races from Tokio threads entering
OCaml.

## Verification

Rust integration tests cover version negotiation, status propagation, binary
and zero-length buffers, invalid null pointers, bounded blocking, repeated
result disposal, and panic containment. A C11 harness compiles against the
public header, links the actual static archive, exercises the ownership
contract, and runs with Address Sanitizer and UndefinedBehavior Sanitizer in
the development container. An OCaml two-Domain test calls the linked Rust
archive and proves another Domain progresses during a native wait. An install
smoke test builds a fresh OCaml executable from the staged package and invokes
the negotiated ABI through the public `Temporal.Runtime_info` module.
