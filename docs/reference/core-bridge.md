# Native Core Bridge ABI

This document is for contributors changing the OCaml/C/Rust boundary. Workflow
authors do not call this interface directly.

The product is an OCaml Temporal SDK, not only a Temporal service client. It
implements workflow workers and the deterministic workflow runtime as well as
client operations that start and observe workflows. The final worker process
is owned and launched by OCaml. The public OCaml library calls private C stubs,
which call the versioned Rust ABI documented here. Rust links the official
Temporal Core library and never invokes arbitrary OCaml functions from its
background threads.

## Version and symbols

ABI version 1 uses only symbols beginning with `ocaml_temporal_core_v1_`.
Before using the bridge, OCaml asks Rust which ABI version it implements and
checks that it matches `OCAML_TEMPORAL_CORE_ABI_VERSION`. The bridge represents
the Rust runtime, and will represent server connections and workers, with
opaque handles. “Opaque” means OCaml can pass a handle back to Rust but cannot
inspect the Rust object it refers to. A connection handle is only one internal
part of the SDK; the public package is not merely a service client.

The canonical header is
`rust/core-bridge/include/ocaml_temporal_core.h`. Both Rust and C compile-time
assertions protect the status width and field ordering of the documented
`repr(C)` structures.

## Semantic workflow adapter

`rust/core-bridge/src/workflow_protocol.rs` is the Rust-only protobuf boundary
for the first activation/completion slice. It converts pinned official Core
types to a closed semantic model, serializes that model as strict JSON, and
performs the inverse conversion for workflow commands. The private OCaml module
`Temporal_protocol.Workflow_protocol` implements the same model and validation
without importing protobuf definitions.

Both encoders reparse their own output before it can cross the native boundary.
Both decoders reject duplicate or unknown fields, unknown variants, numeric
range violations, non-canonical base64, invalid workflow invariants, and
oversized values. Core fields not represented by the current semantic slice are
accepted only at their documented default; a non-default value returns a typed
`Unsupported` conversion error rather than being lost. See the
[protocol reference](core-protocol.md), machine-readable schemas, and
[ADR 0006](../decisions/0006-first-workflow-semantic-protocol.md).

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

The implemented private supervisor currently owns the real runtime only. Its
backend protocol exposes typed GADT operations but never the owner-confined
state, preventing a raw handle from escaping through an otherwise convenient
callback. Client and worker handles will be added to that same graph after the
corresponding bridge operations exist. See
[ADR 0004](../decisions/0004-sdk-instance-supervisor.md) for its lifecycle,
failure, and scheduler contracts.

Notification is event-driven without a foreign-thread OCaml callback. Rust
queues readiness and signals a native condition/event primitive; the
supervisor's dedicated Domain/OS thread waits in a C stub with its OCaml
runtime lock released. No workflow effect continuation or general cooperative
scheduler performs this blocking wait. The stub returns normally when
signaled, after which the actor drains ready Core work. Shutdown signals the
same wait path. This avoids polling timers while also avoiding runtime
registration, reentrancy, and teardown races from Tokio threads entering
OCaml.

### Runtime destruction

Creating an SDK runtime starts its Tokio executor and a small Rust cleanup
thread dedicated to that owner. Explicit OCaml shutdown atomically detaches the
opaque pointer, releases the OCaml runtime lock, transfers Core to the cleanup
thread, and waits until Core's destructor has returned. This makes orderly
shutdown observable without preventing other OCaml Domains from running.

The OCaml custom-block finalizer is a fallback for abandoned runtime values. It
performs the same atomic detach and ownership transfer but does not wait. Core
is therefore never destroyed by the OCaml garbage collector thread, whose
progress must not depend on Tokio shutdown. Both paths clear the handle before
transfer and are idempotent; exactly one path can own the native runtime.

## Verification

Rust integration tests cover version negotiation, status propagation, binary
and zero-length buffers, invalid null pointers, bounded blocking, repeated
result disposal, panic containment, explicit runtime closure, and completion of
the asynchronous finalizer fallback. The latter runs in an isolated test
process and observes monotonic cleanup counters only after Core's destructor
returns, preventing a parallel test from producing a false positive. A C11
harness compiles against the public header, links the actual static archive,
exercises the ownership contract, and runs with Address Sanitizer and
UndefinedBehavior Sanitizer in the development container. An OCaml two-Domain
test calls the linked Rust archive and proves another Domain progresses during
a native wait. An install
smoke test builds a fresh OCaml executable from the staged package and invokes
the negotiated ABI through the public `Temporal.Runtime_info` module.

The Dune rule asks `rustc --print=native-static-libs` for the exact native
libraries required by the static archive and consumes the resulting ordered
flags from a generated S-expression file. This keeps platform linker knowledge
owned by the pinned Rust compiler instead of duplicating a fragile Linux,
macOS, and Windows library list in the OCaml build.

The C binding is a Dune `foreign_library`, so Dune first compiles it into a
plain static archive without applying Rust's system-library flags. The OCaml
library then references both that C archive and the Rust archive, and applies
the generated flags only when linking a consumer. The workspace also disables
dynamically linked foreign archives. The internal OCaml library uses
`no_dynlink`, because a native plugin (`.cmxs`) would be another dynamic bridge
artifact and is neither supported nor needed by the final executable.

The supported deployment artifact is an OCaml-owned native executable; the
project does not need a separately loadable bridge DLL. This distinction is
important on Windows: Rust correctly reports GNU linker tokens for the final
native link, but FlexDLL cannot reinterpret all of those tokens while
constructing an intermediate OCaml stub DLL. Keeping the C and Rust inputs as
static foreign archives removes that unnecessary link step without changing
the installed OCaml API or final executable.
