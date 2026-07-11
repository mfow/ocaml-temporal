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

## Verification

Rust integration tests cover version negotiation, status propagation, binary
and zero-length buffers, invalid null pointers, repeated result disposal, and
panic containment. A C11 harness compiles against the public header, links the
actual static archive, exercises the ownership contract, and runs with Address
Sanitizer and UndefinedBehaviorSanitizer in the development container.
