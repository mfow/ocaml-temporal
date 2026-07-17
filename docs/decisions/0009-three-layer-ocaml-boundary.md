# ADR 0009: Three-layer SDK boundary

- Status: accepted
- Date: 2026-07-17
- Decision owners: OCaml Temporal maintainers

## Context

The installed package already hid its implementation libraries below Dune's
package-private directory, but modules in `lib/public` named the protocol,
native bridge, deterministic runtime, supervisor, and future kernel directly.
That made the user-facing implementation depend on details from several lower
libraries even though applications could not import those libraries.

The SDK needs separate places to reason about foreign ownership, deterministic
workflow execution, and idiomatic OCaml authoring. Those concerns have
different invariants and should be testable without widening the supported API.

## Decision

Maintain three dependency layers:

1. The private native transport layer owns strict JSON validation, the C ABI,
   Rust allocations and handles, Temporal Core protobufs, Tokio, and server I/O.
2. The private OCaml kernel owns workflow scheduling and execution-local state,
   future callbacks, mailbox serialization, and the one-owner-Domain supervisor.
   Its `Temporal_sdk_kernel` module is a zero-runtime-cost allow-list of the
   lower modules needed by facade adapters. Module aliases preserve exact type
   identity; the kernel does not copy native state or introduce forwarding
   callbacks.
3. The public `Temporal` facade owns the typed and idiomatic authoring surface.
   Its private `Backend` and `Native_worker` adapters translate public values to
   kernel operations, while application modules expose neither adapter.

The public library may depend directly on `temporal_base` because those copied
value types and validation helpers are shared by public codecs and private
adapters. It may depend on `yojson` for the intentionally public standard JSON
codec and on `threads` for public client/worker lifecycle guards. All access to
the transport, runtime, supervisor, and future implementation passes through
`temporal_sdk_kernel`.

The installed-package regression rejects direct lower-layer source references,
direct lower-layer Dune dependencies, a leaked top-level kernel archive, and a
consumer that can import `Temporal_sdk_kernel` through `(libraries
temporal-sdk)`.

## Consequences

- Public workflow helpers can be reviewed without following JSON, Rust, Tokio,
  or mailbox dependencies through unrelated libraries.
- Kernel and native ownership changes remain private and can evolve without
  adding supported application modules.
- Cross-layer type identity remains compiler-checked rather than reconstructed
  with casts or duplicate records.
- Moving a capability into the public API requires an explicit facade design;
  adding it to a protocol or runtime module alone cannot expose it to callers.
