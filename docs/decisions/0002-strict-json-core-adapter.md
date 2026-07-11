# ADR 0002: Strict JSON between Rust and OCaml

- Status: accepted
- Date: 2026-07-11
- Decision owners: OCaml Temporal maintainers

## Context

Temporal Core polls and completes work using protobuf types generated inside
the official Rust implementation. An earlier plan proposed copying those raw
protobuf bytes into OCaml and maintaining a second partial protobuf
implementation there.

That approach would make the OCaml package depend on Core's private field
numbers and message evolution. It would also require a custom protobuf reader,
creating more compatibility and malformed-input behavior for this project to
own. CPU cost at this boundary is less important than making the contract easy
to inspect, test, and change safely.

The OCaml and Rust components are always compiled and distributed together in
one final executable. They are not independently versioned services, so the
adapter does not need runtime protocol negotiation or backward compatibility
between arbitrary releases.

## Decision

Rust is the only layer that reads or constructs Temporal/Core protobuf. The
project-owned C ABI transfers a strict JSON control document in the same
single-owner byte result already used by bridge operations.

Rust converts a Core activation into the project's activation schema. OCaml
strictly validates that document and copies it into ordinary variants,
records, and opaque payload bytes before continuing workflow execution. OCaml
encodes commands using the matching completion schema; Rust validates them and
constructs the official Core completion protobuf.

Arbitrary payload bytes use canonical padded RFC 4648 base64 strings. JSON is
only the private control protocol and does not require workflow authors to use
JSON payload codecs. The OCaml `base64` package is ISC licensed and has no
non-build runtime dependencies beyond OCaml; its exact version and closure
must still pass the locked dependency audit before adoption.

The schema has these rules:

- Every document contains an exact message-kind tag.
- Integer values that may exceed portable JSON integer ranges are decimal
  strings with validated bounds.
- Duplicate fields, unknown fields or tags, missing fields, wrong JSON types,
  invalid base64, and out-of-range values are errors.
- Input and decoded payload sizes are bounded before allocation.
- Rust and OCaml schema changes land together. Shared fixtures and the normal
  build detect development-time drift between the two implementations.
- Rust and OCaml share checked golden fixtures in both directions. Malformed
  fixtures and generated round trips exercise every message variant.

Validation is deliberately symmetric. Before sending, each side validates the
typed value's semantic invariants, serializes it, and parses the resulting JSON
through an independent strict decoder. The receiver repeats strict parsing and
semantic validation before changing Core or workflow state. Size, nesting,
collection-count, decimal-integer, and decoded-payload limits apply on both
sides. Duplicate object members are rejected during parsing because JSON
Schema sees an object only after duplicate-name information may have been lost.

Machine-readable schemas use JSON Schema Draft 2020-12 with closed object
shapes. CI validates every example and shared fixture against those schemas as
a third check independent of the Rust and OCaml runtime validators.

The bridge retains one numeric compatibility check performed when an SDK
instance starts. That number covers both C binary layout and the strict JSON
schema and is incremented for an incompatible change to either. It catches
stale object files, build caches, partial development builds, and packaging
mistakes without adding per-message version fields or mixed-version support.
It does not make the Rust and OCaml parts independently replaceable.

## Why not a structured handle for every activation?

Native activation accessors and command-builder handles would avoid text
encoding, but they greatly increase the unsafe C surface and introduce more
cross-language lifetimes and cleanup paths. One owned document has the same
allocation contract for every activation and completion and is easier to test
under success, decode failure, cancellation, and shutdown.

## Consequences

- OCaml has no Temporal protobuf dependency or Core field-number knowledge.
- Core protocol upgrades remain localized to Rust plus one atomically changed
  adapter schema.
- Reviewers can inspect fixtures and schema rules without specialized wire
  tooling.
- JSON and base64 use more CPU and temporary memory than a custom binary
  protocol. This is accepted until profiling demonstrates a real bottleneck.
- The adapter must reject malformed documents deterministically and report a
  typed bridge error without leaking its owned result buffer.
