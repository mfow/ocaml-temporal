# ADR 0006: First Workflow Semantic Protocol

- Status: accepted
- Date: 2026-07-11
- Decision owners: OCaml Temporal maintainers

## Context

ADR 0002 chose strict JSON instead of exposing Temporal Core protobuf to
OCaml. The first worker slice now needs an exact bilateral contract for normal
workflow initialization, activity and timer progress, cache eviction, and the
matching completion commands. Silently omitting a Core field would make replay
or scheduling behavior depend on adapter accidents, while mirroring every Core
message immediately would create a large unverified surface.

## Decision

Rust and OCaml implement the same closed semantic types and validate all input
and output. Rust converts only at the official pinned Core protobuf boundary.
The initial activation surface includes initialization, activity resolution,
timer firing, cancellation, and eviction. The completion surface includes
remote activities, timers, and terminal workflow commands.

Normal first-task initialization preserves headers, identity, parent and root
execution identity for child workflows, three workflow timeouts, first execution run ID, start time, priority, attempt, arguments, workflow
identity and type, and the full unsigned 64-bit randomness seed. It also
preserves activation-wide internal flags, history size, continue-as-new advice,
deployment identity, SDK version, and deployment-change state. Other
initialization fields remain explicitly unsupported until their behavior is
implemented and tested. A non-default unsupported Core field fails conversion;
it is never silently dropped.

Binary payloads preserve both data and metadata as canonical padded base64.
Metadata and header maps normalize lexicographically in both languages.
Priority fairness weights use their unsigned IEEE-754 bit pattern, preserving
Core's `f32` exactly without weakening the protocol's integral-number rule.
Sequence numbers and history length use unsigned 32-bit integers, while values
that may span unsigned 64-bit use canonical decimal strings. Exact protobuf
time components avoid floating-point conversion. Sequence zero remains valid:
Core defines sequence numbers as supplied by the language SDK and exercises
zero in its own timer and activity tests.

Core's synthetic cache-eviction activation has no timestamp. The semantic
timestamp is therefore nullable only when eviction is the activation's sole
job; ordinary activations require exact timestamp components. Initialization,
when present, is unique and first. Temporal identifiers are nonempty and use
the bridge's 65,536-byte string safety ceiling rather than an invented server
limit because the server policy is configurable.

The bridge permits 128 MiB in any one opaque payload byte field and 192 MiB in
one complete JSON document. These finite transport safety ceilings correspond
to pinned Core's default 128 MiB inbound gRPC limit plus base64 and structural
headroom; they are not Temporal namespace blob limits. The document cap is
aggregate, so batching does not multiply it. Raw document and encoded base64
lengths are rejected before expensive parsing or decoding where possible.
Per-instance configurability is deferred until the lifecycle configuration
surface exists.

Collection and total-node counters use the document byte ceiling itself,
because every member and node consumes at least one input byte. They remain
finite consistency guards but cannot reject a document that fits solely because
it contains more small jobs. Nesting is capped at 128 levels to match
serde_json's default recursion guard; both sides accept recursive failure
chains beyond 16 levels and reject depth beyond that shared stack-safety bound.
An iterative or configurable depth implementation is deferred rather than
disabling the Rust parser's recursion protection.

Shared positive and malformed fixtures are decoded and normalized by both
implementations. A realistic initialization fixture deliberately reverses map
keys and carries the ordinary metadata above. Draft 2020-12 schemas document
every closed variant. Runtime validation remains authoritative for duplicate
keys, UTF-8 byte counts, aggregate limits, canonical base64, and invariants that
JSON Schema cannot express.

## Consequences

- OCaml remains independent of protobuf and generated Core types.
- Ordinary root workflow initialization is lossless for this slice.
- Unknown Core oneofs, enum values, external payloads, and non-default omitted
  features return typed conversion errors.
- The native worker operations can carry one owned JSON document without adding
  per-field C accessors or cross-language object lifetimes.
- Later protocol slices must expand Rust, OCaml, schemas, fixtures, conversion
  tests, and documentation together.
