# OCaml remote activity protocol adapter

`Temporal_protocol.Activity_protocol` is the private OCaml representation of
the remote activity task and completion JSON exchanged with the Rust Temporal
Core bridge. It is an internal worker building block, not an API that workflow
or activity authors construct directly. Rust remains the only protobuf
boundary; this module sees ordinary OCaml records, variants, byte strings, and
typed validation failures.

## Direction and ownership

Rust sends one `task` after Core has leased a remote activity attempt. The task
contains an opaque binary `task_token` and either a complete start context or a
cancellation update. The native OCaml activity adapter retains the token and
copies it unchanged into exactly one `completion`. The adapter never interprets
token bytes. Rust's outstanding-task ledger remains responsible for proving
that a completion names a currently leased attempt.

Start context retains the scheduling workflow identity, activity identity,
headers, ordered arguments, heartbeat details, timestamps, timeouts, one-based
attempt supplied by Core, normalized retry policy, task priority, and optional
standalone activity run ID. Cancellation context retains both Core's primary
reason and its independent detail flags. The completion result is a closed
variant: completed with an optional payload, failed, cancelled, or
will-complete-asynchronously.

The module has no native handle, mutable global registry, Domain, fiber, or
callback. Decoding allocates OCaml-owned values. Encoding reads those values
and returns a new JSON string. Native worker lifecycle and serialization remain
the supervisor's responsibility, so this adapter adds no race or
cross-language memory-ownership path.

## Shared semantic codecs

Activity JSON deliberately reuses the workflow protocol's canonical codecs for
Temporal payloads, recursive failures, timestamps, durations, workflow
executions, and task priority. This prevents the two OCaml paths from acquiring
different base64, nanosecond, failure-depth, identifier, or floating-point-bit
rules. The shared functions remain inside `temporal-sdk.internal_protocol` and
are not re-exported from the public `Temporal` library.

The typed activity token is `bytes`, although the JSON field is canonical
padded RFC 4648 base64. This avoids treating correlation data as text while
ensuring encoding always uses the audited binary codec. Retry backoff bits stay
as canonical unsigned 64-bit decimal text because all bit patterns do not fit
OCaml's signed `int64`. Activity attempt stays `int64` so the complete unsigned
32-bit protobuf domain is representable without depending on OCaml platform
integer width.

## Validation contract

`decode_task` and `decode_completion` first pass the complete document through
the duplicate-aware, bounded JSON foundation. Every nested object is closed:
missing, unknown, and duplicate fields fail. Fields documented as nullable must
still be present with either their value or JSON `null`.

Semantic validation then enforces:

- nonempty canonical task tokens within the opaque-byte ceiling;
- nonempty workflow, run, namespace, activity, and activity-type identifiers;
- nonempty unique header keys, with maps normalized lexicographically;
- signed timestamp seconds and nonnegative duration seconds, both with
  nanoseconds from 0 through 999,999,999;
- unsigned 32-bit attempts and priority-weight bits, signed 32-bit priority and
  maximum-attempt fields, and Core's 64-byte fairness-key limit;
- canonical unsigned 64-bit decimal retry backoff bits, validated retry
  intervals, and bounded non-retryable failure-type strings; and
- the same bounded recursive structured failure and payload semantics used by
  workflow activations and completions.

Encoding performs the reverse conversion, normalizes object keys, then decodes
the produced JSON again. Invalid typed outgoing records therefore fail before
they can cross the C/Rust boundary. `error_view` exposes only a stable code,
path, and payload-safe message; source JSON, base64 data, token bytes, inputs,
results, and failure details are never included.

## Schemas and tests

The Draft 2020-12
[`activity-task.schema.json`](../schemas/bridge/activity-task.schema.json) and
[`activity-completion.schema.json`](../schemas/bridge/activity-completion.schema.json)
files document the closed wire shapes and link to the existing payload and
failure schemas. Runtime validation remains authoritative for duplicate keys,
UTF-8 byte counts, decoded byte sizes, exact unsigned 64-bit range, aggregate
document limits, and normalized output because JSON Schema cannot prove all of
those properties.

Separated OCaml tests in
`test/bridge/test_ocaml_activity_protocol.ml` cover all task and completion
variants, binary token preservation, required-nullable members, closed nested
objects, identifiers, headers, time and duration domains, attempt and retry
numeric ranges, priority bits, and sender-side duplicate-map rejection.
