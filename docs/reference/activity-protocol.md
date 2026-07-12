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
copies it unchanged into a terminal completion obligation. If native transport
rejects that completion, the adapter retries the same copied value without
invoking the activity implementation again. This keeps the opaque correlation
value and the user-side execution aligned while Rust's outstanding-task ledger
decides whether the lease can be retired. The adapter never interprets token
bytes.

Start context retains the scheduling workflow identity, activity identity,
headers, ordered arguments, heartbeat details, timestamps, timeouts, one-based
attempt supplied by Core, normalized retry policy, task priority, and optional
standalone activity run ID. Cancellation context retains both Core's primary
reason and its independent detail flags. The completion result is a closed
variant: completed with an optional payload, failed, cancelled, or
will-complete-asynchronously.

The `will-complete-asynchronously` variant is protocol-only at this baseline.
The public activity adapter never emits it, and the existing worker completion
operation is for the original Core worker lease only. A future asynchronous
implementation must first acknowledge the handoff through that worker path,
then use a separate namespace-bound client operation for the later completion,
failure, cancellation, or heartbeat. Activity authors cannot construct this
variant or retain the ordinary attempt context today.

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
rules. The shared functions remain inside the `temporal-sdk` package's private
dependency tree and are not re-exported from the public `Temporal` library.

The typed activity token is `bytes`, although the JSON field is canonical
padded RFC 4648 base64. This avoids treating correlation data as text while
ensuring encoding always uses the audited binary codec. Retry backoff bits stay
as canonical unsigned 64-bit decimal text because all bit patterns do not fit
OCaml's signed `int64`. Activity attempt stays `int64` so the complete unsigned
32-bit protobuf domain is representable without depending on OCaml platform
integer width.

## Validation contract

`decode_task`, `decode_completion`, and `decode_heartbeat` first pass the
complete document through the duplicate-aware, bounded JSON foundation. Every
nested object is closed:
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

The OCaml encoders reparse their own output through the corresponding strict
decoders. The Rust bridge validates completion and heartbeat JSON again at the
worker ABI, so a value accepted by one language still crosses a second
validation boundary before reaching Core.

## Heartbeat document

An activity heartbeat is a progress message, not a completion. Its exact JSON
shape is:

```json
{
  "task_token": "AAEC/v8=",
  "details": [
    {
      "metadata": {
        "encoding": {"encoding": "base64", "data": "YmluYXJ5L3BsYWlu"}
      },
      "data": {"encoding": "base64", "data": "cHJvZ3Jlc3M="}
    }
  ]
}
```

The token must be the canonical padded base64 representation of the currently
leased activity token. Details preserve their order and use the same binary
payload representation as task inputs and completion outputs. The OCaml and
Rust validators reject unknown or duplicate object members, malformed base64,
empty tokens, and payloads outside the shared size and metadata rules. The Rust
bridge checks the token against its outstanding-task ledger before handing the
heartbeat to Temporal Core; it does not retire the lease. The adapter must
later submit a terminal completion, including a `Cancelled` result when it
receives a cancellation task; only that terminal completion path removes the
token from the ledger.

The schema is [`activity-heartbeat.schema.json`](../schemas/bridge/activity-heartbeat.schema.json).
The focused bilateral tests are
`test/bridge/test_ocaml_activity_protocol.ml` and
`rust/core-bridge/tests/activity_protocol.rs`. The Rust ownership regression
also drops the source JSON buffer after decoding, demonstrating that task
tokens, heartbeat details, and metadata survive without borrowing caller-owned
input memory.

Encoding performs the reverse conversion, normalizes object keys, then decodes
the produced JSON again. Invalid typed outgoing records therefore fail before
they can cross the C/Rust boundary. `error_view` exposes only a stable code,
path, and payload-safe message; source JSON, base64 data, token bytes, inputs,
results, and failure details are never included.

## Completion and cancellation

`encode_completion` validates the token and the closed result union before
emitting JSON. The Rust worker ABI decodes and validates that document again
before calling Temporal Core. It first checks that the token is currently
leased, then retires the ledger entry only after Core accepts the completion.
If transport or Core rejects the call, the lease remains outstanding so the
OCaml adapter can retry the same pending completion; it never reruns the user
activity merely because submission failed.

A task with `variant.kind = "cancel"` does not invoke user activity code. The
adapter maps its stable reason to a `Cancelled` completion with the standard
Temporal `Canceled` failure. The independent cancellation flags remain task
metadata and are not copied into an application payload. A heartbeat is
non-terminal and therefore cannot acknowledge cancellation or completion by
itself.

`will_complete_async` remains a protocol and Core-conversion variant, but the
current OCaml adapter neither emits it nor exposes a handle for a later
completion. Current activities therefore return a terminal result
synchronously; asynchronous completion remains a future capability.

## Schemas and tests

The Draft 2020-12
[`activity-task.schema.json`](../schemas/bridge/activity-task.schema.json),
[`activity-completion.schema.json`](../schemas/bridge/activity-completion.schema.json),
and [`activity-heartbeat.schema.json`](../schemas/bridge/activity-heartbeat.schema.json)
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
