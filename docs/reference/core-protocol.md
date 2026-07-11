# Private JSON Control Protocol

This contributor reference defines messages exchanged between the private
OCaml runtime and the project-owned Rust Temporal Core bridge. Workflow authors
never construct these documents, and this protocol is not a public API.

## Direction and lifecycle

Either side may send a `request`; the operation defines its direction. The
receiver returns exactly one terminal `response` or `error`, copying the
request's `correlation_id` and `operation`. A request remains pending until that
terminal message arrives or the owning SDK instance shuts down.

This foundation validates transport structure. A future worker operation must
define and validate a closed `body` object in both languages before changing
workflow or Core state. No current operation polls or completes Temporal work.
Rust alone reads and writes Temporal/Core protobuf. OCaml sees validated JSON
control values and decoded opaque payload bytes.

## Startup compatibility

Compatibility number `1` covers the C layout and JSON contract. The bridge
checks it once before creating an SDK runtime. It is absent from messages
because OCaml and Rust are compiled and shipped together. A different number
means a stale or partial build and fails startup; per-message negotiation and
mixed versions are unsupported.

## Envelope and correlation

Requests and successful responses have exactly four fields:

```json
{"kind":"request","correlation_id":"0123456789abcdef0123456789abcdef","operation":"worker.poll","body":{}}
```

`kind` is exactly `request`, `response`, or `error`. A correlation identifier is
32 lowercase hexadecimal characters. It is opaque, not a workflow/run ID,
tracing secret, or database key. Operation names are 1 to 64 ASCII characters,
start with a lowercase letter, and otherwise contain lowercase letters, digits,
`_`, or `.`.

`body` is a JSON object. The foundation enforces global resource and JSON rules;
each future operation must close its body schema and reject its unknown fields.
Arrays, primitives, and opaque serialized protobuf are not envelope bodies.

Failed responses replace `body` with a closed error object:

```json
{"kind":"error","correlation_id":"0123456789abcdef0123456789abcdef","operation":"worker.poll","error":{"code":"invalid_message","message":"request body was invalid","retryable":false}}
```

Codes are `invalid_message`, `unsupported_message`, or `internal_bridge`.
`message` is non-empty UTF-8 of at most 1,024 bytes. `retryable` says whether an
unchanged retry might succeed. Expected failures are structured `result`
values; exceptions, panics, raw parser diagnostics, and Core failures do not
escape their boundaries.

## Strict rules and finite limits

Both incoming and outgoing paths enforce:

| Resource | Limit |
|---|---:|
| Complete UTF-8 document | 1,048,576 bytes |
| Object/array nesting | 16 levels, outer value included |
| One decoded string or object key | 65,536 UTF-8 bytes |
| Members in one object | 256 |
| Elements in one array | 256 |
| Values in the complete tree | 4,096 |
| Decoded opaque payload | 262,144 bytes |
| Error message | 1,024 UTF-8 bytes |

Numbers must be integral and fit the common signed 64-bit range. Domains that
may exceed portable JSON integers must use validated decimal strings in their
future operation schema. Fractional numbers, trailing input, and Yojson
extensions are invalid.

Closed objects reject missing and unknown fields. Every object, including body
objects, rejects duplicate member names while the entry sequence still exists;
decoding directly into a map would silently lose that evidence. JSON Schema
cannot prove this property because many tools receive a map after duplicates
have been discarded.

## Normalized output and outgoing validation

Output is UTF-8 JSON without insignificant whitespace. Envelope and error fields
use the order shown above. Body keys are sorted recursively, arrays retain order,
and integers use shortest decimal form. Maintained serde_json and Yojson
encoders own string escaping.

Before sending, each side validates the typed value, serializes it, and passes
the bytes through its strict incoming decoder. The receiver repeats strict
parsing and validation. Normalization supports review and golden tests; it is
not a cryptographic signing format.

## Opaque payload bytes and privacy

Payloads use a separate closed object:

```json
{"encoding":"base64","data":"AAEC/v8="}
```

`encoding` is exactly `base64`; `data` is canonical padded RFC 4648 base64.
Unpadded, over-padded, non-alphabet, non-canonical, or decoded values over the
limit are rejected. This control encoding does not require workflow authors to
choose a JSON Temporal payload codec.

Protocol errors and logs may contain kind, operation, correlation identifier,
path, limit name, and stable error code. They must never contain source JSON,
payload base64, decoded payload bytes, workflow inputs/results, auth material,
or Core diagnostics that might embed those values.

## Schemas and fixtures

Draft 2020-12 schemas live under [`docs/schemas/bridge`](../schemas/bridge/).
Shared positive and malformed fixtures under `test/bridge/fixtures/protocol`
drive both OCaml and Rust tests. Runtime validators remain authoritative for
duplicate keys, decoded payload length, aggregate node count, normalization,
privacy-safe errors, and every UTF-8 byte limit. JSON Schema `maxLength` counts
Unicode characters rather than encoded UTF-8 bytes, so it documents a useful
upper bound but cannot enforce the byte-count contract; schema validation alone
is insufficient.
