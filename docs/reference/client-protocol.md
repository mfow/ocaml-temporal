# Native client JSON protocol

This document describes the private JSON messages used between the OCaml
client adapter and the Rust Temporal Core bridge. It is an implementation
boundary, not an API that workflow authors need to construct. Rust owns the
Temporal connection and protobuf types; OCaml owns the typed records and the
decision about how a workflow result is exposed.

## Why this protocol exists

Temporal Core is Rust code and the final executable is an OCaml executable.
The boundary therefore needs a representation that is easy to inspect,
validate, test, and free without sharing Rust or OCaml pointers. JSON is used
only for this private boundary. Temporal itself still receives protobuf over
gRPC through the official Rust client implementation.

Every operation copies input bytes before Rust releases the OCaml runtime lock.
Rust copies its output into an owned result buffer. The OCaml C stub copies that
buffer into an OCaml `bytes` value and frees the native result in a protected
cleanup path. No JSON string, payload pointer, or Rust future survives the
call.

## Start a workflow

The OCaml side sends one closed object:

```json
{
  "namespace": "default",
  "workflow_id": "summarize-1",
  "workflow_type": "summarize_document",
  "task_queue": "agents",
  "input": [
    {
      "metadata": {
        "encoding": {
          "encoding": "base64",
          "data": "anNvbi9wbGFpbg=="
        }
      },
      "data": {
        "encoding": "base64",
          "data": "eyJ0ZXh0IjoiSGkifQ=="
      }
    }
  ]
}
```

The example's `data` value is illustrative; the actual encoder emits valid
base64 without whitespace. `input` is ordered and may be empty. Payload
metadata and bytes use the shared workflow payload codec, so binary values are
never treated as UTF-8 text.

Rust validates every identifier, rejects NUL bytes, rejects duplicate or
unknown members, validates payloads, and then calls Core's raw
`WorkflowService::start_workflow_execution`. The first slice deliberately
uses Temporal Server's documented defaults for optional start policies; it does
not invent OCaml-side defaults.

On success Rust returns:

```json
{
  "execution": {
    "namespace": "default",
    "workflow_id": "summarize-1",
    "run_id": "server-assigned-run-id"
  }
}
```

OCaml checks that the returned namespace and workflow ID still match the
request before exposing the run ID. The complete shape is documented by
[`client-start-request.schema.json`](../schemas/bridge/client-start-request.schema.json)
and [`client-start-response.schema.json`](../schemas/bridge/client-start-response.schema.json).

## Wait for one exact run

The wait request contains exactly the three identity fields:

```json
{
  "namespace": "default",
  "workflow_id": "summarize-1",
  "run_id": "server-assigned-run-id"
}
```

There is intentionally no `follow_runs` member. Rust performs a close-event
history long poll with the equivalent of `follow_runs = false`, bounded to
100 ms per native call. When the run is still open, the call returns
`STATUS_NOT_READY` and no response object; the caller or a later orchestration
loop can retry the same request through its mailbox. A timeout is therefore a
pending observation, not a workflow failure. A terminal response always names
the exact run requested:

```json
{
  "execution": {
    "namespace": "default",
    "workflow_id": "summarize-1",
    "run_id": "server-assigned-run-id"
  },
  "outcome": {
    "kind": "completed",
    "result": [],
    "successor": null
  }
}
```

The closed outcome variants are:

| `kind` | Additional members | Meaning |
| --- | --- | --- |
| `completed` | `result`, nullable `successor` | The run completed normally. |
| `failed` | `failure`, nullable `successor` | The run failed with a structured Temporal failure. |
| `cancelled` | `details` | The run was cancelled. |
| `terminated` | `details` | The run was terminated. |
| `timed_out` | nullable `successor` | The run timed out. |
| `continued_as_new` | required `successor` | The requested run ended and created a new run. |

Whenever a successor is present, both sides enforce the same three invariants:

1. successor namespace equals the waited execution's namespace;
2. successor workflow ID equals the waited execution's workflow ID; and
3. successor run ID differs from the waited run ID.

This prevents a malformed response from changing which execution a caller is
observing. OCaml returns `continued_as_new` to the caller; it never follows the
successor implicitly. See
[`client-wait-request.schema.json`](../schemas/bridge/client-wait-request.schema.json)
and [`client-wait-response.schema.json`](../schemas/bridge/client-wait-response.schema.json).

## Structured failures

Routine transport and Core failures do not raise OCaml exceptions. Rust emits
one of these closed error documents in the native result's error buffer:

```json
{"kind":"already_started","workflow_id":"summarize-1","existing_run_id":null}
```

`already_started` is used only for a start operation rejected by Temporal's
AlreadyExists status. Other gRPC failures contain only one stable status code,
such as `deadline_exceeded`, `unavailable`, or `permission_denied`; server text
is intentionally discarded because it may contain user data. Core conversion
failures use `{"kind":"protocol","code":"core_invalid"}` or
`core_unsupported`. The complete code vocabulary is enumerated in the JSON
schema and checked by both Rust and OCaml decoders.

The OCaml protocol exposes an abstract `error` and a small `error_view` with a
code, JSON path, and safe message. Payload bytes and raw input documents never
appear in that view. Public API conversion is a later layer; this private
codec does not decide whether a workflow failure is retryable.

## Validation and ownership checklist

Both implementations validate their own outgoing representation and strictly
decode incoming data. In particular, they reject:

- missing, unknown, or duplicate object members;
- empty, oversized, or NUL-containing identifiers;
- non-canonical payload wrappers, invalid base64, and payload size violations;
- unknown outcome and error variants;
- successor identities that do not remain in the same execution chain; and
- status or protocol error codes outside the documented vocabulary.

The OCaml codec tests live in
`test/bridge/test_ocaml_client_protocol.ml`; Rust tests live beside
`rust/core-bridge/src/client_protocol.rs`. The machine-readable schemas are
normative documentation for the object shapes, while runtime checks remain
authoritative for duplicate keys, byte limits, and cross-field invariants.

The current milestone wires these messages through private OCaml/C/Rust
bindings and the single-owner supervisor. The public `Client` module still
uses its deterministic mock transport until the complete live worker/client
acceptance path is connected; that distinction is intentional and is recorded
in [`docs/progress.md`](../progress.md).
