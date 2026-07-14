# Native client JSON protocol

This document describes the private JSON messages used between the OCaml
client adapter and the Rust Temporal Core bridge. It is an implementation
boundary, not an API that workflow authors need to construct. Rust owns the
Temporal connection and protobuf types; OCaml owns the typed records and the
decision about how a workflow result is exposed.

The public `Temporal.Client` module does not expose these JSON documents,
start tickets, or native status codes. On an HTTP(S) client, `start` returns a
typed exact-run handle, `wait` hides the bounded polling loop and returns a
typed terminal value, while `cancel` and `signal` return after Temporal
acknowledges their control-plane requests. The sections below describe the
private steps that make those public operations safe.

## Why this protocol exists

Temporal Core is Rust code and the final executable is an OCaml executable.
The boundary therefore needs a representation that is easy to inspect,
validate, test, and free without sharing Rust or OCaml pointers. JSON is used
only for this private boundary. Temporal itself still receives protobuf over
gRPC through the official Rust client implementation.

Every operation copies input bytes before Rust releases the OCaml runtime lock.
Rust copies its output into an owned result buffer. The OCaml C stub copies that
buffer into an OCaml `bytes` value and frees the native result in a protected
cleanup path. Synchronous operations do not retain a JSON string or payload
pointer after the call. The asynchronous start operation retains only a
Rust-owned typed request inside its bounded Tokio task; its ticket and final
outcome cross the same copied JSON result boundary.

## Start a workflow

The OCaml side sends one closed object:

```json
{
  "request_id": "start-summarize-1",
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
not invent OCaml-side defaults. The public `Temporal.Client.start` function
accepts an optional `request_id`. When it is supplied, that caller-owned value
is sent unchanged to Temporal; callers should reuse it when retrying a start
whose outcome is uncertain. When it is omitted, the adapter allocates one fresh
ID for that call. The resulting protocol request is created once and reused by
the bounded ticket polls, so polling does not accidentally change the
idempotency key. A request ID identifies one logical start and must not be
reused for unrelated workflow starts.

The direct `start_workflow_json` ABI can return the successful response shown
above, but the public HTTP(S) client uses the asynchronous ticket path. It
begins the request, waits for the ticket to become terminal, and converts the
accepted execution into the typed handle; rejected and unknown outcomes become
typed `Error.t` results. The ticket never leaves the private supervisor.

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

### Asynchronous start tickets

The owner supervisor can submit the same request through the private
`begin_start_workflow_json` operation when it must keep servicing other
messages while Temporal performs the RPC. Rust returns an opaque ticket:

```json
{"ticket":"4a7c3e0e-3e3d-4b9f-9df2-6e55d3b2b4b7"}
```

The supervisor supplies that object to either `poll_start_workflow_json` or
`wait_start_workflow_json`. Poll returns immediately; wait blocks for at most
the bridge's short bounded interval and then returns `STATUS_NOT_READY`, so a
mailbox loop can handle shutdown and other lifecycle messages between waits.
When the RPC is terminal, the ticket is retired and Rust returns one of these
closed values:

```json
{"kind":"accepted","execution":{"namespace":"default","workflow_id":"summarize-1","run_id":"run-1"}}
```

```json
{"kind":"rejected","error":{"kind":"already_started","workflow_id":"summarize-1","existing_run_id":null}}
```

```json
{"kind":"unknown","request_id":"start-summarize-1","workflow_id":"summarize-1"}
```

`accepted` is proof that Temporal allocated the run. `rejected` is used only
when the returned status proves the start was not accepted. `unknown` is
deliberately not a retry instruction: a timeout, transport failure, or
response-conversion failure may have happened after Temporal accepted the
request. The caller must reconcile that logical request using its stable
`request_id` and workflow identity before deciding what to do next. The ticket
and outcome schemas are
[`client-start-ticket.schema.json`](../schemas/bridge/client-start-ticket.schema.json)
and
[`client-start-outcome.schema.json`](../schemas/bridge/client-start-outcome.schema.json).

The runtime owns the ticket's receiver, validated request, and Tokio task
until one terminal read retires the ticket. If a caller abandons the ticket,
shutdown drains the ticket registry, aborts every remaining task, and joins
each handle before the client or Core runtime is released. A task that has
already placed a result in its receiver is still joined exactly once; the
queued result is then dropped with the receiver rather than being delivered to
an absent caller. This is the cancellation boundary for asynchronous starts:
Rust tasks never call OCaml, and no task is detached while it retains a Core
connection clone.

## Request cancellation of one exact run

The public `Temporal.Client.cancel` operation sends a control-plane request for
the exact execution retained by a workflow handle. The OCaml side supplies the
client namespace and these five fields to Rust:

```json
{
  "namespace": "default",
  "workflow_id": "summarize-1",
  "run_id": "server-assigned-run-id",
  "request_id": "cancel-summarize-1",
  "reason": "operator requested shutdown"
}
```

`run_id` is required. There is no implicit “latest run” form, so a cancellation
cannot accidentally target a continued-as-new successor or another execution
with the same workflow ID. `request_id` is the Temporal idempotency key for the
logical cancellation operation. If a transport timeout leaves the outcome
uncertain, the caller should retry with the same request ID and exact handle.
If the public `Temporal.Client.cancel` caller omits `request_id`, OCaml derives
a deterministic ID from that handle's workflow ID and run ID, so repeated
attempts for the same exact handle still identify one logical cancellation.
The optional `reason` is copied as operator context and may be empty; it is
bounded and NUL-free like all bridge strings.

Rust calls Temporal's official `RequestCancelWorkflowExecution` RPC and returns
only this positive acknowledgement:

```json
{"acknowledged":true}
```

The acknowledgement means that Temporal accepted the request, not that the
workflow has already stopped. The request is bounded to a short native RPC
deadline so a stalled server cannot hold the single supervisor owner forever.
On timeout the operation returns a typed bridge failure; retrying the same
`request_id` is safe. The caller then uses `Temporal.Client.wait handle` to
observe the eventual `Cancelled` terminal value. Cancellation errors use the
same closed `rpc` and `protocol` error documents as other client operations;
`already_started` is rejected as impossible for this operation.

The request and acknowledgement shapes are defined by
[`client-cancel-request.schema.json`](../schemas/bridge/client-cancel-request.schema.json)
and
[`client-cancel-response.schema.json`](../schemas/bridge/client-cancel-response.schema.json).
Both OCaml and Rust validate every field, reject unknown/duplicate members,
and validate the positive acknowledgement before it crosses the FFI boundary.
The exact-run cancellation path is covered by local mock, supervisor, OCaml
bridge, and Rust protocol tests. The live driver contains the same scenario,
and the complete [PR #253 run](https://github.com/mfow/ocaml-temporal/actions/runs/29286560471)
verified exact-run cancellation, the eventual typed cancelled result, and
graceful shutdown with outstanding work against a real Temporal Server. The
earlier [PR #210 run](https://github.com/mfow/ocaml-temporal/actions/runs/29221151859)
remains historical evidence for the original nine-workflow slice. See the
[live acceptance coverage](live-acceptance-coverage.md) for the remaining
evidence boundary.

## Send one signal to an exact run

The public `Temporal.Client.signal` operation sends a typed, fire-and-forget
message to the exact workflow run retained by a client handle. It does not
start a workflow, wait for a handler, or follow a continued-as-new successor.
The signal definition supplies the stable Temporal name and its input codec;
the caller receives `Ok ()` only after Temporal acknowledges the RPC.

The private request is a closed object with the same exact-run identity fields
used by cancellation, plus the signal name, idempotency key, and ordered
payload list:

```json
{
  "namespace": "default",
  "workflow_id": "summarize-1",
  "run_id": "server-assigned-run-id",
  "signal_name": "add_document",
  "request_id": "signal-summarize-1-1",
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

OCaml validates the signal name when `Temporal.Signal.define` constructs the
definition and encodes the input before transport. Rust validates the exact
identifiers, request ID, signal name, payload conversions, and closed JSON
shape again before constructing Temporal's official
`SignalWorkflowExecutionRequest` protobuf. The connected client's identity is
used for the RPC; callers cannot provide a second identity or redirect the
request to another namespace.

When `request_id` is omitted, OCaml allocates a fresh process-wide ID shared by
all `Temporal.Client.t` values in that process. This keeps two independent
handles from accidentally presenting the same signal as a retry of an earlier
delivery. Supply an explicit ID when retrying an uncertain transport result so
Temporal can deduplicate the same logical signal. A successful native response
is exactly:

```json
{"acknowledged":true}
```

This acknowledgement says only that Temporal accepted the signal request; a
workflow task may process it later or the run may already be closing. Signal
failures use the closed `rpc` and `protocol` client error documents, while the
start-only `already_started` category is rejected as impossible. Both sides
reject unknown or duplicate members and validate the positive acknowledgement.
The bridge bounds this control-plane RPC to one second, matching cancellation:
an unavailable server cannot hold the supervisor's single owner Domain
indefinitely, and a timeout is returned as a typed `deadline_exceeded` error.
Callers that retry an uncertain result should reuse the same `request_id`.
The request and response shapes are defined by
[`client-signal-request.schema.json`](../schemas/bridge/client-signal-request.schema.json)
and
[`client-signal-response.schema.json`](../schemas/bridge/client-signal-response.schema.json).

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
the exact run requested.

The public `Temporal.Client.wait handle` performs that retry loop internally:
it resubmits the same exact-run request after each bounded `NOT_READY` result
and yields the calling Domain between attempts. Code using the private bridge
directly may handle the status itself, but ordinary client callers receive only
a terminal `Ok` value or an outer typed `Error.t`.

For example, a terminal response has this shape:

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

The public client exposes the successor as an opaque-to-codec
`Temporal.Client.execution` value containing the validated workflow and run
identity and its namespace. `Temporal.Client.follow client ~workflow successor`
combines that identity with the caller's existing client and workflow
definition to produce a typed exact-run handle, but first requires the
successor namespace to equal the client's configured namespace. This is not
another protocol message: no start or lookup is sent to Temporal, and no
successor is selected implicitly. The operation only checks the local lifecycle
bit and the same non-empty, NUL-free, 65,536-byte identifier limits used by
`start` and `wait`; malformed, cross-namespace, or shut-down-client input is
returned as an ordinary `Error.t` result.

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

Status details are server input too. Rust includes an existing run ID only
when it is non-empty, within the protocol string limit, and free of NUL bytes.
If the optional detail is malformed, the error remains `already_started` but
its `existing_run_id` is `null`. This keeps the status category and JSON body
consistent with the same identifier validation used for OCaml-originated
requests.

The OCaml protocol exposes an abstract `error` and a small `error_view` with a
code, JSON path, and safe message. Payload bytes and raw input documents never
appear in that view. Public API conversion is a later layer; this private
codec does not decide whether a workflow failure is retryable.

At the public boundary, a bridge or codec problem is the outer `Error.t` from
`Client.start`, `Client.wait`, or `Client.cancel`. A workflow that reached a
Temporal terminal state instead remains inside the successful result: for
example, `Client.wait` returns `Ok (Failed error)` or `Ok (Cancelled error)`.

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
[`test/bridge/test_ocaml_client_protocol.ml`](../../test/bridge/test_ocaml_client_protocol.ml).
Rust protocol unit tests live beside
[`rust/core-bridge/src/client_protocol.rs`](../../rust/core-bridge/src/client_protocol.rs),
and ABI-level client tests live in
[`rust/core-bridge/tests/client_bridge.rs`](../../rust/core-bridge/tests/client_bridge.rs).
The machine-readable schemas are
normative documentation for the object shapes, while runtime checks remain
authoritative for duplicate keys, byte limits, and cross-field invariants.

The current milestone wires these messages through private OCaml/C/Rust
bindings and the single-owner supervisor. Public `Temporal.Client` uses this
native path for `http://` and `https://` targets, including asynchronous start
and exact-run wait. The deterministic `mock://` transport remains available
only as a private unit-test seam. A historical green two-binary Compose run
exercises native client starts and exact-run waits while a public worker polls
and dispatches the first success scenarios. The current driver also contains
heartbeat-detail retry and exact-run cancellation assertions, but those paths
are locally covered rather than live evidence until a run completes
successfully. The boundary and remaining cases are tracked in the
[`two-OCaml-binary acceptance design`](two-ocaml-binary-e2e-acceptance.md).
