# Internal replay worker bridge

This reference describes the bounded replay slice behind the private Rust
bridge. It is an implementation component, not a public OCaml API and not live
Temporal acceptance evidence. The public worker still executes normal server
activations; this worker is used to feed recorded histories to Temporal Core's
replay implementation in deterministic tests and in the later restart
acceptance fixture.

## Why this layer exists

Temporal Core already owns the difficult replay state machine: it turns a
recorded history into workflow activations and checks the workflow's commands
against that history. The bridge should not duplicate that state machine or
make OCaml parse Temporal's generated protobuf types. Instead, Rust validates a
small JSON document, converts its history body to Core's `HistoryForReplay`,
and owns the Core replay worker until it is finalized.

The history feeder is deliberately bounded to one queued history. A caller
that submits a second history waits until Core has consumed the first one.
This keeps replay order explicit and prevents malformed or slow input from
creating an unbounded native allocation. The feeder is FIFO, and dropping its
sender is the documented end-of-input signal.

## Document format

One history is represented by the closed document in
[`replay-history.schema.json`](../schemas/bridge/replay-history.schema.json):

```json
{
  "workflow_id": "workflow-replay-test",
  "history": {
    "encoding": "base64",
    "data": "<canonical padded standard-base64 protobuf>"
  }
}
```

`workflow_id` is separate because Temporal's `History` protobuf carries event
data but does not carry the workflow identity needed to construct a replay
task. `history.data` is the official Temporal Core `History` protobuf encoded
as padded standard base64. JSON is only the private OCaml/Rust representation;
the Temporal Server never receives this document.

The schema describes the shape, but it is not the complete security or
correctness check. The OCaml sender first applies the shared strict JSON and
canonical-payload validator. The Rust decoder then repeats the checks at the
FFI boundary and also:

- rejects duplicate members before Serde decoding and rejects unknown members
  in both the outer and nested objects;
- rejects empty, NUL-containing, or over-limit workflow IDs;
- accepts only `encoding: "base64"`, decodes at most the bridge payload limit,
  and compares the input with a canonical re-encoding; and
- decodes the protobuf and runs Core's `HistoryInfo` invariant validation
  before constructing `HistoryForReplay`.

The encoder uses the same invariant gate and immediately decodes its own JSON
output. This makes a future OCaml adapter fail at the Rust boundary instead
of handing a partially checked history to Core. Parser and Core diagnostics
are mapped to stable private categories; workflow-controlled bytes and server
error text are not copied into an ABI diagnostic.

## OCaml supervisor operation

The public `Temporal` module does not expose a replay handle. Internally,
`Sdk_supervisor.Native_backend` sends these typed operations to the one
supervisor Domain that owns the runtime:

1. `Start_replay_worker` creates the workflow-only Core worker. It is mutually
   exclusive with the live worker and does not require a client connection.
2. `Feed_replay_history` validates and queues one history. The feeder has one
   slot, so a second feed waits for Core to consume the first history.
3. `Wait_replay_workflow` wakes the supervisor when Core has an activation or
   has reached end-of-input. `Try_poll_replay_workflow` then converts the
   activation to the normal typed OCaml workflow protocol and retains its
   completion lease.
4. `Complete_replay_workflow` encodes the typed completion and submits it for
   the exact retained run. If OCaml cannot decode a handoff, the supervisor
   sends the untouched document to `Reject_replay_workflow` so Core's debt is
   retired rather than stranded.
5. `Finish_replay_input` closes the feeder. After all activations are completed
   and `Wait_replay_workflow` has observed `Shutdown`, `Finalize_replay` joins
   the lanes. `Dispose_replay` is the explicit abandonment path used by
   shutdown and error cleanup.

The C stubs copy every OCaml `bytes` value before releasing the OCaml runtime
lock. Rust owns the Core worker and all poll tasks; no OCaml continuation,
pointer, or future is retained by the feeder. This keeps replay communication
on the same single-owner mailbox path as live worker operations while allowing
Rust/Tokio to run Core's internal tasks concurrently.

## Ownership and shutdown

`ReplayWorker` owns two values:

1. a workflow-only `PollLanes` instance, which owns Core's worker and the
   guarded poll/join state; and
2. an optional `HistoryFeeder` sender, whose single owner controls when input
   ends.

Core's Tokio runtime owns network-independent replay tasks. The bridge enters
that runtime only while constructing the worker or driving a feeder send. No
OCaml pointer, callback, continuation, or Rust future is stored in the feeder.
The replay worker uses `PollLanes::start_workflow_only`, so no activity poller
is started for a worker that cannot receive activities.

Normal finalization is intentionally stricter than live-worker disposal. The
caller closes the feeder, continues taking and completing every workflow
activation, and calls `wait_workflow` until it observes `Shutdown`. Only then
does `finalize` join the already-terminating poll lane and consume Core's
terminal worker future. A feeder close by itself is not enough: if a queued
history or completion debt remains, `finalize` returns a typed
`ReplayNotDrained` error together with the still-owned worker. This prevents a
queued history from being cancelled while looking like a successful replay.

When abandonment is intentional, `dispose` is the separate destructive path.
It initiates Core shutdown, force-completes queued or leased work, joins the
poll lane, and attempts terminal finalization twice. Each force-completed
workflow identity is retired before its asynchronous Core failure is awaited;
late duplicate workflow polls therefore cannot become new ledger entries in the
snapshot/queue-drain window. It must never be used as replay success evidence.
A poll-lane failure performs the same force-completion cleanup before returning
the worker and typed error. If both terminal finalization attempts fail,
`dispose` returns the worker and a `Finalization` error together; the caller
must retry disposal or otherwise retain that owner. The bridge never silently
drops the unfinalized native graph.

## Current evidence and limits

The focused Rust tests in
[`tests/support/replay_bridge.rs`](../../rust/core-bridge/tests/support/replay_bridge.rs)
cover:

- valid history encode/decode round trips;
- duplicate and unknown JSON field rejection;
- base64 and malformed-protobuf rejection;
- construction and clean shutdown without a Temporal client; and
- admission, activation completion, natural shutdown, and finalization of one
  valid history through the bounded feeder; and
- rejection of finalization after the feeder is closed but before its queued
  history is drained, with explicit disposal of the retained worker; and
- retention and reporting of a still-shared Core worker during disposal,
  reporting of a joined poll-lane failure, and successful retry after each
  retained owner is safe to release.

The ABI-focused integration test in
[`tests/replay_abi.rs`](../../rust/core-bridge/tests/replay_abi.rs) adds null
handle, missing-worker, malformed-document, and idempotent-disposal coverage.
The OCaml bridge test in
[`test_ocaml_bridge.ml`](../../test/bridge/test_ocaml_bridge.ml) proves that
sender-side canonical-payload validation rejects malformed replay input before
it reaches Rust.

This is **unit-tested native and supervisor plumbing**, not proof that an OCaml
workflow replays successfully against a real server. The Rust ABI exports,
OCaml supervisor operations, strict sender/receiver validation, and lifecycle
cleanup paths now have focused tests. The live restart design remains in
[`worker-restart-replay-acceptance.md`](worker-restart-replay-acceptance.md);
it must not be marked live until a two-generation Compose test observes the
exact run, replay marker, terminal result, and volume cleanup.
