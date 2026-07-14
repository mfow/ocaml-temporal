# Native workflow interactions

This document specifies how Temporal Core signals, queries, and updates cross
the private Rust/OCaml boundary. It is both a design reference and an
implementation-status record. The bilateral bridge now transports
`SignalWorkflow`, `QueryWorkflow`, and the first bounded `DoUpdate` activation
slice safely through Rust, semantic JSON, and the OCaml runtime. Signals are
queued on the workflow scheduler; queries are answered synchronously by a
registered read-only handler; and a native update handler currently runs to a
single result in the activation that delivered it. Suspended update handlers
and their later-activation completion records remain future work.

For the public, in-memory typing that already exists, see
[Signals, queries, and updates](../reference/interactive-workflows.md). That
page describes `Temporal.Interaction`, an immutable local dispatcher. It does
not contact Temporal Server and must not be presented as native interaction
delivery.

## Scope and sources of truth

Temporal Core is pinned by the Rust workspace to commit
`95e97686a079dcfe6c42e3254b2f3f5e3d97408f` of `temporalio/sdk-core`. The
authoritative interaction definitions are the pinned Core
`workflow_activation.proto` and `workflow_commands.proto` files:

| Core message | Direction | Meaning |
| --- | --- | --- |
| `SignalWorkflow` | Core -> language SDK | Deliver a signal to a running workflow. |
| `QueryWorkflow` | Core -> language SDK | Ask a workflow for a read-only result. |
| `DoUpdate` | Core -> language SDK | Run an update validator and then an update handler. |
| `QueryResult` | language SDK -> Core | Answer one query activation. |
| `UpdateResponse` | language SDK -> Core | Acknowledge, reject, or complete one update. |

The Core protobuf types remain Rust-only. OCaml receives a closed semantic
record over the existing JSON bridge, and Rust converts that record to or from
Core protobuf. This keeps protobuf ownership, network I/O, and Core handles on
the Rust side while giving the OCaml runtime a small type-checked vocabulary.

## Current boundary

The current semantic protocol supports initialization, activity and child
resolutions, timers, cancellation, eviction, `SignalWorkflow`, and
`QueryWorkflow`/`QueryResult`, and the immediate `DoUpdate`/`UpdateResponse`
slice. A signal is validated in Rust, encoded as semantic JSON, decoded and
copied by OCaml, and represented as a runtime signal job in the original
activation order. A query activation is validated as a query-only batch,
answered inline on the execution owner Domain, and returned as query-only
completion commands. An update job is validated and copied in the same way,
looked up by name, and either rejected or dispatched through the typed public
update handler. A successful non-suspending handler produces an accepted
response followed by a completed response; a validator, input, or handler
error produces a structured rejection.

Consequently:

- `SignalWorkflow` is deliverable to a registered OCaml workflow handler,
  including its name, repeated payload list, sender identity, and headers.
  The handler is queued on the execution scheduler. The public handler policy
  accepts exactly one payload; a missing handler or unsupported arity is logged
  and completed as a non-retryable workflow failure rather than acknowledged
  as a no-op.
- `QueryWorkflow` is deliverable to a registered output-only OCaml query
  handler. Core's repeated arguments and headers remain in the private runtime
  record. Until a typed-input public query API exists, any non-empty argument
  list returns `QueryResult.failed` and is not silently truncated. Missing
  handlers and handler errors use the same failed-query response and leave the
  workflow execution unchanged.
- `DoUpdate` is deliverable to a registered OCaml update handler when the
  handler accepts exactly one input payload and returns without suspending.
  The private runtime retains the full input, headers, identity, metadata ID,
  protocol-instance ID, and replay-validation flag while the public handler
  currently exposes only the typed input value. Missing handlers and
  unsupported input arity are rejected as typed non-retryable workflow errors.
  The bridge accepts an accepted-plus-completed pair in one activation,
  completion-only responses in later activations, and accepted-plus-rejected
  terminal responses. It rejects duplicate acceptance or terminal responses.
- `Temporal.Interaction` can exercise typed handler registration, codec
  validation, duplicate-name rejection, and validator ordering locally.
- The signal transport tests prove the native activation boundary. Separately,
  the typed signal/condition success path is live-verified against Temporal
  Server in the [PR #266 Actions run](https://github.com/ocaml-temporal/actions/runs/29311239247):
  the driver signals an exact run only after its worker-visible readiness
  marker, and the handler wakes a deterministic condition before returning its
  terminal value. That run does not establish live query or update delivery;
  the focused scheduler and bridge tests remain the evidence for those paths.

Unsupported Core fields and oneof variants still fail explicitly. This is
intentional: a newer Core field or update metadata field must not silently
disappear at the language boundary.

## End-to-end mapping

The native path will retain one ownership boundary and four semantic stages:

```mermaid
flowchart LR
    C[Temporal Core protobuf] --> R1[Rust strict Core conversion]
    R1 --> J1[Workflow activation JSON]
    J1 --> O1[OCaml strict decoder]
    O1 --> O2[Activation/job translator]
    O2 --> O3[Workflow scheduler and interaction handlers]
    O3 --> O4[Semantic completion JSON]
    O4 --> R2[Rust strict completion conversion]
    R2 --> C2[Temporal Core protobuf]
```

Each stage validates before handing ownership to the next stage:

1. Rust reads the Core oneof and constructs a typed semantic value. Unsupported
   or non-default Core fields return a structured conversion error.
2. Rust encodes the value as the canonical operation-specific JSON document.
   The supervisor retains the native lease until OCaml either completes or
   rejects that exact document.
3. OCaml validates and copies payload bytes, then translates the semantic job
   into a private runtime job. For `SignalWorkflow`, this retains the signal
   name, ordered inputs, identity, and headers, resolves the registered handler,
   and queues it on the execution scheduler. For `QueryWorkflow`, it retains
   the query ID, type, repeated arguments, and headers, resolves the handler,
   invokes it inline, and emits a result without running ordinary fibers. No
   Rust future, pointer, protobuf value, or callback crosses into workflow code.
4. OCaml handlers emit typed semantic commands. The OCaml encoder validates and
   round-trips the complete completion before the supervisor hands owned bytes
   to Rust. Rust validates the bytes again and converts them to Core protobuf.

The private JSON document is not a Temporal Server wire format. Temporal
payloads remain opaque bytes plus encoding metadata; a JSON payload codec is an
application choice, not a requirement of the interaction protocol.

## Core ordering and activation ownership

Core documents the following ordering inside an ordinary activation:

1. workflow initialization;
2. patch notifications;
3. random-seed updates;
4. signal and update jobs;
5. other ordinary jobs;
6. local-activity resolutions;
7. queries; and
8. cache eviction.

Queries and evictions have stronger guarantees: each is delivered in its own
activation, and an eviction activation contains only `RemoveFromCache`. The
OCaml runtime must preserve the supplied list order. It may first record all
state changes and then drive runnable fibers, but it must not reorder signals,
updates, resolutions, or commands based on hash-table traversal or arrival on
an unrelated thread.

One SDK-instance supervisor remains the only owner of the Rust runtime, Core
worker, and native handle graph. It serializes poll, completion, rejection, and
shutdown messages on its owner Domain. Rust readiness is observed through the
native wait mechanism with the OCaml runtime lock released; an OCaml workflow
fiber must never block waiting for a Rust mutex. Rust threads must not invoke
OCaml closures directly. Interaction handlers run on the owning workflow
scheduler after the supervisor has delivered the validated activation.

## Signal delivery

`SignalWorkflow` contains:

- `signal_name`, the registered handler name;
- a repeated `input` payload list;
- `identity`, the sender identity; and
- a payload-valued `headers` map.

The semantic and runtime records now retain all four fields, including an empty
input list and headers. The bridge validates the signal name and header keys as
identifiers, checks every payload, and checks the sender identity as bounded,
NUL-free UTF-8 text (an empty identity is allowed). Ordered repeated inputs
are preserved; the bridge does not select an arbitrary element or silently
discard extra payloads.

The current public signal definition accepts one typed input. The native public
handler therefore requires exactly one payload and returns a typed,
non-retryable workflow error for zero or multiple payloads. The runtime still
retains the complete repeated list so a future repeated-input API can be added
without changing the transport record.

A signal handler is mutating workflow code but has no result command of its
own. Its state changes and any ordinary activity, child, or timer commands are
returned in the completion for the containing activation. The handler observes
the Core-provided ordering relative to other signal and update jobs. A missing
handler, invalid arity or payload, or handler exception follows the SDK's typed
non-retryable workflow-defect path; raw exceptions and payload values never
cross the bridge. Public `Temporal.Signal.Handler.t` currently receives the
single typed payload only. Identity and headers remain validated runtime data
until a metadata-aware public handler API is designed.

Signal input, identity, and headers are history-derived data. They may be
read by the handler, but the handler must still use only replay-safe workflow
operations. Sending a signal is not a permission to read wall time, random
process state, the network, or mutable global state.

## Query delivery and response

`QueryWorkflow` contains a `query_id`, `query_type`, repeated argument
payloads, and headers. Core guarantees that query jobs run after mutating jobs
and that queries are delivered in their own activation. A query must therefore
observe the workflow state after the preceding activation work, but it must not
mutate that state.

The native query path is implemented as a read-only, non-suspending handler
mode:

1. validate and copy the query arguments and headers at the semantic boundary;
2. invoke the output-only handler inline on the owner Domain without allowing
   workflow commands, timers, activities, child workflows, or arbitrary
   effects;
3. encode the result or typed failure; and
4. emit exactly one `QueryResult` command with the same `query_id`.

`QueryResult` is either `succeeded` with one payload or `failed` with a
structured Temporal failure. The response is part of the query activation's
completion. It is not a workflow terminal result and must not be confused
with `CompleteWorkflowExecution`.

The first public OCaml query definition has no input. That API restriction is
not a Core restriction: Core carries repeated arguments. Native integration
must preserve the repeated list in the semantic record and either add a typed
query-input definition before accepting arguments or reject a non-empty list
with a documented typed error. It must not silently decode only the first
argument.

The query mode is intentionally stricter than a normal workflow fiber. A
query cannot suspend on an activity, child, timer, or future that requires a
later activation. The current public handler is output-only, so a non-empty
argument list returns a typed non-retryable failure in `QueryResult.failed`.
The query handler's result and failure are emitted without running the normal
workflow scheduler, so no pending continuation is retained.

## Update delivery and two-phase response

`DoUpdate` contains:

- `id`, the workflow-scoped update identifier;
- `protocol_instance_id`, Core's internal protocol tracking identifier;
- `name`, the registered update handler name;
- repeated input payloads;
- headers;
- `meta`, whose pinned Core fields include the update identity and requester
  identity; and
- `run_validator`, which is false during replay because validation is not
  rerun against historical input.

An update has two observable response stages. The language SDK must emit an
`UpdateResponse` with the same `protocol_instance_id` in the completion that
contains the `DoUpdate` job:

| Stage | Response | When |
| --- | --- | --- |
| Validation | `accepted` or `rejected` | Always in the same activation as `DoUpdate`. |
| Handler | `completed` or `rejected` | In that activation or a later activation after the handler finishes. |

The current native implementation covers the non-suspending case. It decodes
one payload with the registered input codec, runs the validator when
`run_validator` is true, runs the implementation, and encodes one result. It
then emits `accepted` followed by `completed` in the same completion. A
validator or implementation failure emits only `rejected`; no workflow
continuation is retained. On replay, the validator is skipped while the
handler still follows the recorded update path. The current public API does
not expose update headers or requester identity to the callback, but the
private record validates and retains them so adding a metadata-aware API will
not change the bridge shape.

The private JSON shape is deliberately small and mirrors the semantic records
used by both decoders. For example, one activation job is:

```json
{
  "kind": "do_update",
  "id": "update-42",
  "protocol_instance_id": "protocol-42",
  "name": "set-status",
  "input": [{"metadata": {"encoding": "binary/plain"}, "data": "dXBkYXRl"}],
  "headers": {},
  "meta": {"identity": "client", "update_id": "update-42"},
  "run_validator": true
}
```

The corresponding completion commands use the same protocol ID and a closed
`response` object: `{"kind":"update_response","protocol_instance_id":"protocol-42","response":{"kind":"accepted"}}`,
followed by either `{"kind":"completed","payload":...}` or
`{"kind":"rejected","failure":...}`. Unknown fields, duplicate object
members, invalid identifiers, mismatched semantic metadata IDs, malformed
payloads, and duplicate response phases are rejected by both the OCaml and
Rust decoders. At the protobuf/Core boundary, the Rust adapter accepts Core's
default-valued (stripped) nested metadata ID, rejects a non-empty conflicting
copy, and reconstructs the canonical semantic metadata ID from `DoUpdate.id`.
The JSON Schema files under
[`docs/schemas/bridge`](../schemas/bridge/) document these shapes for tooling;
the bilateral decoders remain authoritative for byte limits and lifecycle
rules.

The validator runs before the handler only when `run_validator` is true. A
successful validator must emit `accepted` before a future suspended handler
is allowed to continue. A validator failure emits `rejected`, does not run the
handler, and must not mutate workflow state. During replay, the runtime skips
the validator and follows the recorded update path; the current immediate
implementation emits the deterministic acceptance response before its
completed result in the same activation. A handler failure emits a structured
`rejected` response; a successful handler encodes its output in `completed`.

If a future handler suspends on a supported workflow operation, the initial
acceptance must still belong to the current activation and the terminal handler
response must be retained until the future resolves. That implementation
requires an update-owned continuation keyed by `protocol_instance_id`, not a
global mutable callback map. Shutdown, eviction, duplicate delivery, and
malformed completion must release that continuation exactly once. The current
native slice deliberately does not claim this behavior: a handler that needs a
later activation is rejected by its public one-result boundary until the
continuation machinery and replay tests are implemented.

`id` and `protocol_instance_id` have different purposes and must not be
interchanged. The former is workflow-visible update identity; the latter is
the Core protocol instance used to correlate every `UpdateResponse`. `meta`
must be represented completely when it enters the semantic protocol. Until
the semantic schema includes every field needed for the pinned Core revision,
Rust must reject a non-default or otherwise unrepresentable field rather than
silently dropping it.

## Validation and failure policy

Native interaction records will use the existing closed JSON rules documented
in the [private JSON protocol](../reference/core-protocol.md):

- required fields are present, and unknown fields and duplicate object members
  are rejected;
- names, IDs, identity, and header keys are bounded, NUL-free, valid UTF-8
  text where the Core contract requires text;
- every payload is validated as an opaque payload object, including metadata,
  canonical base64, and size limits;
- repeated payload lists retain order and are checked against the supported
  arity policy; and
- the complete outgoing document is canonicalized and decoded again before it
  crosses C, with Rust repeating validation before Core conversion.

The JSON Schema files are useful documentation and tooling input, but the
bilateral decoders remain authoritative for duplicate members, byte limits,
Core enum values, and activation-dependent lifecycle rules. A Core oneof that
is not represented by the current semantic protocol is an `unsupported`
conversion error, not a best-effort empty handler call.

Expected operational failures are typed `result` values. A malformed signal,
query, or update cannot partially mutate the workflow: decode and definition
lookup happen before handler invocation, and failed validation leaves no
pending continuation. Handler exceptions are converted to non-retryable
defects at the scheduler boundary. Diagnostic logs may include a stable kind,
operation, run identifier classification, and latency, but never payload bytes,
headers, update input, query output, or raw Core diagnostics.

## Replay and determinism rules

Interaction history is part of the workflow's deterministic input. A replay
must therefore make the same handler lookup, validator decision (or replay
skip), state transition, and command sequence for the same ordered activation.
In particular:

- signal and update handlers run in Core's supplied order;
- query execution is isolated and produces only its matching query response;
- update acceptance is emitted at the validator stage even when the handler
  completes later;
- query and validator modes cannot use nondeterministic I/O, wall-clock reads,
  randomness, or process-global mutation; and
- opaque payload bytes and metadata are copied at the OCaml boundary so a
  later Rust or C lifetime cannot change replay-visible data.

The normal workflow clock is the activation timestamp supplied by Core. Query
and update metadata does not provide an alternate clock or authorization to
read host state.

## Implementation sequence and evidence

Native interactions should be implemented in small bilateral slices, with no
single side accepting a new variant early:

1. Add typed Rust/OCaml semantic records and closed JSON shapes for
   `SignalWorkflow` and its activation mapping. This slice is now implemented,
   including malformed identity, payload, ordering, lossless-round-trip tests,
   public handler registration, scheduler delivery, exact-one arity validation,
   and fail-closed missing-handler tests.
2. Add `QueryWorkflow` and `QueryResult`, including the no-suspension query
   mode and query-only completion tests. **Implemented in the current
   semantic/runtime slice:** bilateral Core conversion, exact query-ID
   preservation (including Core's `legacy_query` path), output-only handler
   dispatch, and rejected extra arguments. Live Server coverage remains open.
3. **Current milestone:** add `DoUpdate` and `UpdateResponse` semantic records,
   strict JSON/schema validation, pinned-Core conversion, immediate
   non-suspending public handler dispatch, replay validator skipping, and
   response-phase tests. Suspended handler completion, shutdown, and eviction
   cleanup remain open.
4. Add update-owned continuations, later-activation completion, and lifecycle
   tests before advertising full update support.
5. Add Core conversion fixtures in `rust/core-bridge/tests/`, OCaml runtime
   tests under `test/`, and bilateral JSON round-trip tests for every supported
   variant. Run the representative local Makefile gates; queued GitHub
   Actions checks remain unexecuted evidence until the repository quota clears.
6. Expand the Docker Compose acceptance scenario with Temporal Server and
   PostgreSQL to issue a query and wait for an update through the two OCaml
   binaries. The typed signal/condition path is already live-verified by the
   [PR #266 Actions run](https://github.com/ocaml-temporal/actions/runs/29311239247);
   record the query and update results separately from synthetic and
   bridge-only evidence.

Until the later update continuation stages have passed, the overall feature
status remains experimental: native `SignalWorkflow` transport and its typed
signal/condition success path, output-only `QueryWorkflow` delivery, and
immediate non-suspending update dispatch are implemented and focused-tested.
Live query/update delivery, suspended updates, and broader interaction
acceptance remain pending.
`Temporal.Interaction` remains the public local-testing path for all three
interaction kinds.
