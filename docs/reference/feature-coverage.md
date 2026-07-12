# Feature coverage and implementation status

This reference is the short, evidence-based answer to “what can this
repository do today?”. It describes the source and tests at the current
`master` baseline. It is not a promise that every implemented type or wire
record is ready for production, and it does not promote a planned design into
a supported feature.

## How to read the status

The project has four deliberately separate evidence levels:

- **Verified live** means a test used the real Temporal Server and PostgreSQL
  Compose stack. At this baseline, live evidence is limited to infrastructure
  and Core client/worker lifecycle; no workflow result has crossed the live
  stack yet.
- **Unit-tested or mock-only** means deterministic OCaml, Rust, C, or
  in-memory fake tests passed without a workflow execution hosted by Temporal
  Server. This is strong evidence for local semantics and ownership, but not
  proof of server compatibility.
- **Bridge implemented, live path pending** means the OCaml/Rust protocol and
  the Rust/Temporal Core conversion are implemented and covered by focused
  tests, while the complete live worker/client path has not yet exercised that
  record against a server.
- **Deferred or unsupported** means the public API or runtime intentionally
  does not provide the capability yet. These items are roadmap work, not
  hidden promises.

The detailed scenario matrix in [Live acceptance coverage](live-acceptance-coverage.md)
records the same boundary at a finer grain. When this summary and a plan
document disagree, the source and tests win; [Verified progress](../progress.md)
records the evidence for completed milestones.

## Verified live functionality

The live gate is `make test-temporal-integration`. It starts the fixture under
[`test/integration/temporal/`](../../test/integration/temporal/), waits for
PostgreSQL and Temporal frontend health, runs the OCaml lifecycle executable,
and cleans up the Compose project. The assertions are in
[`test/integration/test_core_lifecycle.ml`](../../test/integration/test_core_lifecycle.ml).

| Capability | What the live test proves | What it does not prove |
| --- | --- | --- |
| PostgreSQL-backed Temporal Server startup | The pinned SQL schemas and Temporal frontend become healthy in the documented Compose topology. | It does not execute an OCaml workflow or activity. |
| OCaml/C/Rust bridge and Core graph construction | The OCaml supervisor can validate native settings, create the Rust/Core graph, connect a client, and start a workflow-only worker against the real endpoint. | It does not prove that a workflow activation or activity task can be completed through the server. |
| Lifecycle rejection and idempotence | Invalid repeated client/worker transitions are rejected, repeated worker/client shutdown is safe, and reverse-order supervisor shutdown completes. | It does not prove graceful cancellation of an outstanding live execution. |

There is intentionally no row here for “workflow returns a result”. The
two-OCaml-binary driver and worker are guarded by
`TEMPORAL_TWO_BINARY_LIVE=1`, and the current Compose target does not enable
that guard. Until a live job starts a workflow, waits for its exact run, and
checks the decoded result, the repository must not describe that path as
end-to-end acceptance. See [the local stack reference](local-temporal-stack.md)
and [the two-binary acceptance design](two-ocaml-binary-e2e-acceptance.md).

## Unit-tested or mock-only functionality

These capabilities have deterministic evidence, but their current tests use
the in-memory `mock://` backend, fake supervisor operations, synthetic
activations, or direct Rust/C fixtures. The tests are useful for authoring and
ownership correctness; they are not live Temporal compatibility tests.

| Capability | Evidence | Current boundary |
| --- | --- | --- |
| Typed workflow and activity definitions | [`test/unit/test_definition.ml`](../../test/unit/test_definition.ml), [`test/unit/test_workflow_authoring.ml`](../../test/unit/test_workflow_authoring.ml) | Ordinary OCaml functions return typed `result` values. Local and remote definitions keep codecs paired with names; remote-only definitions cannot be registered as executable worker code. |
| Payload codecs and typed failures | [`test/unit/test_codec.ml`](../../test/unit/test_codec.ml), [`test/unit/test_error.ml`](../../test/unit/test_error.ml) | String (`json/plain`), bytes (`binary/plain`), unit (`binary/null`), options, custom codecs, metadata validation, and error details are tested without a server. JSON is a payload option, not a Temporal requirement. |
| Direct-style workflow suspension | [`test/runtime/test_scheduler.ml`](../../test/runtime/test_scheduler.ml), [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml) | OCaml 5 effects suspend one workflow fiber on a future and resume it from a later synthetic activation. No public effect constructor or continuation is exposed. |
| Futures and concurrent composition | [`test/unit/test_workflow_authoring.ml`](../../test/unit/test_workflow_authoring.ml), [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml) | `await`, `map`, `map_error`, `both`, `all`, `race`, `first`, `peek`, and readiness are deterministic and workflow-owned. Losing operations are not implicitly cancelled. |
| Durable timers in the runtime model | [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml), [`test/runtime/test_native_execution.ml`](../../test/runtime/test_native_execution.ml) | Timer commands, zero-duration behavior, exact millisecond conversion, firing, and cancellation are tested synthetically. No live timer has been observed through Temporal Server. |
| Activity command authoring | [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml), [`test/runtime/test_native_execution.ml`](../../test/runtime/test_native_execution.ml) | IDs, queues, timeout fields, cancellation policy, eager-execution flag, deterministic defaults, payload copying, and invalid-option rejection are covered before a completion is emitted. |
| Child-workflow scheduling and resolution state | [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml), [`test/runtime/test_native_worker_execution.ml`](../../test/runtime/test_native_worker_execution.ml) | Synthetic parent executions cover start acknowledgement, terminal resolution, start failure, final-before-start, duplicate sequences, and lease retirement. No parent/child result has crossed the live stack. |
| Workflow and activity dispatch | [`test/unit/test_client_worker.ml`](../../test/unit/test_client_worker.ml), [`test/runtime/test_native_worker_execution.ml`](../../test/runtime/test_native_worker_execution.ml), [`test/runtime/test_native_activity_execution.ml`](../../test/runtime/test_native_activity_execution.ml) | Mock tasks and fake supervisors exercise codec decode, typed implementation calls, completion encoding, unknown registrations, ordinary task failures, and continuation after a task-level error. |
| Completion retry and shutdown drainage | [`test/runtime/test_native_worker_lifecycle.ml`](../../test/runtime/test_native_worker_lifecycle.ml), [`test/runtime/test_native_activity_lifecycle.ml`](../../test/runtime/test_native_activity_lifecycle.ml), [`test/sdk_supervisor/test_sdk_supervisor.ml`](../../test/sdk_supervisor/test_sdk_supervisor.ml) | A rejected completion is retained and retried without rerunning user code; shutdown waits for drained leases and remains retryable when transport acknowledgement is unavailable. |
| Public mock client | [`test/unit/test_client_worker.ml`](../../test/unit/test_client_worker.ml) | `Temporal.Client` start, exact-run handle identity, typed output decoding, validation, and idempotent shutdown are tested against `mock://`; this mock echoes input and does not execute a real workflow. |
| Mailbox and one-owner supervisor invariants | [`test/mailbox_processor/test_mailbox_processor.ml`](../../test/mailbox_processor/test_mailbox_processor.ml), [`test/sdk_supervisor/test_sdk_supervisor.ml`](../../test/sdk_supervisor/test_sdk_supervisor.ml) | Bounded admission, FIFO calls, reply settlement, close/join, concurrent producers, shutdown races, and one owner Domain are tested. The mailbox is a private implementation unit, not a public actor API. |
| Observability | [`test/observability/test_logging.ml`](../../test/observability/test_logging.ml), [`test/observability/test_tag_normalization.ml`](../../test/observability/test_tag_normalization.ml) | Structured `logs` events, levels, sources, tags, and privacy-safe diagnostics are tested without logging payload bytes or bridge JSON. |

## Bridge implemented, live path pending

The following pieces are implemented below the public API and have bilateral
or focused evidence. They should be described as **native support under test**,
not as live SDK features, until the two-binary Compose gate exercises them.

| Native capability | Implemented boundary and evidence | Missing live proof |
| --- | --- | --- |
| Strict OCaml/Rust JSON control protocol | Closed envelopes, duplicate/unknown-field rejection, bounded numbers and text, UTF-8/base64 handling, canonical re-encoding, and privacy-safe errors are specified in [Core protocol](core-protocol.md), with schemas under [`docs/schemas/bridge/`](../schemas/bridge/) and tests in [`test/bridge/`](../../test/bridge/) plus [`rust/core-bridge/tests/`](../../rust/core-bridge/tests/). | A complete workflow activation and completion has not yet traversed the live worker/client binaries. |
| Rust/Temporal Core protobuf conversion | Rust owns Core protobuf and gRPC conversion for workflow activations/completions, remote activities, child resolutions, client starts, and exact-run waits. The focused conversion tests are in `rust/core-bridge/tests/`. | A successful live workflow result, activity completion, or child result has not been asserted. |
| Native client start and exact-run wait | The private [client protocol](client-protocol.md), Rust client bridge tests, supervisor operations, request correlation, bounded waits, terminal outcome mapping, and typed public handles are implemented. | The real client has not started a workflow and waited for its result in Compose. |
| Native workflow poll and completion | The workflow protocol, [native execution translation](native-execution-translation.md), private worker registry, readiness waits, command validation, activation ordering, timers, cancellation, eviction, and completion retry are implemented and focused-tested. | No live workflow task has been polled, run by an OCaml definition, and completed through Temporal Server. |
| Native remote-activity poll and completion | The [activity protocol](activity-protocol.md), private activity adapter, copied opaque-token lease, cancellation completion, strict validation, and retryable drain are implemented and focused-tested. | No live activity task has been polled and completed by the OCaml worker. Heartbeats and asynchronous completion remain deferred. |
| Child-workflow two-stage lifecycle | Start commands, start acknowledgements, terminal child resolution, sequence correlation, failure causes, and lease retirement are represented in the semantic protocol and tested in both languages. | No parent workflow has awaited a child result from a real Temporal Server. |
| Native readiness and lifecycle ownership | Rust readiness lanes, bounded waits that release the OCaml runtime lock, one owner-Domain supervisor, C/Rust response ownership, cleanup, and ABI checks are documented in [Core bridge](core-bridge.md) and covered by bridge, Rust, and supervisor tests. | The live worker loop has not been observed under real task load or live shutdown with outstanding work. |

For ownership and cleanup guarantees, read [runtime invariants](runtime-invariants.md)
and the [Core bridge reference](core-bridge.md). For the public authoring
surface, read [Writing workflows in OCaml](../guides/workflows.md). Those
documents explain how the native pieces are intended to become one SDK while
preserving the distinction between a tested bridge and a live feature.

## Deferred or unsupported Temporal SDK features

These capabilities are not currently presented as supported by the public
OCaml SDK. They are listed explicitly so a type or a protocol enum cannot be
mistaken for an implemented feature.

| Feature | Status at this baseline | Planned reference |
| --- | --- | --- |
| Live two-binary workflow-result acceptance | **Deferred.** The driver/worker scaffold exists, but the Compose job is guarded and does not assert a workflow result. | [Live acceptance coverage](live-acceptance-coverage.md), [roadmap Phase 2](../implementation-roadmap.md#delivery-order) |
| Live fan-out, timer, activity, failure, cancellation, child, restart, replay, and cache-eviction scenarios | **Deferred as acceptance scenarios.** Synthetic coverage exists for several of these semantics; the real-server assertions remain planned. | [Live acceptance coverage](live-acceptance-coverage.md) |
| Signals, queries, updates, validators, conditions, and handler policies | **Not implemented in the public API.** | [Roadmap Phase 5](../implementation-roadmap.md#delivery-order) |
| Continue-as-new, patches, side effects, versioning, external workflow operations, memo, search attributes, priority, and fairness controls | **Not implemented in the public workflow API.** Some related Core fields are retained by private protocol types, but they are not exposed as executable OCaml operations. | [Roadmap Phase 6](../implementation-roadmap.md#delivery-order) |
| Local activities, heartbeats, asynchronous activity completion, and interceptors | **Not implemented in the public activity API.** The native activity protocol retains some context for future additive work; the adapter does not expose these controls. | [Native activity execution](native-activity-execution.md), [roadmap Phase 7](../implementation-roadmap.md#delivery-order) |
| Structured-concurrency scopes and implicit cancellation of losing futures | **Not implemented.** The current `both`, `all`, `race`, and `first` combinators are deterministic value combinators; they do not provide cancellation scopes. | [Roadmap Phase 4](../implementation-roadmap.md#delivery-order), [workflow guide](../guides/workflows.md#5-combine-futures) |
| Schedules, visibility, reset/terminate/cancel client commands, update handles, Nexus, and test-server controls | **Not implemented in the public client API.** The client currently starts and exact-run waits, then exposes terminal outcomes as values. | [Roadmap Phase 8](../implementation-roadmap.md#delivery-order) |
| Stable release, benchmarked performance, full replay corpus, and publication hardening | **Deferred.** The package is experimental and pre-`0.1.0`; compatibility and feature-parity work remain open. | [Roadmap Phases 9–10](../implementation-roadmap.md#delivery-order), [architecture specification](../superpowers/specs/2026-07-11-ocaml-temporal-sdk-design.md) |

## Evidence commands

The supported Makefile commands map to the evidence levels above:

```sh
make test-unit                  # public definitions, codecs, mock client/worker
make test-runtime               # scheduler, activations, futures, native adapters
make test-bridge                # bilateral JSON, ABI, and ownership fixtures
make verify                     # broad local build, lint, Rust, and bridge gates
make test-temporal-integration  # real PostgreSQL/Temporal lifecycle only
```

`make test-temporal-integration` is intentionally the only command in this
list with a real Temporal Server. A green `make verify` or a green lifecycle
smoke must not be reported as end-to-end workflow acceptance until a dedicated
two-binary live assertion exists and passes.
