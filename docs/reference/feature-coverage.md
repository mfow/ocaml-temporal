# Feature coverage and implementation status

This reference is the short, evidence-based answer to “what can this
repository do today?”. It describes the source and tests at the current
`master` baseline. It is not a promise that every implemented type or wire
record is ready for production, and it does not promote a planned design into
a supported feature.

## How to read the status

The project has four deliberately separate evidence levels:

- **Verified live** means a test used the real Temporal Server and PostgreSQL
  Compose stack. At this baseline, it includes the two-binary success path:
  public client start/exact-run wait, public worker workflow/activity dispatch,
  activity fan-out, a timer-then-activity workflow, one parent awaiting a
  timer-owning child workflow, and one activity that succeeds after a server
  retry. The live retry assertion passed in CI run
  [`29187733405`](https://github.com/mfow/ocaml-temporal/actions/runs/29187733405)
  for commit `b895d3c`.
- **Unit-tested or mock-only** means deterministic OCaml, Rust, C, or
  in-memory fake tests passed without a workflow execution hosted by Temporal
  Server. This is strong evidence for local semantics and ownership, but not
  proof of server compatibility.
- **Bridge implemented, partly live-tested** means the OCaml/Rust protocol and
  Rust/Temporal Core conversion are covered by focused tests, and the first
  happy-path records have crossed the live worker/client boundary. It does not
  promote unexercised variants to live support.
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
starts `smoke-worker`, runs the independent `smoke-driver`, and cleans up the
Compose project. The lifecycle assertions are in
[`test/integration/test_core_lifecycle.ml`](../../test/integration/test_core_lifecycle.ml);
the live workflow assertions are in
[`test/integration/temporal/driver/smoke_driver.ml`](../../test/integration/temporal/driver/smoke_driver.ml).

| Capability | What the live test proves | What it does not prove |
| --- | --- | --- |
| PostgreSQL-backed Temporal Server startup | The pinned SQL schemas and Temporal frontend become healthy in the documented Compose topology. | It does not prove production topology or upgrade compatibility. |
| OCaml/C/Rust bridge and Core graph construction | The OCaml supervisor creates the Rust/Core graph, connects a client, and creates a worker against the real endpoint. | It does not prove every protocol error or lifecycle transition under load. |
| Lifecycle rejection and idempotence | Invalid repeated client/worker transitions are rejected, repeated worker/client shutdown is safe, and reverse-order supervisor shutdown completes. | It does not prove graceful cancellation of an outstanding live execution. |
| Public client start and exact-run wait | A separate OCaml driver starts the four top-level workflows through `Temporal.Client`, retains all returned handles, and waits for each exact workflow/run pair. | CI run [`29187733405`](https://github.com/mfow/ocaml-temporal/actions/runs/29187733405) verifies the four successful terminal results; non-success terminal outcomes and continued-as-new are not covered live. |
| Public worker workflow and activity dispatch | A separate OCaml worker registers the workflows and activities, including the retry workflow/activity, through real Core polling/completion. | CI run [`29187733405`](https://github.com/mfow/ocaml-temporal/actions/runs/29187733405) verifies the successful dispatch and one retry; activity timeout, heartbeat, asynchronous completion, non-retryable failure, and child failure/cancellation are not covered live. |
| Fan-out, durable timer, and child success paths | `smoke.fan_out` returns `SMOKE:LEFT|SMOKE:RIGHT` after scheduling two mock activities before its first wait; `smoke.timer_then_activity` returns `SMOKE:TIMER` after its durable timer and activity; and `smoke.parent_awaits_child` returns `SMOKE:CHILD` after its child completes a durable timer. | It does not independently inspect history events, prove every concurrency ordering, or cover cancellation semantics. |
| Activity retry delivery | `smoke.activity_retry` schedules `smoke.retry_once` with an explicit two-attempt policy. The worker intentionally fails the first attempt and the driver requires the exact `SMOKE:ATTEMPT:2` result from the second task. | **Verified (one live success-after-retry path).** CI run [`29187733405`](https://github.com/mfow/ocaml-temporal/actions/runs/29187733405) proves this short path only; it does not prove timeout-triggered retries, non-retryable error-type matching, long backoff, or retry across worker restart. |

The driver and worker remain guarded by `TEMPORAL_TWO_BINARY_LIVE=1`; the
dedicated Compose services set it so an ordinary local invocation cannot be
mistaken for live acceptance. See [the local stack reference](local-temporal-stack.md)
and [the two-binary acceptance design](two-ocaml-binary-e2e-acceptance.md) for
the topology and its deliberately narrow current boundary.

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
| Durable timers in the runtime model | [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml), [`test/runtime/test_native_execution.ml`](../../test/runtime/test_native_execution.ml) | Timer commands, zero-duration behavior, exact millisecond conversion, firing, and cancellation are tested synthetically. The first timer success path is also exercised live; detailed timer variants remain synthetic-only. |
| Activity command authoring | [`test/unit/test_activity_retry_policy.ml`](../../test/unit/test_activity_retry_policy.ml), [`test/runtime/test_activity_retry_policy.ml`](../../test/runtime/test_activity_retry_policy.ml), [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml), [`test/runtime/test_native_execution.ml`](../../test/runtime/test_native_execution.ml), [`test/bridge/test_ocaml_workflow_protocol.ml`](../../test/bridge/test_ocaml_workflow_protocol.ml), [`rust/core-bridge/tests/workflow_retry_policy.rs`](../../rust/core-bridge/tests/workflow_retry_policy.rs) | IDs, queues, timeout fields, cancellation policy, eager-execution flag, deterministic defaults, payload copying, optional retry policies, exact IEEE-754 coefficient bits, bilateral validation, and invalid-option rejection are covered before a completion is emitted. The live acceptance row now proves one real server retry; policy encoding and malformed-input evidence remains synthetic. |
| Child-workflow scheduling and resolution state | [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml), [`test/runtime/test_native_worker_execution.ml`](../../test/runtime/test_native_worker_execution.ml) | Synthetic parent executions cover start acknowledgement, terminal resolution, start failure, final-before-start, duplicate sequences, and lease retirement. The live gate separately covers one successful parent calling `Child_workflow.execute`; those failure and recovery edges remain synthetic-only. |
| Workflow and activity dispatch | [`test/unit/test_client_worker.ml`](../../test/unit/test_client_worker.ml), [`test/runtime/test_native_worker_execution.ml`](../../test/runtime/test_native_worker_execution.ml), [`test/runtime/test_native_activity_execution.ml`](../../test/runtime/test_native_activity_execution.ml) | Mock tasks and fake supervisors exercise codec decode, typed implementation calls, completion encoding, unknown registrations, ordinary task failures, and continuation after a task-level error. The public happy path is additionally covered by the live gate above. |
| Completion retry and shutdown drainage | [`test/runtime/test_native_worker_lifecycle.ml`](../../test/runtime/test_native_worker_lifecycle.ml), [`test/runtime/test_native_activity_lifecycle.ml`](../../test/runtime/test_native_activity_lifecycle.ml), [`test/sdk_supervisor/test_sdk_supervisor.ml`](../../test/sdk_supervisor/test_sdk_supervisor.ml) | A rejected completion is retained and retried without rerunning user code; shutdown waits for drained leases and remains retryable when transport acknowledgement is unavailable. |
| Public mock client | [`test/unit/test_client_worker.ml`](../../test/unit/test_client_worker.ml) | `Temporal.Client` start, exact-run handle identity, typed output decoding, validation, and idempotent shutdown are tested against `mock://`; this mock echoes input and does not execute a real workflow. |
| Mailbox and one-owner supervisor invariants | [`test/mailbox_processor/test_mailbox_processor.ml`](../../test/mailbox_processor/test_mailbox_processor.ml), [`test/sdk_supervisor/test_sdk_supervisor.ml`](../../test/sdk_supervisor/test_sdk_supervisor.ml) | Bounded admission, FIFO calls, reply settlement, close/join, concurrent producers, shutdown races, and one owner Domain are tested. The mailbox is a private implementation unit, not a public actor API. |
| Observability | [`test/observability/test_logging.ml`](../../test/observability/test_logging.ml), [`test/observability/test_tag_normalization.ml`](../../test/observability/test_tag_normalization.ml) | Structured `logs` events, levels, sources, tags, and privacy-safe diagnostics are tested without logging payload bytes or bridge JSON. |

## Bridge implemented, partly live-tested

The following pieces are implemented below the public API and have bilateral
or focused evidence. The two-binary Compose gate exercises the first complete
success path; records and variants outside that path remain **native support
under test**, not live SDK features.

| Native capability | Implemented boundary and evidence | Live boundary |
| --- | --- | --- |
| Strict OCaml/Rust JSON control protocol | Closed envelopes, duplicate/unknown-field rejection, bounded numbers and text, UTF-8/base64 handling, canonical re-encoding, and privacy-safe errors are specified in [Core protocol](core-protocol.md), with schemas under [`docs/schemas/bridge/`](../schemas/bridge/) and tests in [`test/bridge/`](../../test/bridge/) plus [`rust/core-bridge/tests/`](../../rust/core-bridge/tests/). | A complete happy-path workflow activation and completion has traversed the live binaries. Rejection and unusual record variants remain focused-test evidence. |
| Rust/Temporal Core protobuf conversion | Rust owns Core protobuf and gRPC conversion for workflow activations/completions, remote activities, child resolutions, client starts, and exact-run waits. The focused conversion tests are in `rust/core-bridge/tests/`. | Live success results, activity completions, and one child start/resolution pair are asserted; non-success variants remain unexercised live. |
| Native client start and exact-run wait | The private [client protocol](client-protocol.md), Rust client bridge tests, supervisor operations, request correlation, bounded waits, terminal outcome mapping, and typed public handles are implemented. | The real client starts all four top-level workflows and the live CI run [`29187733405`](https://github.com/mfow/ocaml-temporal/actions/runs/29187733405) verifies their successful terminal results. Non-success outcomes and continued-as-new remain untested live. |
| Native workflow poll and completion | The workflow protocol, [native execution translation](native-execution-translation.md), private worker registry, readiness waits, command validation, activation ordering, timers, cancellation, eviction, and completion retry are implemented and focused-tested. | The live worker runs registered OCaml workflows through successful completion. Cancellation, eviction, and recovery behavior remain focused-test evidence. |
| Native remote-activity poll and completion | The [activity protocol](activity-protocol.md), private activity adapter, copied opaque-token lease, cancellation completion, strict validation, and retryable drain are implemented and focused-tested. | The live worker completes the mock activities used by the fan-out and timer scenarios, and CI run [`29187733405`](https://github.com/mfow/ocaml-temporal/actions/runs/29187733405) verifies the retry activity's one-failure/second-attempt result. Heartbeats, asynchronous completion, timeout, and non-retryable failure remain deferred or synthetic-only. |
| Child-workflow two-stage lifecycle | Start commands, start acknowledgements, terminal child resolution, sequence correlation, failure causes, and lease retirement are represented in the semantic protocol and tested in both languages. | A real parent calls `Child_workflow.execute`, receives a start acknowledgement and terminal result through the worker, and returns the child's timer-derived value. Child failure, cancellation, retry, replay, and recovery remain untested live. |
| Native readiness and lifecycle ownership | Rust readiness lanes, bounded waits that release the OCaml runtime lock, one owner-Domain supervisor, C/Rust response ownership, cleanup, and ABI checks are documented in [Core bridge](core-bridge.md) and covered by bridge, Rust, and supervisor tests. | The live worker loop runs under the initial task load and shuts down after completion. Live shutdown with outstanding work remains untested. |

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
| Live failure, cancellation, child failure/cancellation, restart, replay, and cache-eviction scenarios | **Deferred as acceptance scenarios.** The two-binary gate now requests one activity that fails once and should succeed on a server-managed retry; broader failure and recovery cases remain planned. | [Live acceptance coverage](live-acceptance-coverage.md) |
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
make test-temporal-integration  # real PostgreSQL/Temporal + two OCaml binaries
```

`make test-temporal-integration` is intentionally the only command in this
list with a real Temporal Server. A green `make verify` alone is not live
workflow evidence; the two-binary gate is the supported success-path
acceptance evidence, and it must not be generalized to untested terminal or
recovery scenarios.
