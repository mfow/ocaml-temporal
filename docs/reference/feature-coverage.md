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
  timer-owning child workflow, one activity that succeeds after a server
  retry, and one typed non-retryable workflow failure. The historical CI run
  [`29191260073`](https://github.com/mfow/ocaml-temporal/actions/runs/29191260073)
  for merge commit `a4eaccc8` verifies those five baseline executions. Exact-
  run cancellation is implemented and covered by local acceptance checks, but
  it is not live-verified yet: the attempted [Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29193818312)
  was cancelled before producing a green result.
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
| Lifecycle rejection and idempotence | Invalid repeated client/worker transitions are rejected, repeated worker/client shutdown is safe, and reverse-order supervisor shutdown completes. | The current **local** acceptance target additionally checks the driver's `client_shutdown` phase and the worker's graceful-shutdown marker after outstanding work is cancelled; those checks are not live-verified yet. |
| Public client start, exact-run wait, and cancellation | A separate OCaml driver starts seven top-level workflows through `Temporal.Client`, retains all returned handles, waits for a marker activity before acknowledging cancellation for the long-running handle, and waits for each exact workflow/run pair. | **Historical:** five baseline executions were verified in CI run [`29191260073`](https://github.com/mfow/ocaml-temporal/actions/runs/29191260073) for merge commit `a4eaccc8`. The current seven-run heartbeat/cancellation assertions are implemented and locally covered, but the attempted [Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29193818312) was cancelled, so they are not live-verified yet. Continued-as-new remains untested live. |
| Public worker workflow and activity dispatch | A separate OCaml worker registers the workflows and activities, including ordinary retry, context-aware heartbeat retry, typed-failure, and cancellation-marker definitions, through real Core polling/completion. | CI run [`29191260073`](https://github.com/mfow/ocaml-temporal/actions/runs/29191260073) verifies the successful dispatch, one retry, and typed non-retryable workflow failure; the heartbeat scenario is implemented and locally contract-checked but not yet covered by live evidence. Activity timeout, asynchronous completion, and child failure/cancellation remain deferred. |
| Fan-out, durable timer, and child success paths | `smoke.fan_out` returns `SMOKE:LEFT|SMOKE:RIGHT` after scheduling two mock activities before its first wait; `smoke.timer_then_activity` returns `SMOKE:TIMER` after its durable timer and activity; and `smoke.parent_awaits_child` returns `SMOKE:CHILD` after its child completes a durable timer. | It does not independently inspect history events, prove every concurrency ordering, or cover cancellation semantics. |
| Activity retry delivery | `smoke.activity_retry` schedules `smoke.retry_once` with an explicit two-attempt policy. The worker intentionally fails the first attempt and the driver requires the exact `SMOKE:ATTEMPT:2` result from the second task. `smoke.activity_heartbeat_retry` additionally sends a typed heartbeat before its first retry and requires the returned detail on attempt 2. | **Verified (one live success-after-retry path).** CI run [`29191260073`](https://github.com/mfow/ocaml-temporal/actions/runs/29191260073) proves the ordinary short path only; the heartbeat-detail retry is implemented and locally contract-checked but awaits a green live Compose run. Neither path proves timeout-triggered retries, non-retryable error-type matching, long backoff, or retry across worker restart. |

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
| Payload codecs and typed failures | [`test/unit/test_codec.ml`](../../test/unit/test_codec.ml), [`test/unit/test_error.ml`](../../test/unit/test_error.ml), [`test/runtime/test_native_execution.ml`](../../test/runtime/test_native_execution.ml) | String (`json/plain`), bytes (`binary/plain`), unit (`binary/null`), options, custom codecs, duplicate-metadata rejection, and error details are tested without a server. JSON is a payload option, not a Temporal requirement. |
| Direct-style workflow suspension | [`test/runtime/test_scheduler.ml`](../../test/runtime/test_scheduler.ml), [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml) | OCaml 5 effects suspend one workflow fiber on a future and resume it from a later synthetic activation. No public effect constructor or continuation is exposed. |
| Futures and concurrent composition | [`test/unit/test_workflow_authoring.ml`](../../test/unit/test_workflow_authoring.ml), [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml) | `await`, `map`, `map_error`, `both`, `all`, `race`, `first`, `peek`, and readiness are deterministic and workflow-owned. Losing operations are not implicitly cancelled. |
| Durable timers in the runtime model | [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml), [`test/runtime/test_native_execution.ml`](../../test/runtime/test_native_execution.ml) | Timer commands, zero-duration behavior, exact millisecond conversion, firing, and cancellation are tested synthetically. The first timer success path is also exercised live; detailed timer variants remain synthetic-only. |
| Activity command authoring | [`test/unit/test_activity_retry_policy.ml`](../../test/unit/test_activity_retry_policy.ml), [`test/runtime/test_activity_retry_policy.ml`](../../test/runtime/test_activity_retry_policy.ml), [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml), [`test/runtime/test_native_execution.ml`](../../test/runtime/test_native_execution.ml), [`test/bridge/test_ocaml_workflow_protocol.ml`](../../test/bridge/test_ocaml_workflow_protocol.ml), [`rust/core-bridge/tests/workflow_retry_policy.rs`](../../rust/core-bridge/tests/workflow_retry_policy.rs) | IDs, queues, timeout fields, cancellation policy, eager-execution flag, deterministic defaults, payload copying, optional retry policies, exact IEEE-754 coefficient bits, bilateral validation, and invalid-option rejection are covered before a completion is emitted. The live acceptance row now proves one real server retry; policy encoding and malformed-input evidence remains synthetic. |
| Child-workflow scheduling and resolution state | [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml), [`test/runtime/test_native_worker_execution.ml`](../../test/runtime/test_native_worker_execution.ml), [`test/bridge/test_ocaml_workflow_protocol.ml`](../../test/bridge/test_ocaml_workflow_protocol.ml), [`rust/core-bridge/tests/workflow_protocol.rs`](../../rust/core-bridge/tests/workflow_protocol.rs) | Synthetic parent executions cover start acknowledgement, start rejection (including already-exists and unspecified causes), terminal resolution and failure, retryable child failure state, final-before-start, duplicate start/terminal events, unknown sequences, all four cancellation policies, and lease retirement. The live gate separately covers one successful parent calling `Child_workflow.execute`; these failure and recovery edges remain synthetic-only. |
| Workflow and activity dispatch | [`test/unit/test_client_worker.ml`](../../test/unit/test_client_worker.ml), [`test/runtime/test_native_worker_execution.ml`](../../test/runtime/test_native_worker_execution.ml), [`test/runtime/test_native_activity_execution.ml`](../../test/runtime/test_native_activity_execution.ml) | Mock tasks and fake supervisors exercise codec decode, typed implementation calls, completion encoding, unknown registrations, ordinary task failures, and continuation after a task-level error. The public happy path is additionally covered by the live gate above. |
| Completion retry and shutdown drainage | [`test/runtime/test_native_worker_lifecycle.ml`](../../test/runtime/test_native_worker_lifecycle.ml), [`test/runtime/test_native_activity_lifecycle.ml`](../../test/runtime/test_native_activity_lifecycle.ml), [`test/sdk_supervisor/test_sdk_supervisor.ml`](../../test/sdk_supervisor/test_sdk_supervisor.ml) | A rejected completion is retained and retried without rerunning user code; shutdown waits for drained leases and remains retryable when transport acknowledgement is unavailable. |
| Public mock client | [`test/unit/test_client_worker.ml`](../../test/unit/test_client_worker.ml) | `Temporal.Client` start, exact-run handle identity, typed output decoding, exact-run cancellation, validation, and idempotent shutdown are tested against `mock://`; this mock echoes input and does not execute a real workflow. |
| Mailbox and one-owner supervisor invariants | [`test/mailbox_processor/test_mailbox_processor.ml`](../../test/mailbox_processor/test_mailbox_processor.ml), [`test/sdk_supervisor/test_sdk_supervisor.ml`](../../test/sdk_supervisor/test_sdk_supervisor.ml) | Bounded admission, FIFO calls, concurrent producer ordering, close/join idempotence, atomic terminal admission, abandoned terminal replies, handler-failure propagation, blocked-producer wakeups, shutdown races, and one owner Domain are tested. The mailbox is a private implementation unit, not a public actor API. |
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
| Native client cancellation-input validation | The Rust/C ABI rejects malformed cancellation JSON with `STATUS_PROTOCOL` before consulting an unconnected runtime; [`rust/core-bridge/tests/client_bridge.rs`](../../rust/core-bridge/tests/client_bridge.rs) also frees the returned diagnostic and runtime on this path. | Focused native ABI evidence only; no live Temporal acceptance scenario exercises malformed client input. |
| Native client start, exact-run wait, and cancellation | The private [client protocol](client-protocol.md), Rust client bridge tests, supervisor operations, request correlation, bounded waits, positive cancellation acknowledgement, terminal outcome mapping, and typed public handles are implemented. Cancellation uses the exact retained run identity and a bounded native RPC. | **Historical:** five baseline executions were verified in CI run [`29191260073`](https://github.com/mfow/ocaml-temporal/actions/runs/29191260073) for merge commit `a4eaccc8`. The current seven-run exact-run cancellation/heartbeat scenario is implemented and locally covered, but the attempted [Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29193818312) was cancelled, so it is not live-verified yet. Continued-as-new remains untested live. |
| Native workflow poll and completion | The workflow protocol, [native execution translation](native-execution-translation.md), private worker registry, readiness waits, command validation, activation ordering, timers, cancellation, eviction, and completion retry are implemented and focused-tested. | **Historical:** five baseline executions were verified in CI run [`29191260073`](https://github.com/mfow/ocaml-temporal/actions/runs/29191260073). The current seven-run path adds marker-guarded long-running timer cancellation, heartbeat retry, and graceful shutdown; it is locally covered but not live-verified because the attempted [Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29193818312) was cancelled. Eviction and recovery remain focused-test evidence. |
| Native remote-activity poll and completion | The [activity protocol](activity-protocol.md), private activity adapter, copied opaque-token lease, cancellation completion, strict validation, retryable drain, and context-aware heartbeat submission are implemented and focused-tested. `Temporal.Activity.define_with_context` exposes prior heartbeat details, heartbeat timeout, and typed progress calls while keeping the context lifetime private. | The live worker completes the mock activities used by the fan-out and timer scenarios, and CI run [`29191260073`](https://github.com/ocaml-temporal/actions/runs/29191260073) verifies the retry activity's one-failure/second-attempt result. The new heartbeat workflow is implemented and locally contract-checked but has no live heartbeat evidence yet; asynchronous completion, timeout, and non-retryable failure remain deferred. |
| Child-workflow two-stage lifecycle | Start commands, start acknowledgements, terminal child resolution, sequence correlation, failure causes, all four cancellation policies, duplicate/out-of-order rejection, and lease retirement are represented in the semantic protocol and tested in both languages. Focused native-worker tests also preserve nested failure details and retryability while retiring the parent lease on malformed lifecycle transitions. | A real parent calls `Child_workflow.execute`, receives a start acknowledgement and terminal result through the worker, and returns the child's timer-derived value. The expanded failure, cancellation, retry, duplicate, and recovery cases are local protocol/worker evidence only and remain untested live. |
| Native readiness and lifecycle ownership | Rust readiness lanes, bounded waits that release the OCaml runtime lock, one owner-Domain supervisor, C/Rust response ownership, cleanup, and ABI checks are documented in [Core bridge](core-bridge.md) and covered by bridge, Rust, and supervisor tests. | **Historical:** five baseline executions were verified in CI run [`29191260073`](https://github.com/mfow/ocaml-temporal/actions/runs/29191260073). The current seven-run path adds marker-guarded cancellation while work is outstanding, heartbeat retry, and checks the worker shutdown marker after its signal watcher and public shutdown operation complete; those assertions are locally covered but not live-verified because the attempted [Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29193818312) was cancelled. |

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
| Live child failure/cancellation, restart, replay, and cache-eviction scenarios | **Deferred as acceptance scenarios.** Focused local protocol and worker tests now cover child start rejection, terminal failure (including retryable and non-retryable state), cancellation-policy translation, duplicate/out-of-order lifecycle events, and lease cleanup. The local two-binary target also adds exact cancellation of one outstanding long-running execution and a heartbeat-detail retry, but the expanded seven-run assertion and child failure/cancellation paths are not live-verified; broader recovery cases remain planned. | [Live acceptance coverage](live-acceptance-coverage.md) |
| Signals, queries, updates, validators, conditions, and handler policies | **Not implemented in the public API.** | [Roadmap Phase 5](../implementation-roadmap.md#delivery-order) |
| Continue-as-new | **Implemented in the public workflow API, unit-tested, and covered by bilateral Core conversion tests.** `Temporal.Workflow.continue_as_new` emits a terminal command with a freshly encoded successor input; live Temporal Server coverage is still pending. | [Writing workflows in OCaml](../guides/workflows.md#continue-a-run-with-fresh-history), [Core protocol](core-protocol.md#continue-as-new) |
| Patches, side effects, versioning, external workflow operations, memo, search attributes, priority, and fairness controls | **Not implemented in the public workflow API.** Some related Core fields are retained by private protocol types, but they are not exposed as executable OCaml operations. | [Roadmap Phase 6](../implementation-roadmap.md#delivery-order) |
| Local activities, asynchronous activity completion, and interceptors | **Not implemented in the public activity API.** | [Native activity execution](native-activity-execution.md), [roadmap Phase 7](../implementation-roadmap.md#delivery-order) |
| Live activity heartbeat acceptance | **Implemented as a focused native slice plus a two-binary acceptance scenario, not live-verified.** `smoke.activity_heartbeat_retry` sends `SMOKE:HEARTBEAT:PROGRESS:1` with a 500 ms heartbeat timeout, fails once, and requires the next attempt to read that detail and timeout through the context. Strict bilateral JSON validation, lease retention, and post-completion context invalidation are covered by local tests; the fixture source contract covers both registrations and the driver assertion. | A real Temporal Server heartbeat/detail/retry run remains required. Heartbeat-timeout-triggered retry is still deferred because stale completion handling and asynchronous activity completion need a separate capability slice. [Native activity execution](native-activity-execution.md), [activity protocol](activity-protocol.md), [Live acceptance coverage](live-acceptance-coverage.md) |
| Structured-concurrency scopes and implicit cancellation of losing futures | **Experimental cooperative slice.** `Temporal.Scope.create`, `with_scope`, `check`, `cancel`, and `await` provide a workflow-owned cancellation signal and typed `Cancelled` observation result. The scope does not emit Temporal activity or child-workflow cancellation commands, and `Future` combinators still do not cancel losing inputs implicitly. | [Roadmap Phase 4](../implementation-roadmap.md#delivery-order), [workflow guide](../guides/workflows.md#cooperative-cancellation-scopes) |
| Schedules, visibility, reset/terminate client commands, update handles, Nexus, and test-server controls | **Not implemented in the public client API.** Exact-run cancellation is implemented, but the client does not yet expose these other control-plane features. | [Roadmap Phase 8](../implementation-roadmap.md#delivery-order) |
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
