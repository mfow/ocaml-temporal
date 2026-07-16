# Implementation Roadmap

This roadmap decomposes the approved architecture into independently testable
subprojects. It does not redefine the final objective: the project is complete
only when the acceptance criteria in the architecture specification and every
capability in the parity matrix are verified.

“Complete” in the table means that the repository has passed the evidence for
that phase; it does not mean the whole SDK is production-ready. Core worker and
replay behavior is implemented before ergonomic features borrowed from other
Temporal SDKs. Those later features should preserve the useful behavior while
using idiomatic OCaml APIs. They may be implemented in OCaml even when another
SDK implements them in its host-language layer or in Rust, if that produces a
cleaner and more maintainable OCaml design.

## Delivery order

| Phase | Deliverable | Runtime evidence | Status |
|---|---|---|---|
| 1 | Repository foundation, typed public definitions, codecs, deterministic futures, effect scheduler, and synthetic activations | `make verify` runs from Docker Compose and deterministic command tests pass | Complete |
| 2 | Rust static library, OCaml C stubs, private owner-Domain mailbox, live worker poll/completion loop, minimum OCaml client, and the real Compose smoke-test topology | An OCaml test-client container starts workflows executed by a separate OCaml worker against Temporal Server and PostgreSQL | Complete: the initial two-binary fan-out and timer/activity success paths pass in Linux CI |
| 3 | Expand the same smoke suite across payloads, durable timers, mock activities, concurrent scheduling, failures, retries, cancellation, restart replay, and cache eviction | Every implemented essential path has a live success test and its important failure/lifecycle tests | In progress: fan-out, timer/activity, parent/child, ordinary activity retry, heartbeat-detail retry, timeout-triggered retry, typed non-retryable workflow failure, child failure/cancellation, continue-as-new, delayed asynchronous completion, and marker-guarded exact-run cancellation passed the local OCaml 5.5 Compose run. The two-generation restart/replay controller, activation diagnostics, history normalizer, and offline contract passed the real Temporal/PostgreSQL acceptance job in [PR #253](https://github.com/mfow/ocaml-temporal/actions/runs/29286560471). Sticky-cache eviction against a real worker passed in the complete [PR #322 run](https://github.com/mfow/ocaml-temporal/actions/runs/29402103748), and the bilateral parent/child replacement gate passed in the complete [PR #351 run](https://github.com/mfow/ocaml-temporal/actions/runs/29434016013); broader recovery and failure coverage remain. |
| 4 | Child workflows and structured concurrency (`both`, `all`, `race`, `first`, scopes), added to the live smoke suite | Parent workflows fan out to mock activities and children, await one/all, and cancel safely through the real cluster | In progress: child command and two-stage start/terminal resolution translation are complete; focused tests cover child start rejection/failure, duplicate or out-of-order lifecycle events, cancellation-policy translation, explicit child retry-policy Core conversion, and lease cleanup. Parent/child success, propagated failure, cancellation, retry, and duplicate-ID start failure are live-verified by [PR #289](https://github.com/mfow/ocaml-temporal/actions/runs/29333761719). The complete [PR #351 run](https://github.com/mfow/ocaml-temporal/actions/runs/29434016013) live-verifies the dedicated exact-run parent/child replay/recovery gate; server-side cancellation and broader child failure recovery remain. |
| 5 | Signals, queries, updates, validators, conditions, and handler policies | CLI-driven interactive workflow tests pass, including mode violations | In progress: typed definitions, validator ordering, deterministic local dispatch, native scheduler-owned signal-handler delivery, output-only query delivery, typed exact-run client signal submission with bilateral validation, the immediate one-input/non-suspending update bridge with replay validator skipping, and workflow-local `Temporal.Condition` waits with FIFO rechecking and teardown cleanup are implemented and focused-tested; typed signal delivery and condition wake-up are live-verified by [PR #266](https://github.com/mfow/ocaml-temporal/actions/runs/29311239247), while suspended update continuations, richer handler policies, live query/update acceptance, and broader interaction coverage remain. |
| 6 | Continue-as-new, patches, worker versioning, side effects, external workflow operations, memo, search attributes, priority, and fairness | Recorded histories replay and advanced command integration tests pass | In progress: continue-as-new is implemented and live-verified. `Temporal.Workflow.patched` and unit-returning `deprecate_patch` share per-execution decisions and emit active/deprecated Core markers with mixed-mode protection. Bilateral JSON validation, Core conversion, and the three-transition live acceptance gate are implemented and live-verified in [PR #356](https://github.com/mfow/ocaml-temporal/actions/runs/29469232271). Legacy build-ID worker routing is now exposed through `Temporal.Worker.Options` and maps to Core's `LegacyBuildIdBased` strategy; a dedicated live routing gate, deployment-based versioning, side effects, external operations, memo/search attributes, priority, fairness, and broader CI evidence remain pending |
 | 6 | Continue-as-new, patches, worker versioning, side effects, external workflow operations, memo, search attributes, priority, and fairness | Recorded histories replay and advanced command integration tests pass | In progress: continue-as-new is implemented and live-verified. `Temporal.Workflow.patched` and unit-returning `deprecate_patch` share per-execution decisions and emit active/deprecated Core markers with mixed-mode protection. Bilateral JSON validation, Core conversion, and the three-transition live acceptance gate are implemented and live-verified in [PR #356](https://github.com/mfow/ocaml-temporal/actions/runs/29469232271). Legacy build-ID worker routing is now exposed through `Temporal.Worker.Options` and maps to Core's `LegacyBuildIdBased` strategy. Activity scheduling now exposes validated priority/fairness metadata (priority key, fairness group, and exact single-precision weight bits). Dedicated live routing evidence, deployment-based versioning, side effects, external operations, memo/search attributes, workflow-level priority/fairness, and broader CI evidence remain pending |
| 7 | OCaml activities, local activities, heartbeats, async completion, interceptors, payload codecs, and graceful shutdown | Activity conformance and Kubernetes-style termination tests pass | In progress: remote activities, context-aware heartbeat detail/retry, graceful shutdown, and the typed asynchronous completion bridge are implemented and focused-tested; ordinary, heartbeat-detail, timeout-triggered retry, delayed asynchronous completion, and shutdown paths are live-verified locally. Core heartbeat response flags, heartbeat-timeout retry, local activities, interceptors, and broader conformance remain |
| 8 | Client API, schedules, visibility, reset/terminate/cancel, update handles, Nexus, and test-server controls | Client conformance suite passes against supported Temporal Server versions | In progress: public client start, exact-run wait, exact-run cancellation, typed exact-run signal submission, and idempotent shutdown are implemented and focused-tested; start/wait/cancellation remain live-verified in [PR #210](https://github.com/mfow/ocaml-temporal/actions/runs/29221151859), and typed signal acceptance is live-verified by [PR #266](https://github.com/mfow/ocaml-temporal/actions/runs/29311239247), while schedules, visibility, reset/terminate, updates, Nexus, and test-server controls remain. |
| 9 | Performance, observability, security, packaging, API stability, and release automation | Published benchmark report, SBOM/license audit, OPAM lint, docs, and release dry run pass | In progress: structured `logs` observability, quality/security checks, permissive-license audits, and OPAM packaging/lint gates are implemented; benchmarks, SBOM, API-stability validation, and release automation remain |
| 10 | Parity closure | Every parity-matrix row links to implementation, tests, and documentation | Planned |

## Plan documents

1. [Foundation and deterministic runtime](superpowers/plans/2026-07-11-foundation-and-deterministic-runtime.md)
2. [Core bridge and first real workflow](superpowers/plans/2026-07-11-core-bridge-and-first-real-workflow.md)
   The private mailbox processor is a completed Phase 2 foundation described
   by [ADR 0003](decisions/0003-private-mailbox-processor.md). The one-Domain
   SDK graph supervisor now owns the real Rust runtime, client, and validated
   workflow/remote-activity worker as described by [ADR
   0004](decisions/0004-sdk-instance-supervisor.md). The bilateral first
   activation/completion semantic adapter is complete as described by [ADR
   0006](decisions/0006-first-workflow-semantic-protocol.md). Rust now owns one
   guarded workflow poll lane, one guarded remote-activity lane, their shared
   task ledger, and bounded owner-domain readiness waits. The pure-OCaml
   activation translation, execution command conversion, and private
   existential run registry are now covered by focused tests and exercised by
   the first public worker/driver live success path.
   Readiness waits intentionally return to that mailbox after 100 ms when Core
   is quiet. The current
   translation now preserves and validates every field needed by Core activity
   commands, including deterministic defaults for omitted queue and timeout
   options. Child commands now have closed semantic records and Core
   conversion; start acknowledgments and terminal child results are translated
   through the same JSON protocol and are covered by focused lifecycle tests.
   The basic live worker wiring and Compose acceptance path are complete. One
   parent/child success path is wired into that fixture. The public API now has
   an experimental cooperative `Temporal.Scope` slice: it deterministically
   cancels observation of a future and returns a typed `Cancelled` result, but
   it does not yet emit activity or child-workflow cancellation commands.
   Focused tests now cover scope ownership, repeated cancellation, child
   start/terminal lifecycle edges, and malformed cancellation input. The
   two-binary acceptance live-verifies child cancellation and exact-run
   top-level cancellation. The two-generation worker restart/replay path is
   now also live-verified in [PR #253](https://github.com/mfow/ocaml-temporal/actions/runs/29286560471);
   broader live child lifecycle and cache-recovery coverage remain pending.
   Poll decode failures use an exact-document rejection ABI: Rust retains
   semantic handoff state and will not retire a lease for a changed workflow
   activation or activity task.
3. Activities, timers, and replay (written after Phase 2 evidence is committed)
4. Child workflows and structured concurrency (written after Phase 3 evidence is committed)
5. Interactive and advanced features (split further at the preceding review gate)
6. Platform breadth and publication hardening (split further at the preceding review gate)

Each detailed plan is written immediately before its phase so it can use the
actual interfaces and upstream Core revision proven by prior phases. This
prevents later plans from pretending that unstable internal APIs are already
known while preserving the full target in this roadmap.

## End-to-end acceptance topology

The first live vertical slice creates the deployment shape used by all later
essential-feature tests:

- PostgreSQL stores Temporal Server state.
- Temporal Server uses PostgreSQL and exposes its normal frontend service.
- An OCaml worker container links this library and registers the workflow and
  deterministic mock activity implementations used by the suite.
- A separate OCaml test-client container links the same library, starts each
  test workflow, waits for its result, and checks the expected outcome.

The smoke suite contains thirteen top-level scenarios: fan-out, timer/activity,
continue-as-new successor following, ordinary activity retry, heartbeat-detail
activity retry, delayed asynchronous activity completion, start-to-close timeout
retry, successful parent/child execution, propagated child failure, child
cancellation, typed non-retryable workflow failure, marker-guarded exact-run
cancellation, and signal/condition acceptance. The current driver starts twelve before its first terminal wait, waits for the signal workflow's
worker-visible readiness marker before signaling it, then starts the timeout-retry workflow after heartbeat completion. It asserts
each expected terminal outcome and records bounded operation-phase and shutdown
diagnostics. The [PR #266 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29311239247)
passed this baseline against Temporal Server and PostgreSQL, followed by the
two-generation restart/replay acceptance. Every subsequent essential capability
adds scenarios to the same suite. It is not considered complete while an
essential SDK capability is exercised only by the synthetic interpreter.

## Dependency and licensing gate

Every phase must update the dependency inventory before it can be committed.
Project dependencies must use MIT, Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC,
Zlib, PostgreSQL, or another explicitly reviewed permissive license. The only
standing exception is `LGPL-2.1-or-later WITH OCaml-LGPL-linking-exception` for
the OCaml compiler/runtime or an individually reviewed OCaml dependency. No
ordinary GPL, AGPL, LGPL, MPL, EPL, CDDL, SSPL, BUSL, Commons Clause,
source-available, non-commercial, missing, or unknown dependency is accepted.

The release artifacts and runtime container are audited independently from the
host operating system and ephemeral CI/build environment. Build tools are
recorded in the inventory; their licenses and whether they are redistributed
are made explicit rather than inferred from the final binary.
