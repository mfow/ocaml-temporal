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
| 2 | Rust static library, OCaml C stubs, private owner-Domain mailbox, live worker poll/completion loop, minimum OCaml client, and the real Compose smoke-test topology | An OCaml test-client container starts a workflow executed by a separate OCaml worker against Temporal Server and PostgreSQL | In progress |
| 3 | Expand the same smoke suite across payloads, durable timers, mock activities, concurrent scheduling, failures, retries, cancellation, restart replay, and cache eviction | Every implemented essential path has a live success test and its important failure/lifecycle tests | Planned |
| 4 | Child workflows and structured concurrency (`both`, `all`, `race`, `first`, scopes), added to the live smoke suite | Parent workflows fan out to mock activities and children, await one/all, and cancel safely through the real cluster | In progress: synthetic authoring kernel complete; live translation and scopes pending |
| 5 | Signals, queries, updates, validators, conditions, and handler policies | CLI-driven interactive workflow tests pass, including mode violations | Planned |
| 6 | Continue-as-new, patches, side effects, external workflow operations, memo, search attributes, priority, and fairness | Recorded histories replay and advanced command integration tests pass | Planned |
| 7 | OCaml activities, local activities, heartbeats, async completion, interceptors, payload codecs, and graceful shutdown | Activity conformance and Kubernetes-style termination tests pass | Planned |
| 8 | Client API, schedules, visibility, reset/terminate/cancel, update handles, Nexus, and test-server controls | Client conformance suite passes against supported Temporal Server versions | Planned |
| 9 | Performance, observability, security, packaging, API stability, and release automation | Published benchmark report, SBOM/license audit, OPAM lint, docs, and release dry run pass | Planned |
| 10 | Parity closure | Every parity-matrix row links to implementation, tests, and documentation | Planned |

## Plan documents

1. [Foundation and deterministic runtime](superpowers/plans/2026-07-11-foundation-and-deterministic-runtime.md)
2. [Core bridge and first real workflow](superpowers/plans/2026-07-11-core-bridge-and-first-real-workflow.md)
   The private mailbox processor is a completed Phase 2 foundation described
   by [ADR 0003](decisions/0003-private-mailbox-processor.md). The one-Domain
   SDK graph supervisor now owns the real Rust runtime as described by
   [ADR 0004](decisions/0004-sdk-instance-supervisor.md); live client/worker
   handles and their poll/completion operations remain pending.
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

The smoke suite starts with one live workflow as soon as the minimum worker and
client paths exist. Every subsequent essential capability adds scenarios to
that same suite. The suite is table-driven and records both the supported case
and its important error, cancellation, replay, or shutdown behavior. It is not
considered complete while an essential SDK capability is exercised only by the
synthetic interpreter.

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
