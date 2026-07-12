# Documentation guide

This directory describes an experimental OCaml Temporal SDK. The project is
building a worker SDK, not only a client that submits a request and reads a
result. Read the documents in this order when you are new to the repository:

1. [Writing workflows](guides/workflows.md) introduces the public OCaml API
   and marks which examples are synthetic-only versus native-ready.
2. [Verified progress](progress.md) records what has actually been implemented
   and tested. It is the best source for the current milestone boundary.
3. [Implementation roadmap](implementation-roadmap.md) lists work that is
   planned but not yet a supported feature.
4. [Runtime invariants](reference/runtime-invariants.md) explains the rules
   that workflow execution and replay must preserve.
5. [Native Core bridge](reference/core-bridge.md) explains ownership and the
   OCaml/C/Rust boundary.

The remaining reference documents are useful when changing one subsystem:

- [Native execution translation](reference/native-execution-translation.md)
  describes checked activation and command conversion.
- [Private JSON control protocol](reference/core-protocol.md) defines the
  bilateral envelope and its validation rules.
- [Native client JSON protocol](reference/client-protocol.md) documents typed
  workflow-start and exact-run-wait messages.
- [OCaml activity protocol adapter](reference/activity-protocol.md) documents
  remote activity tasks, completions, heartbeats, and opaque task-token
  ownership. The heartbeat schema is
  [`activity-heartbeat.schema.json`](schemas/bridge/activity-heartbeat.schema.json).
- [Native workflow execution](reference/native-worker-execution.md) documents
  the current native worker command slice and two-stage child resolution.
- [Native activity execution](reference/native-activity-execution.md) documents
  activity dispatch and completion retention.
- [OCaml SDK logging](reference/observability.md) documents log sources, tags,
  levels, privacy, and Domain behavior.
- [Local Temporal stack](reference/local-temporal-stack.md) documents the
  PostgreSQL/Temporal Server Compose fixture and Make targets.
- [Two-OCaml-binary acceptance design](reference/two-ocaml-binary-e2e-acceptance.md)
  records the live driver/worker result test, its success-path evidence, and
  the scenarios that still need real-server coverage.
- [Live acceptance coverage](reference/live-acceptance-coverage.md) separates
  synthetic evidence from the verified real-server two-binary success path
  and planned scenario expansion.
- [Worker restart and replay acceptance design](reference/worker-restart-replay-acceptance.md)
  specifies the next controlled worker-replacement scenario, its exact
  assertions, diagnostic evidence, and fresh-volume cleanup rules. It is a
  design document, not live verification.
- [Worker restart/replay diagnostic contract](reference/worker-restart-replay-diagnostics.md)
  defines the payload-free normalized history and generation/replay records
  used by the offline contract gate before the live controller exists.
- [Feature coverage and implementation status](reference/feature-coverage.md)
  gives the short status reference and distinguishes live evidence, mock-only
  tests, partly live-tested native bridge support, and deferred features.
- [Quality and security gates](reference/quality-gates.md) documents pinned
  scanners and the checks run locally and in CI.
- [Installed package boundary](reference/package-boundary.md) documents which
  libraries are package-private and the installed-consumer regression that
  protects the public `Temporal` surface.
- [Architecture specification](superpowers/specs/2026-07-11-ocaml-temporal-sdk-design.md)
  records the long-term design. APIs described there may be future work.

Files under `superpowers/plans/` are historical implementation plans. When a
plan and the source disagree, the source, tests, and progress record win.

## How the pieces fit together

An application writes ordinary OCaml functions and links the `temporal-sdk`
library into its own final executable. `Temporal.Worker` owns workflow and
activity registration and executes the current native task slice. `Temporal.Client`
starts an execution and waits for the exact workflow/run pair returned by
Temporal; it does not execute workflow code.

For an HTTP(S) endpoint, the public library creates one private supervisor per
SDK instance. That supervisor owns the Rust runtime, Temporal Core client, and
optional worker, and serializes lifecycle operations on one owner Domain. Rust
is a static library linked into the OCaml executable. There is no required
Rust sidecar or separate OCaml/Rust service to deploy.

The private OCaml/Rust boundary uses strictly validated JSON semantic records.
This is an internal, inspectable representation chosen to keep the ownership
and validation contract small. Temporal Server does not receive these JSON
records: Rust converts them to and from the official Temporal Core protobuf and
gRPC APIs. JSON is also one possible **payload encoding** (`json/plain`), which
is a separate concern from the private bridge protocol. Payloads are otherwise
opaque bytes with encoding metadata.

## Current status at a glance

| Layer | Evidence today | Important limit |
| --- | --- | --- |
| Pure OCaml workflow runtime | Dune unit and runtime tests | Synthetic activation/replay, not proof of live Server compatibility |
| Public native worker | Focused adapter, supervisor, Rust bridge, lifecycle tests, and a real two-binary Compose path with historical evidence for four successes and one typed workflow failure. The six-run exact-run cancellation assertion is implemented and locally covered, but is not live-verified because its attempted Actions run was cancelled. | Child failure/cancellation, replay, and recovery scenarios remain untested live |
| Public native client | Typed start/wait/cancel protocol. The historical live evidence is the five-run baseline; the sixth exact-run cancellation assertion is implemented and locally covered, but is not live-verified. The continue-as-new semantic command is implemented and tested locally. | Continue-as-new still needs live Temporal Server validation, and other client commands remain untested live |
| Child workflows | Scheduling, command translation, and two-stage native resolution are covered by focused Rust/OCaml tests and one live parent/child success path | Child start failure, cancellation, retry, replay, and recovery remain untested live |
| Temporal/PostgreSQL stack | `make test-temporal-integration` starts real containers, runs a public worker and a separate public client driver, and asserts exact results | The first gate is deliberately narrow and does not yet cover every terminal or recovery path |

This distinction prevents a green local synthetic test from being read as a
claim that an unimplemented native feature is ready. Continue-as-new and
context-aware activity heartbeats are implemented and focused-tested at the
OCaml/native bridge, but neither has live Temporal Server acceptance yet.
Features such as signals, queries, updates, versioning, local activities,
Nexus, and the remaining SDK parity work are tracked as later milestones.

## Build and test commands

The Makefile is the supported command interface. The normal commands run in a
Docker Compose development container, so host language-tool installation is
optional for build and test work:

```sh
make build
make test-unit
make test-runtime
make verify
make quality
make license-check
make test-temporal-worker-restart
make test-temporal-integration
```

`make verify` combines the OCaml version check, Dune build/lint, Rust format and
Clippy checks, Rust tests, bridge/install smoke tests, and the repository
quality contract. `make quality` is a separate host-tool gate and requires the
pinned `cargo-deny`, `cargo-machete`, and `typos` binaries. The license audit is
intentionally separate from the OCaml-version matrix and runs in containers.
If a local Docker VM has limited memory for native linkers, set
`DUNE_JOBS=1` (or another small value) on build/lint targets; the variable is
empty by default so CI keeps its normal parallelism.
For the real server smoke, the Makefile owns Compose project selection,
readiness checks, failure logs, and volume cleanup; do not run the fixture's
Compose file from the repository root.

`make test-temporal-worker-restart` is deliberately different from the live
integration command: it uses no Docker and validates only the normalized
history/replay diagnostic contract and its rejection paths. It must not be
used as evidence that a worker was restarted or that Temporal replay occurred.

## Terms used in this project

- **Temporal Server** stores workflow history and dispatches workflow and
  activity work. It does not execute OCaml workflow functions.
- **Temporal Core** is Temporal's official Rust worker/client library. It
  manages server communication and durable worker state machines for an SDK.
- **SDK** is the whole OCaml-facing worker implementation: workflow runtime,
  activity dispatch, lifecycle, and the optional client start/wait surface.
- **Client** is the smaller `Temporal.Client` API for starting a workflow and
  waiting for one exact execution. It is not the worker.
- **Worker** is a `Temporal.Worker` value that registers local OCaml workflow
  and activity functions and polls/completes tasks.
- **Workflow activation** is a validated batch of work that Core delivers to
  the OCaml runtime, such as starting a workflow, resolving an activity, firing
  a timer, cancelling, or removing a cached execution.
- **Command** is a validated instruction emitted by workflow code, such as
  scheduling an activity or starting a durable timer. Core sends the command
  to Temporal Server as part of the workflow completion.
- **Replay** means running workflow code again against recorded history. A
  replay must make the same decisions and produce the same commands.
- **Payload** is an opaque byte sequence plus metadata naming its encoding.
  Temporal does not require JSON for payloads.
- **Codec** converts a typed OCaml value to and from a payload. The built-in
  string codec uses `json/plain`; bytes and unit use binary encodings.
- **Future** is a workflow-owned value that becomes ready in a later
  activation. Waiting suspends the current workflow fiber, not an operating
  system thread or an unrelated application task.
- **Supervisor** is the private one-owner Domain that serializes native
  lifecycle and bridge operations for one SDK instance. Applications do not
  handle its native graph directly.
- **Bridge** is the small C-compatible boundary between the OCaml library and
  the Rust static library. It copies owned bytes and keeps protobuf details out
  of public OCaml code.

When a term is still unclear, start with the workflow guide and then follow the
subsystem reference linked above; avoid inferring live feature support from a
type or a synthetic test alone.
