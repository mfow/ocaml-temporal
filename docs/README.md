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
  remote activity tasks, completions, and opaque task-token ownership.
- [Native workflow execution](reference/native-worker-execution.md) documents
  the current native worker command slice and its child-workflow gate.
- [Native activity execution](reference/native-activity-execution.md) documents
  activity dispatch and completion retention.
- [OCaml SDK logging](reference/observability.md) documents log sources, tags,
  levels, privacy, and Domain behavior.
- [Local Temporal stack](reference/local-temporal-stack.md) documents the
  PostgreSQL/Temporal Server Compose fixture and Make targets.
- [Two-OCaml-binary acceptance design](reference/two-ocaml-binary-e2e-acceptance.md)
  records the planned live driver/worker result test. Its scaffold is not a
  passing live acceptance test yet.
- [Quality and security gates](reference/quality-gates.md) documents pinned
  scanners and the checks run locally and in CI.
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
| Public native worker | Focused adapter, supervisor, Rust bridge, and lifecycle tests | The live Compose target does not yet run a workflow-result driver and worker |
| Public native client | Typed start/wait protocol and lifecycle tests | No enabled two-process Compose result assertion yet |
| Child workflows | Scheduling and command translation in the synthetic/runtime layers | Native parent completion is gated until child-resolution activations are implemented |
| Temporal/PostgreSQL stack | `make test-temporal-integration` starts and health-checks real containers | Current assertion is Core client/worker lifecycle, not a workflow result |

This distinction prevents a green local synthetic test from being read as a
claim that an unimplemented native feature is ready. Features such as signals,
queries, updates, continue-as-new, versioning, local activities, Nexus, and
the remaining SDK parity work are tracked as later milestones.

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
make test-temporal-integration
```

`make verify` combines the OCaml version check, Dune build/lint, Rust format and
Clippy checks, Rust tests, bridge/install smoke tests, and the repository
quality contract. `make quality` is a separate host-tool gate and requires the
pinned `cargo-deny`, `cargo-machete`, and `typos` binaries. The license audit is
intentionally separate from the OCaml-version matrix and runs in containers.
For the real server smoke, the Makefile owns Compose project selection,
readiness checks, failure logs, and volume cleanup; do not run the fixture's
Compose file from the repository root.

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
