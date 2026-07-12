# Live acceptance coverage

This matrix records what the repository has actually proved, and what still
needs a real Temporal Server. It separates deterministic unit/runtime evidence
from the current PostgreSQL/Temporal lifecycle smoke and from the planned
two-process workflow test.

## How to read the statuses

- **Verified (synthetic only)** means the OCaml runtime, protocol adapter, or
  bridge behavior was tested without a workflow execution hosted by Temporal
  Server. It is useful evidence, but it is not live compatibility evidence.
- **Verified (lifecycle only)** means `make test-temporal-integration` used the
  real PostgreSQL and Temporal containers and exercised Core client/worker
  construction and shutdown. It does not mean that a workflow result was
  produced.
- **Planned — two-binary gate** means the assertion belongs in the first live
  Compose test: one OCaml executable drives the client and another owns the
  worker. The existing scaffold compiles, but its live flag is deliberately
  disabled.
- **Planned — later expansion** means the capability is intentionally added
  after the first end-to-end result path is trustworthy.

Pull requests that add protocol or runtime pieces do not change these labels
until the corresponding live assertion has run. In particular, child-workflow
resolution has focused tests in merged PR #44, but it has not yet been proved
through the complete two-process Temporal path. PR #41 is still open; it is not
evidence of a passing live test.

## Coverage matrix

| Capability or scenario | Current local evidence | Real server evidence today | Next live assertion and status |
| --- | --- | --- | --- |
| Typed workflow/activity definitions, helper composition, and codecs | **Verified (synthetic only).** `make test-unit`; [`test/unit/test_definition.ml`](../../test/unit/test_definition.ml), [`test/unit/test_codec.ml`](../../test/unit/test_codec.ml), and [`test/unit/test_workflow_authoring.ml`](../../test/unit/test_workflow_authoring.ml). | None for a workflow result. The lifecycle smoke does not execute a registered OCaml definition. | The worker registers ordinary OCaml functions and the driver decodes their typed results. **Planned — two-binary gate.** |
| Direct-style workflow execution, suspension, futures, and deterministic replay | **Verified (synthetic only).** `make test-runtime`; [`test/runtime/test_scheduler.ml`](../../test/runtime/test_scheduler.ml), [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml), and native execution tests. | None for a workflow activation/result. | Run the same direct-style definitions through real poll, activation, completion, and result-wait operations. **Planned — two-binary gate.** |
| OCaml/Rust JSON bridge validation and payload boundaries | **Verified (synthetic only).** `make test-bridge`; OCaml protocol tests under [`test/bridge/`](../../test/bridge/) and Rust protocol tests under [`rust/core-bridge/tests/`](../../rust/core-bridge/tests/). The tests validate closed semantic records, ownership, rejection, and normalization. | **Verified (lifecycle only)** for the bridge calls needed to connect and construct the Core graph. No workflow payload has crossed the live path yet. | Send encoded inputs and results through both public binaries; validate every poll and completion record before state changes. **Planned — two-binary gate.** |
| PostgreSQL, Temporal Server, namespace, and Core lifecycle | Focused supervisor/bridge lifecycle tests cover invalid and repeated transitions. | **Verified (lifecycle only).** `make test-temporal-integration` starts the fixture, waits for health, runs [`test/integration/test_core_lifecycle.ml`](../../test/integration/test_core_lifecycle.ml), and cleans the project. | Keep this as the infrastructure prerequisite for every live scenario. It is not a workflow-result test. |
| A workflow starts, runs, and returns a terminal result | Synthetic activation and native adapter tests cover command construction and terminal handling. | No live workflow result is asserted today. | The driver starts known workflow IDs, retains exact run IDs, waits through `Temporal.Client`, and exits nonzero for any wrong result. The worker must execute the registered OCaml workflow. **Planned — two-binary gate.** |
| Durable timers and wake-up | **Verified (synthetic only).** [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml) covers zero-duration behavior, timer scheduling, and timer resolution. | The lifecycle smoke does not schedule a workflow timer. | `smoke.timer_then_activity` starts a short durable timer, waits for it, and then runs its activity. **Planned — two-binary gate.** |
| Remote activity task polling and completion | **Verified (synthetic only).** [`test/runtime/test_native_activity_execution.ml`](../../test/runtime/test_native_activity_execution.ml), worker execution tests, and activity bridge tests cover typed task/completion conversion and lease retention. | The live lifecycle test creates a worker but does not poll or complete a remote activity. | `smoke.mock_transform` must run in the worker and return a deterministic decoded result. Activity failure, retry, timeout, and heartbeat cases are **Planned — later expansion**. |
| Multiple operations scheduled before awaiting (`both`, `all`, `race`, `first`) | **Verified (synthetic only).** Scheduler and activation tests cover completion ordering, first-error behavior, and cancellation semantics. | No live workflow currently demonstrates concurrent scheduling. | `smoke.fan_out` starts two mock activities before its first wait and checks the ordered combined result. Race and cancellation scenarios are **Planned — later expansion**. |
| Typed workflow failures and non-success terminal outcomes | **Verified (synthetic only).** Error, protocol, client, worker, and runtime tests check typed rejection and terminal state handling rather than using exceptions for expected failures. | **Verified (lifecycle only)** for invalid/repeated lifecycle transitions; no live failed workflow is asserted. | The driver reports typed failure, cancellation, timeout, or codec errors and fails the test on an unexpected terminal state. **Planned — two-binary gate** for the first success path; failure variants are **Planned — later expansion**. |
| Cancellation and graceful shutdown with outstanding work | **Verified (synthetic only).** Activation cancellation/cache eviction and supervisor shutdown tests cover state cleanup and idempotence. | **Verified (lifecycle only)** for deterministic worker/client shutdown after setup; no outstanding live workflow is cancelled. | Cancel an outstanding live execution, verify the worker retires its task, and exercise graceful Compose teardown. **Planned — later expansion.** |
| Child-workflow start, acknowledgement, and terminal resolution | **Verified (synthetic only).** Merged PR #44 adds bilateral fixtures and focused tests for ordered start/resolution, failures, duplicate sequences, and lease retirement. | No live parent/child result has been asserted. | Route both child-resolution activations through the supervisor and exercise a parent awaiting a child against Temporal Server. **Planned — later expansion.** |
| Worker restart, replay, sticky-cache eviction, and continued execution | **Verified (synthetic only).** Runtime tests cover replay-stable commands and cache eviction; native tests cover retention and lease cleanup. | The lifecycle smoke starts one graph and shuts it down; it does not restart a worker or replay a workflow. | Restart the worker while executions are pending, then verify replay and eviction through terminal results. **Planned — later expansion.** |

## Stable evidence commands

These commands are the supported local gates for the corresponding matrix
rows:

```sh
make test-unit                 # definitions, codecs, public API, and errors
make test-runtime              # scheduler, activations, futures, replay
make test-bridge               # OCaml/Rust ABI and protocol fixtures
make verify                    # broad build, lint, Rust, bridge, and quality gates
make test-temporal-integration # real PostgreSQL/Temporal Core lifecycle only
```

The planned live gate will be a new Make target owned by
[`test/integration/temporal/`](../../test/integration/temporal/). It must start
the fixture, run separate driver and worker OCaml binaries, assert terminal
results, print useful logs on failure, and clean up its Compose project. Until
that target exists and exits successfully, a green lifecycle check must not be
described as end-to-end workflow acceptance.
