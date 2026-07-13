# Live acceptance coverage

This matrix records what the repository has actually proved against a real
Temporal Server and what still relies on deterministic local tests. It keeps
the first two-OCaml-binary success path separate from broader Temporal SDK
claims.

## How to read the statuses

- **Verified (live success path)** means `make test-temporal-integration` used
  real PostgreSQL and Temporal containers, a public OCaml worker, and a
  separate public OCaml driver. It proves only the listed successful scenario;
  it does not imply that every related failure or recovery path is live-tested.
- **Verified (synthetic only)** means the OCaml runtime, protocol adapter, or
  bridge behavior was tested without a workflow execution hosted by Temporal
  Server. It is useful evidence, but it is not live compatibility evidence.
- **Planned — later expansion** means a real-server assertion belongs in the
  existing two-binary Compose fixture after the success path is broadened.

The initial live gate passed in Linux CI for commit `d4456b7`, covering two
workflows. The current driver starts nine workflows before it waits for any
result: `smoke.fan_out`, `smoke.timer_then_activity`,
`smoke.activity_retry`, `smoke.activity_heartbeat_retry`,
`smoke.parent_awaits_child`, `smoke.parent_awaits_failed_child`,
`smoke.parent_cancels_child`, `smoke.non_retryable_failure`, and
`smoke.long_running_cancellation`. The complete [PR #210 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29221151859)
passed all CI jobs, including the live Temporal/PostgreSQL smoke job, for PR
head `47c9a93`; that PR was squash-merged as `f877fbf`.

The run live-verified four exact successes, a second activity task delivered
by an ordinary retry policy, a heartbeat detail and timeout delivered to a
second activity attempt, successful parent/child completion, propagated
non-retryable child failure, child cancellation with
`Wait_cancellation_requested`, a typed non-retryable workflow failure, and
marker-guarded exact-run cancellation. The heartbeat scenario is server-visible:
the first attempt sends `SMOKE:HEARTBEAT:PROGRESS:1`, fails retryably, and the
second attempt can return `SMOKE:HEARTBEAT:RETRIED:SMOKE` only after receiving
that detail and timeout from Temporal. The child-failure driver checks the
public `Workflow` terminal category and retryability flag, while the worker
checks the in-workflow `Child_workflow` category.

The Docker-free contract remains useful for fast feedback:
`test/smoke/test_temporal_heartbeat_contract.sh` protects the independent
driver/worker roles and `test/integration/temporal/common/marker_test/test_smoke_definitions.ml`
checks copied heartbeat details, timeout propagation, and invalidated-context
rejection. Retry-policy construction, JSON representation, and Core conversion
remain separately covered by synthetic tests. Retry timeouts, activity-level
non-retryable error-type classification, replay, and recovery remain outside
the nine-scenario live gate.

The driver in this matrix is a one-shot OCaml assertion runner, not another
worker. It starts known workflows through `Temporal.Client`, waits for their
exact workflow/run results, and exits nonzero when an assertion fails. The
separate `smoke-worker` process registers and executes the workflows and mock
activity. Every integration run starts from a fresh Compose project and drops
the PostgreSQL data volume before and after the test, so its assertions never
depend on history left by an earlier run.

The public client also has an exact-run cancellation operation. Its request,
bounded native RPC, positive acknowledgement, idempotency key, and eventual
`Cancelled` result are covered by local mock, supervisor, OCaml protocol, and
Rust protocol tests. The current local driver implementation starts a
long-running durable-timer workflow, waits for its test-only marker activity to
publish the current run token after the timer and marker commands are issued
together, then sends `Temporal.Client.cancel` using the returned exact handle.
It waits on that same handle and checks the `Cancelled` category, retryability,
and stable message. The driver and worker also report successful shutdown
phases; the Makefile checks both markers before removing the Compose project.
The complete PR #210 run also checked the driver and worker shutdown phases
before Compose cleanup. The client cancellation request is still tested
locally for malformed input, idempotency, and bounded native-RPC behavior, but
the nine-scenario run is live evidence for the successful exact-run cancellation
path. Restart, replay, and cache-eviction behavior require separate runs.

## Coverage matrix

| Capability or scenario | Current local evidence | Real server evidence today | Remaining live boundary |
| --- | --- | --- | --- |
| Typed workflow/activity definitions, helper composition, and codecs | **Verified (synthetic only).** `make test-unit`; [`test/unit/test_definition.ml`](../../test/unit/test_definition.ml), [`test/unit/test_codec.ml`](../../test/unit/test_codec.ml), and [`test/unit/test_workflow_authoring.ml`](../../test/unit/test_workflow_authoring.ml). | **Verified (live success path).** The worker registers ordinary OCaml definitions and the driver decodes their typed results. | Failure and codec-rejection cases need dedicated live scenarios. |
| Direct-style workflow execution, suspension, futures, and deterministic replay | **Verified (synthetic only).** `make test-runtime`; [`test/runtime/test_scheduler.ml`](../../test/runtime/test_scheduler.ml), [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml), and native execution tests. | **Verified for the live suspension path.** Timer, activity, and child waits suspend and later resume smoke workflows through real polling and completion; this path does not exercise deterministic replay. | Replay and unusual scheduling paths remain synthetic-only. |
| OCaml/Rust JSON bridge validation and payload boundaries | **Verified (synthetic only).** `make test-bridge`; OCaml protocol tests under [`test/bridge/`](../../test/bridge/) and Rust protocol tests under [`rust/core-bridge/tests/`](../../rust/core-bridge/tests/) validate closed records, ownership, rejection, and normalization. | **Verified (live success path).** Happy-path client, workflow, and activity records cross both public processes. | Malformed and uncommon record variants need dedicated live fault injection if they become supported scenarios. |
| PostgreSQL, Temporal Server, namespace, and Core lifecycle | Focused supervisor/bridge lifecycle tests cover invalid and repeated transitions. | **Verified (live success path).** The fixture starts the stack, waits for health, runs [`test/integration/test_core_lifecycle.ml`](../../test/integration/test_core_lifecycle.ml), and cleans the project. | Upgrade, persistence, and production-topology coverage are separate concerns. |
| A workflow starts, runs, and returns a terminal result | Synthetic activation and native adapter tests cover command construction and terminal handling. | **Verified (live success path).** The complete [PR #210 run](https://github.com/mfow/ocaml-temporal/actions/runs/29221151859) starts all nine top-level workflows before the first wait and asserts their exact results or typed terminal outcomes. | Timed-out, terminated, and continued-as-new outcomes remain untested live. |
| Durable timers and wake-up | **Verified (synthetic only).** [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml) covers zero-duration behavior, timer scheduling, and timer resolution. | **Verified (live success path).** `smoke.timer_then_activity` and the child in `smoke.parent_awaits_child` each wait for a short durable timer before returning. | Timer cancellation, unusual durations, and replay need separate live assertions. |
| Remote activity task polling and completion | **Verified (synthetic only).** [`test/runtime/test_native_activity_execution.ml`](../../test/runtime/test_native_activity_execution.ml), worker execution tests, and activity bridge tests cover typed task/completion conversion, lease retention, context-aware activity dispatch, prior heartbeat detail delivery, typed heartbeats, and context invalidation. | **Verified for the live success path, including ordinary retry and heartbeat-detail retry.** `smoke.mock_transform`, `smoke.retry_once`, and `smoke.heartbeat_retry` cross the real worker poll/completion path in the [PR #210 run](https://github.com/mfow/ocaml-temporal/actions/runs/29221151859). | Activity timeout, asynchronous completion, and non-retryable failure remain **Planned — later expansion**. |
| Activity retry policy and retry delivery | **Verified (synthetic only).** [`test/unit/test_activity_retry_policy.ml`](../../test/unit/test_activity_retry_policy.ml), [`test/runtime/test_activity_retry_policy.ml`](../../test/runtime/test_activity_retry_policy.ml), OCaml/Rust protocol tests, and Core conversion tests validate the immutable policy, exact coefficient bits, and malformed-input rejection without a server. | **Verified (live ordinary and heartbeat-detail retry paths).** `smoke.activity_retry` requires `SMOKE:ATTEMPT:2`; `smoke.activity_heartbeat_retry` requires the second attempt to receive the first attempt's heartbeat detail and timeout. Both passed in the [PR #210 run](https://github.com/mfow/ocaml-temporal/actions/runs/29221151859). | Retry backoff under larger intervals, non-retryable error-type matching, timeout-triggered retries, and retry behavior across worker restart remain **Planned — later expansion**. |
| Multiple operations scheduled before awaiting | **Verified (synthetic only).** Scheduler and activation tests cover completion ordering, first-error behavior, and cancellation semantics. | **Verified (live success path).** `smoke.fan_out` schedules two activities before its first wait; the cancellation workflow schedules its timer and marker activity before waiting; and the driver starts all nine top-level workflows before awaiting any result. These assertions passed in the [PR #210 run](https://github.com/mfow/ocaml-temporal/actions/runs/29221151859). | `race` and explicit server-history ordering assertions are **Planned — later expansion**. |
| Typed workflow failures and non-success terminal outcomes | **Verified (synthetic and live failure paths).** Error, protocol, client, worker, and runtime tests check typed rejection and terminal state handling without exceptions for expected failures. | **Verified (live success and failure paths).** The [PR #210 run](https://github.com/mfow/ocaml-temporal/actions/runs/29221151859) checks a typed non-retryable top-level workflow failure, propagated non-retryable child failure, child cancellation marker, and exact top-level cancellation metadata. | Timeout, termination, continued-as-new, and child start-failure outcomes remain **Planned — later expansion**. |
| Exact-run client cancellation and graceful shutdown with outstanding work | **Verified (synthetic only).** The public mock client, supervisor, OCaml bridge protocol, and Rust protocol tests validate exact run identity, bounded request handling, positive acknowledgement, stable request IDs, and typed `Cancelled` observation. The nine-run driver also has local marker and result assertions. | **Verified (live success path).** The [PR #210 run](https://github.com/mfow/ocaml-temporal/actions/runs/29221151859) cancels `two-binary-long-running-cancellation`, waits for its exact run, checks category `Cancelled` and `non_retryable=false`, and checks the driver's and worker's graceful-shutdown markers. | Restart, replay, and cache eviction remain **Planned — later expansion**. |
| Activity heartbeat details and timeout propagation | **Verified (synthetic only).** Native activity execution, OCaml/Rust heartbeat protocol, lease retention, context lifetime, and bilateral validation are covered by focused local tests. The Docker-free [`test_temporal_heartbeat_contract.sh`](../../test/smoke/test_temporal_heartbeat_contract.sh) protects the two-process registration, start-before-wait, result assertion, and cleanup shape. [`test_smoke_definitions.ml`](../../test/integration/temporal/common/marker_test/test_smoke_definitions.ml) invokes the exact shared contextual activity twice and checks copied details, timeout propagation, and invalidated-context rejection. | **Verified (live detail/retry path).** `smoke.activity_heartbeat_retry` sends one progress detail with a 500 ms timeout, intentionally fails, and the [PR #210 run](https://github.com/mfow/ocaml-temporal/actions/runs/29221151859) confirms that Temporal returns the detail and timeout on the second attempt. | Heartbeat-timeout-triggered retry, asynchronous completion, and worker restart remain later expansions. |
| Child-workflow start, acknowledgement, and terminal resolution | **Verified (synthetic only).** Focused Rust and OCaml tests cover ordered start/resolution, failures, duplicate sequences, and lease retirement. | **Verified (live success and selected failure paths).** The [PR #210 run](https://github.com/mfow/ocaml-temporal/actions/runs/29221151859) covers `Temporal.Child_workflow.execute` success, propagated non-retryable child failure, and child-handle cancellation. Native worker tests check the in-workflow `Child_workflow` error category, while the client driver checks the parent execution's terminal `Workflow` category and retryability. | Child start failure, retry, replay, and recovery remain **Planned — later expansion**. |
| Worker restart, replay, sticky-cache eviction, and continued execution | **Verified (synthetic only).** Runtime tests cover replay-stable commands and cache eviction; native tests cover retention and lease cleanup. | The live gate starts one worker and shuts it down after the smoke runs. | Restart a worker while executions are pending, then verify replay and eviction through terminal results: **Planned — later expansion.** |

## Stable evidence commands

These commands are the supported local gates for the corresponding matrix
rows:

```sh
make test-unit                 # definitions, codecs, public API, and errors
make test-runtime              # scheduler, activations, futures, replay
make test-bridge               # OCaml/Rust ABI and protocol fixtures
make verify                    # broad build, lint, Rust, bridge, and repository contracts
make test-temporal-integration # real PostgreSQL/Temporal + two OCaml binaries
```

`make test-temporal-integration` is the only command in this list that starts
a real Temporal Server. It owns the fixture lifecycle, starts the independent
worker and one-shot assertion driver, and prints useful failure logs. The
complete [PR #210 run](https://github.com/mfow/ocaml-temporal/actions/runs/29221151859)
is the authoritative live evidence for the nine scenarios listed above. The
target cleans the Compose project and PostgreSQL volume. A green `make verify`
alone is not live workflow evidence, and the green two-binary gate must not be
generalized to unlisted terminal, retry-timeout, restart, replay, or recovery
scenarios.
