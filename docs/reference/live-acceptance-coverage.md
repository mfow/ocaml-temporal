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
- **Implemented — live verification pending** means the Compose controller,
  worker instrumentation, and strict assertions are present, but no successful
  real-server run has yet been recorded for that scenario.

The initial live gate passed in Linux CI for commit `d4456b7`, covering two
workflows. The current driver starts fifteen workflows before it waits for any
result: `smoke.fan_out`, `smoke.timer_then_activity`,
`smoke.continue_as_new`, `smoke.activity_retry`,
`smoke.activity_heartbeat_retry`, `smoke.async_activity_completion`,
`smoke.parent_awaits_child`, `smoke.parent_awaits_failed_child`,
`smoke.parent_cancels_child`, `smoke.non_retryable_failure`,
`smoke.activity_non_retryable_failure`, `smoke.parent_retries_child`,
`smoke.long_running_cancellation`,
`smoke.parent_observes_child_start_failure`, and `smoke.signal_condition`. The
signal scenario starts a workflow that waits on a deterministic condition,
submits a typed signal to the exact run, and requires the worker-delivered
handler value in the terminal result. The signal workflow first completes a
worker-visible, per-run readiness activity; the driver waits for its exact
marker before submitting the signal. The child-start-failure parent starts
after the long-running cancellation execution is accepted, then requires
Temporal to reject its duplicate child ID with typed non-retryable metadata.
After the heartbeat result is terminal it starts
`smoke.activity_timeout_retry`, then starts
`smoke.activity_heartbeat_timeout_retry` after the start-to-close timeout
result. The [PR #289 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29333761719)
passed all seventeen exact results against Temporal Server 1.31 and PostgreSQL.
The
[PR #253 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29286560471)
passed the prior twelve assertions against Temporal Server 1.31 and PostgreSQL,
including the exact delayed asynchronous result, timeout retry, and
continue-as-new successor. The same run then passed the two-generation
continue-as-new successor. The [PR #266 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29310656994)
then passed all thirteen assertions, including the typed signal acknowledgement,
worker handler delivery, deterministic condition wake-up, and exact terminal
value. The PR #253 run also passed the two-generation restart/replay controller.
The historical [PR #210 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29221151859)
and [PR #226 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29224854182)
remain useful CI evidence for the earlier nine- and ten-scenario slices.

The signal scenario is now **Verified (live success path)** by PR #266. The
driver waits for the signal workflow's exact readiness marker before sending
the signal, so the green integration result demonstrates server delivery to a
worker-accepted execution and a deterministic condition wake-up rather than a
local callback or a driver-side shortcut. The handler value is stored in
`Temporal.Workflow_context.Local`, which keeps repeated workflow executions
from sharing mutable module state.

The earlier run live-verified four exact successes, a second activity task delivered
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

The Docker-free contracts remain useful for fast feedback:
`test/smoke/test_temporal_heartbeat_contract.sh` protects the heartbeat path,
and `test/smoke/test_temporal_activity_timeout_contract.sh` protects the
timeout-retry registration, start-before-wait ordering, and exact marker. The
`test/integration/temporal/common/marker_test/test_smoke_definitions.ml` test
checks copied heartbeat details, timeout propagation, and invalidated-context
rejection. Retry-policy construction, JSON representation, and Core conversion
remain separately covered by synthetic tests. The current seventeen-result
fixture also live-verifies heartbeat-timeout retry and activity-level
non-retryable error-type classification. The restart/replay controller is
implemented as `make test-temporal-worker-restart`; its contract and
real-server execution passed in the [PR #253 Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29286560471).
The controller's thirteen-step record, exact run identity, replay marker,
normalized history, and volume cleanup are now live evidence; sticky-cache
eviction and crash recovery remain separate scenarios.

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
the seventeen-result CI run is live evidence for the successful exact-run
cancellation path. Restart, replay, and cache-eviction behavior require
separate runs.

## Coverage matrix

| Capability or scenario | Current local evidence | Real server evidence today | Remaining live boundary |
| --- | --- | --- | --- |
| Typed workflow/activity definitions, helper composition, and codecs | **Verified (synthetic only).** `make test-unit`; [`test/unit/test_definition.ml`](../../test/unit/test_definition.ml), [`test/unit/test_codec.ml`](../../test/unit/test_codec.ml), and [`test/unit/test_workflow_authoring.ml`](../../test/unit/test_workflow_authoring.ml). | **Verified (live success path).** The worker registers ordinary OCaml definitions and the driver decodes their typed results. | Failure and codec-rejection cases need dedicated live scenarios. |
| Direct-style workflow execution, suspension, futures, and deterministic replay | **Verified (synthetic only).** `make test-runtime`; [`test/runtime/test_scheduler.ml`](../../test/runtime/test_scheduler.ml), [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml), and native execution tests. | **Verified for the live suspension path.** Timer, activity, and child waits suspend and later resume smoke workflows through real polling and completion; this path does not exercise deterministic replay. | Replay and unusual scheduling paths remain synthetic-only. |
| OCaml/Rust JSON bridge validation and payload boundaries | **Verified (synthetic only).** `make test-bridge`; OCaml protocol tests under [`test/bridge/`](../../test/bridge/) and Rust protocol tests under [`rust/core-bridge/tests/`](../../rust/core-bridge/tests/) validate closed records, ownership, rejection, and normalization. | **Verified (live success path).** Happy-path client, workflow, and activity records cross both public processes. | Malformed and uncommon record variants need dedicated live fault injection if they become supported scenarios. |
| PostgreSQL, Temporal Server, namespace, and Core lifecycle | Focused supervisor/bridge lifecycle tests cover invalid and repeated transitions. | **Verified (live success path).** The fixture starts the stack, waits for health, runs [`test/integration/test_core_lifecycle.ml`](../../test/integration/test_core_lifecycle.ml), and cleans the project. | Upgrade, persistence, and production-topology coverage are separate concerns. |
| A workflow starts, runs, and returns a terminal result | Synthetic activation and native adapter tests cover command construction and terminal handling. | **Verified (live success path).** The OCaml 5.5 Compose run starts fifteen workflows before the first wait, waits for the signal workflow's worker-visible readiness marker before signaling it, starts the two timeout-retry workflows in serialized order, and asserts all seventeen exact results or typed terminal outcomes, including continue-as-new. | Terminated outcomes remain pending. |
| Durable timers and wake-up | **Verified (synthetic only).** [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml) covers zero-duration behavior, timer scheduling, and timer resolution. | **Verified (live success path).** `smoke.timer_then_activity` and the child in `smoke.parent_awaits_child` each wait for a short durable timer before returning. | Timer cancellation, unusual durations, and replay need separate live assertions. |
| Remote activity task polling and completion | **Verified (synthetic only).** [`test/runtime/test_native_activity_execution.ml`](../../test/runtime/test_native_activity_execution.ml), worker execution tests, and activity bridge tests cover typed task/completion conversion, lease retention, context-aware activity dispatch, prior heartbeat detail delivery, typed heartbeats, asynchronous lease retention, terminal lease retirement, and context invalidation. | **Verified live for ordinary, heartbeat-detail, start-to-close-timeout, heartbeat-timeout, non-retryable, and delayed asynchronous completion paths** in [PR #279](https://github.com/mfow/ocaml-temporal/actions/runs/29329420364), with earlier runs providing evidence for the original slices. | Retry backoff under larger intervals and retry behavior across worker restart remain **Planned — later expansion**. |
| Activity retry policy and retry delivery | **Verified (synthetic only).** [`test/unit/test_activity_retry_policy.ml`](../../test/unit/test_activity_retry_policy.ml), [`test/runtime/test_activity_retry_policy.ml`](../../test/runtime/test_activity_retry_policy.ml), OCaml/Rust protocol tests, and Core conversion tests validate the immutable policy, exact coefficient bits, and malformed-input rejection without a server. | **Verified live for ordinary, heartbeat-detail, start-to-close-timeout, heartbeat-timeout, and non-retryable retry delivery.** The seventeen-result smoke requires the exact second-attempt markers and the activity policy's non-retryable classification in [PR #289](https://github.com/mfow/ocaml-temporal/actions/runs/29333761719). | Retry backoff under larger intervals and retry behavior across worker restart remain **Planned — later expansion**. |
| Multiple operations scheduled before awaiting | **Verified (synthetic only).** Scheduler and activation tests cover completion ordering, first-error behavior, and cancellation semantics. | **Verified live.** `smoke.fan_out` schedules two activities before its first wait; the cancellation workflow schedules its timer and marker activity before waiting; and the driver starts fifteen top-level workflows before awaiting any result. It waits for the signal workflow's readiness marker before signaling, starts the child-start-failure parent after the conflicting run is accepted, then starts the two timeout workflows in serialized order and asserts all seventeen results. | `race` and explicit server-history ordering assertions are **Planned — later expansion**. |
| Typed workflow failures and non-success terminal outcomes | **Verified (synthetic and live failure paths).** Error, protocol, client, worker, and runtime tests check typed rejection and terminal state handling without exceptions for expected failures. | **Verified (live success and failure paths).** The [PR #289 run](https://github.com/mfow/ocaml-temporal/actions/runs/29333761719) checks a typed non-retryable top-level workflow failure, propagated non-retryable child failure, duplicate-ID child start failure, child cancellation marker, and exact top-level cancellation metadata. | Timeout and termination remain **Planned — later expansion**; continued-as-new is live-verified separately. |
| Exact-run client cancellation and graceful shutdown with outstanding work | **Verified (synthetic only).** The public mock client, supervisor, OCaml bridge protocol, and Rust protocol tests validate exact run identity, bounded request handling, positive acknowledgement, stable request IDs, and typed `Cancelled` observation. The seventeen-result driver also has live marker and result assertions. | **Verified live** in the [PR #289 run](https://github.com/mfow/ocaml-temporal/actions/runs/29333761719). The driver cancels `two-binary-long-running-cancellation`, waits for its exact run, checks category `Cancelled` and `non_retryable=false`, and checks the driver's and worker's graceful-shutdown markers. | Restart/replay is covered by the separate live controller; cache eviction remains **Planned — later expansion**. |
| Activity heartbeat details and timeout propagation | **Verified (synthetic only).** Native activity execution, OCaml/Rust heartbeat protocol, lease retention, context lifetime, and bilateral validation are covered by focused local tests. The Docker-free [`test_temporal_heartbeat_contract.sh`](../../test/smoke/test_temporal_heartbeat_contract.sh) protects the two-process registration, start-before-wait, result assertion, and cleanup shape. [`test_smoke_definitions.ml`](../../test/integration/temporal/common/marker_test/test_smoke_definitions.ml) invokes the exact shared contextual activity twice and checks copied details, timeout propagation, and invalidated-context rejection. | **Verified live** in [PR #279](https://github.com/mfow/ocaml-temporal/actions/runs/29329420364). `smoke.activity_heartbeat_retry` preserves the first attempt's detail, while `smoke.activity_heartbeat_timeout_retry` requires the server-managed second attempt after heartbeats stop. | No heartbeat-specific live boundary remains in this fixture; worker restart and recovery remain separate. |
| Child-workflow start, acknowledgement, and terminal resolution | **Verified (synthetic only).** Focused Rust and OCaml tests cover ordered start/resolution, failures, duplicate sequences, and lease retirement. | **Verified live for success, propagated failure, cancellation, retry, and duplicate-ID start failure.** The [PR #289 run](https://github.com/mfow/ocaml-temporal/actions/runs/29333761719) requires `smoke.parent_retries_child` to return `SMOKE:CHILD_RETRY:ATTEMPT:2` and `smoke.parent_observes_child_start_failure` to return `SMOKE:CHILD:START_FAILED`; the latter proves Temporal rejects a child start that conflicts with an accepted top-level workflow ID and that the typed error crosses the bridge. | Child replay and recovery remain **Planned — later expansion**. |
| Worker restart, replay, sticky-cache eviction, and continued execution | **Verified (live restart/replay path).** Runtime tests cover replay-stable commands and cache eviction; native activation diagnostics, strict history normalization, and the ordered controller contract pass locally. | **Verified live** in the [PR #253 run](https://github.com/mfow/ocaml-temporal/actions/runs/29286560471). `make test-temporal-worker-restart-live` started the real stack, replaced generation 1 with a fresh generation 2, checked the exact run and replay marker, validated the ordered history and thirteen controller steps, and removed the PostgreSQL volume. | Sticky-cache eviction still needs a separate live `RemoveFromCache` scenario; crash recovery remains separate. |

## Stable evidence commands

These commands are the supported local gates for the corresponding matrix
rows:

```sh
make test-unit                 # definitions, codecs, public API, and errors
make test-runtime              # scheduler, activations, futures, replay
make test-bridge               # OCaml/Rust ABI and protocol fixtures
make verify                    # broad build, lint, Rust, bridge, and repository contracts
make test-temporal-integration # real PostgreSQL/Temporal + two OCaml binaries
make test-temporal-worker-restart # contract plus two-generation live restart/replay
```

Both `make test-temporal-integration` and `make test-temporal-worker-restart`
start a real Temporal Server. The former owns the seventeen-result baseline; the
latter owns the two-generation restart/replay sequence and its Docker-free
contract. Each target owns the fixture lifecycle, starts the independent
worker and one-shot assertion driver, and removes the PostgreSQL volume. A
green `make verify` alone is not live workflow evidence, and the baseline
two-binary gate must not be generalized to unlisted terminal, cache-eviction,
or recovery scenarios. The [PR #253 run](https://github.com/mfow/ocaml-temporal/actions/runs/29286560471)
is the restart/replay evidence, while [PR #289](https://github.com/mfow/ocaml-temporal/actions/runs/29333761719)
is the current complete CI evidence for the seventeen-result baseline.
