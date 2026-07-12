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
workflows: the fan-out and timer/activity cases. The current driver starts
nine workflows before it waits for any result: `smoke.fan_out`,
`smoke.timer_then_activity`, `smoke.activity_retry`,
`smoke.activity_heartbeat_retry`,
`smoke.parent_awaits_child`, `smoke.parent_awaits_failed_child`,
`smoke.parent_cancels_child`, `smoke.non_retryable_failure`, and
`smoke.long_running_cancellation`. The parent invokes
`Temporal.Child_workflow.execute`; its registered child waits on a durable
timer, and the driver asserts `SMOKE:CHILD` from the parent. The retry workflow
uses a policy with two attempts: its activity deliberately fails once and the
driver requires the exact `SMOKE:ATTEMPT:2` result, which proves a second
activity task is delivered by the live server/Core path. The heartbeat workflow
uses a 500 ms heartbeat timeout. Its first `smoke.heartbeat_retry` attempt
records `SMOKE:HEARTBEAT:PROGRESS:1`, waits briefly for Core's asynchronous
heartbeat manager to flush the request, and returns a retryable activity
failure. The second attempt must receive that detail through
`Temporal.Activity.Context.details` and observe the same timeout through
`Context.heartbeat_timeout` before it can return
`SMOKE:HEARTBEAT:RETRIED:SMOKE`. This is a server-visible heartbeat and retry
assertion, not a worker-local attempt counter. No live heartbeat run has been
observed in this environment, so this scenario remains an implementation and
local-contract milestone until a green Compose execution is available. The
non-retryable-failure workflow returns a deterministic `Workflow` error with
`non_retryable=true`; the driver checks that typed terminal outcome instead of
treating every workflow as a success. The historical five-execution evidence is
CI run [`29191260073`](https://github.com/mfow/ocaml-temporal/actions/runs/29191260073)
for merge commit `a4eaccc8`; it verifies exactly these five baseline workflows:
`smoke.fan_out`, `smoke.timer_then_activity`, `smoke.activity_retry`,
`smoke.parent_awaits_child`, and `smoke.non_retryable_failure`. The current
nine-run driver adds `smoke.activity_heartbeat_retry`, the marker-guarded
`smoke.long_running_cancellation`, and two child lifecycle assertions.
`smoke.parent_awaits_failed_child` propagates a deterministic non-retryable
child failure and the driver checks the public `Child_workflow` error category.
`smoke.parent_cancels_child` uses an opaque child handle and
`Wait_cancellation_requested`, then returns an exact success marker only after
Core resolves the child cancellation. These additions are implemented and
covered by local acceptance checks, but they are not live-verified yet: the attempted
[Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29193818312)
was cancelled before producing a green result. The local driver waits for a
test-only marker activity after the timer and marker commands are issued
together, then cancels the exact run before waiting for typed `Cancelled`
metadata. Retry-policy construction, JSON representation, and Core conversion
remain separately covered by synthetic tests. Retry timeouts, activity-level
non-retryable error-type classification, replay, and recovery remain outside
the live gate.

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
These nine-run cancellation, child-lifecycle, and heartbeat/shutdown assertions are not live evidence yet;
the historical live result covers the five baseline executions, and the
attempted cancellation run was cancelled before producing a green result.

## Coverage matrix

| Capability or scenario | Current local evidence | Real server evidence today | Remaining live boundary |
| --- | --- | --- | --- |
| Typed workflow/activity definitions, helper composition, and codecs | **Verified (synthetic only).** `make test-unit`; [`test/unit/test_definition.ml`](../../test/unit/test_definition.ml), [`test/unit/test_codec.ml`](../../test/unit/test_codec.ml), and [`test/unit/test_workflow_authoring.ml`](../../test/unit/test_workflow_authoring.ml). | **Verified (live success path).** The worker registers ordinary OCaml definitions and the driver decodes their typed results. | Failure and codec-rejection cases need dedicated live scenarios. |
| Direct-style workflow execution, suspension, futures, and deterministic replay | **Verified (synthetic only).** `make test-runtime`; [`test/runtime/test_scheduler.ml`](../../test/runtime/test_scheduler.ml), [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml), and native execution tests. | **Verified for the live suspension path.** Timer, activity, and child waits suspend and later resume smoke workflows through real polling and completion; this path does not exercise deterministic replay. | Replay and unusual scheduling paths remain synthetic-only. |
| OCaml/Rust JSON bridge validation and payload boundaries | **Verified (synthetic only).** `make test-bridge`; OCaml protocol tests under [`test/bridge/`](../../test/bridge/) and Rust protocol tests under [`rust/core-bridge/tests/`](../../rust/core-bridge/tests/) validate closed records, ownership, rejection, and normalization. | **Verified (live success path).** Happy-path client, workflow, and activity records cross both public processes. | Malformed and uncommon record variants need dedicated live fault injection if they become supported scenarios. |
| PostgreSQL, Temporal Server, namespace, and Core lifecycle | Focused supervisor/bridge lifecycle tests cover invalid and repeated transitions. | **Verified (live success path).** The fixture starts the stack, waits for health, runs [`test/integration/test_core_lifecycle.ml`](../../test/integration/test_core_lifecycle.ml), and cleans the project. | Upgrade, persistence, and production-topology coverage are separate concerns. |
| A workflow starts, runs, and returns a terminal result | Synthetic activation and native adapter tests cover command construction and terminal handling. | **Historical:** five baseline executions were verified in CI run [`29191260073`](https://github.com/mfow/ocaml-temporal/actions/runs/29191260073). The current driver retains exact workflow/run handles and starts nine top-level workflows before the first wait, including heartbeat retry, child failure/cancellation, and the marker-guarded cancellation scenario. Those newly expanded paths are locally covered but not live-verified because the attempted [Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29193818312) was cancelled. | Timed-out, terminated, and continued-as-new outcomes remain untested live. |
| Durable timers and wake-up | **Verified (synthetic only).** [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml) covers zero-duration behavior, timer scheduling, and timer resolution. | **Verified (live success path).** `smoke.timer_then_activity` and the child in `smoke.parent_awaits_child` each wait for a short durable timer before returning. | Timer cancellation, unusual durations, and replay need separate live assertions. |
| Remote activity task polling and completion | **Verified (synthetic only).** [`test/runtime/test_native_activity_execution.ml`](../../test/runtime/test_native_activity_execution.ml), worker execution tests, and activity bridge tests cover typed task/completion conversion, lease retention, context-aware activity dispatch, prior heartbeat detail delivery, typed heartbeats, and context invalidation. | **Verified for the live success path, including one retry.** `smoke.mock_transform` and `smoke.retry_once` both cross the real worker poll/completion path; CI run [`29191260073`](https://github.com/mfow/ocaml-temporal/actions/runs/29191260073) confirms the retry activity succeeds on its second attempt. The new `smoke.activity_heartbeat_retry` scenario is implemented and locally contract-checked, but no live heartbeat run has completed in this environment. | Activity timeout, asynchronous completion, and non-retryable failure remain **Planned — later expansion**. |
| Activity retry policy and retry delivery | **Verified (synthetic only).** [`test/unit/test_activity_retry_policy.ml`](../../test/unit/test_activity_retry_policy.ml), [`test/runtime/test_activity_retry_policy.ml`](../../test/runtime/test_activity_retry_policy.ml), OCaml/Rust protocol tests, and Core conversion tests validate the immutable policy, exact coefficient bits, and malformed-input rejection without a server. | **Verified (one live success-after-retry path).** `smoke.activity_retry` schedules `smoke.retry_once` with an explicit two-attempt policy; the first implementation call returns a retryable activity failure, the second returns `SMOKE:ATTEMPT:2`, and the one-shot driver asserts that exact result in CI run [`29191260073`](https://github.com/mfow/ocaml-temporal/actions/runs/29191260073). The heartbeat scenario adds a second retry path whose completion depends on server-returned heartbeat details. | Retry backoff under larger intervals, non-retryable error-type matching, timeout-triggered retries, and retry behavior across worker restart remain **Planned — later expansion**. |
| Multiple operations scheduled before awaiting | **Verified (synthetic only).** Scheduler and activation tests cover completion ordering, first-error behavior, and cancellation semantics. | **Historical:** five baseline executions were verified in CI run [`29191260073`](https://github.com/mfow/ocaml-temporal/actions/runs/29191260073). `smoke.fan_out` creates two mock-activity futures before its first wait; the current nine-run driver also starts a timer and marker activity in one cancellation-workflow activation before waiting for the marker, starts the child-failure and child-cancellation parents before any result wait, and starts the heartbeat retry before any result wait. These expanded scenarios are locally covered but not live-verified because the attempted [Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29193818312) was cancelled. | `race` and explicit server-history ordering assertions are **Planned — later expansion**. |
| Typed workflow failures and non-success terminal outcomes | **Verified (synthetic and one live workflow-failure path).** Error, protocol, client, worker, and runtime tests check typed rejection and terminal state handling without exceptions for expected failures. | **Typed non-retryable workflow failure verified** in CI run [`29191260073`](https://github.com/mfow/ocaml-temporal/actions/runs/29191260073). The local nine-run assertion additionally requires the child-failure category, child-cancellation marker, and exact top-level cancellation/heartbeat metadata; those expanded assertions are not live-verified. | Timeout, termination, continued-as-new, and live child-failure outcomes remain **Planned — later expansion**. |
| Exact-run client cancellation and graceful shutdown with outstanding work | **Verified (synthetic only).** The public mock client, supervisor, OCaml bridge protocol, and Rust protocol tests validate exact run identity, bounded request handling, positive acknowledgement, stable request IDs, and typed `Cancelled` observation. The nine-run driver also has local marker and result assertions. | **Implemented, but not live-verified.** The local driver cancels `two-binary-long-running-cancellation`, waits for its exact run, requires category `Cancelled`, `non_retryable=false`, and the stable cancellation message, then checks the driver's client-shutdown marker and the worker's graceful-shutdown marker. The attempted [Actions run](https://github.com/mfow/ocaml-temporal/actions/runs/29193818312) was cancelled, so no green CI run currently proves this path against a live server. | Restart, replay, and cache eviction remain **Planned — later expansion**. |
| Activity heartbeat details and timeout propagation | **Verified (synthetic only).** Native activity execution, OCaml/Rust heartbeat protocol, lease retention, context lifetime, and bilateral validation are covered by focused local tests. The nested fixture contract checks that both binaries register and assert the context-aware heartbeat workflow. | **Implemented, but not live-verified.** `smoke.activity_heartbeat_retry` sends one progress detail with a 500 ms timeout, intentionally fails, then requires Temporal to return the detail and timeout on the second attempt. | A green Temporal/PostgreSQL run is required before this row becomes live evidence. Heartbeat-timeout-triggered retry, asynchronous completion, and worker restart remain later expansions. |
| Child-workflow start, acknowledgement, and terminal resolution | **Verified (synthetic only).** Focused Rust and OCaml tests cover ordered start/resolution, failures, duplicate sequences, and lease retirement. | **Implemented, but not live-verified beyond the historical success path.** `smoke.parent_awaits_child` calls `Temporal.Child_workflow.execute`; its registered child waits on a timer, and the driver asserts the parent's exact `SMOKE:CHILD` result. The local fixture additionally propagates a non-retryable child failure and performs a child-handle cancellation, checking the public child error category and exact cancellation marker. | A green Compose run is still required for child failure/cancellation; child start failure, retry, replay, and recovery remain **Planned — later expansion**. |
| Worker restart, replay, sticky-cache eviction, and continued execution | **Verified (synthetic only).** Runtime tests cover replay-stable commands and cache eviction; native tests cover retention and lease cleanup. | The live gate starts one worker and shuts it down after the smoke runs. | Restart a worker while executions are pending, then verify replay and eviction through terminal results: **Planned — later expansion.** |

## Stable evidence commands

These commands are the supported local gates for the corresponding matrix
rows:

```sh
make test-unit                 # definitions, codecs, public API, and errors
make test-runtime              # scheduler, activations, futures, replay
make test-bridge               # OCaml/Rust ABI and protocol fixtures
make verify                    # broad build, lint, Rust, bridge, and quality gates
make test-temporal-integration # real PostgreSQL/Temporal + two OCaml binaries
```

`make test-temporal-integration` is the only command in this list that starts
a real Temporal Server. It owns the fixture lifecycle, starts the independent
worker and one-shot assertion driver, and prints useful failure logs. The
historical live result covers the retry attempt marker and typed non-retryable
failure; the current nine-run cancellation, heartbeat, and child
failure/cancellation assertions are local-only until a green live run verifies
them. The target cleans the Compose project and PostgreSQL volume. A green
`make verify` alone is not live workflow evidence, and the local child
failure/cancellation assertions must not be generalized to live compatibility
until a green Compose run verifies them. The green two-binary gate must not be
generalized to unlisted terminal, retry-timeout, or recovery scenarios.
