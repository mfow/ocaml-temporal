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

The initial live gate passed in Linux CI for commit `d4456b7`, covering the
fan-out and timer/activity cases. The current driver starts
`smoke.fan_out`, `smoke.timer_then_activity`, `smoke.activity_retry`, and
`smoke.parent_awaits_child` before it waits for any result. The parent invokes
`Temporal.Child_workflow.execute`; its registered child waits on a durable
timer, and the driver asserts `SMOKE:CHILD` from the parent. The retry workflow
uses a policy with two attempts: its activity deliberately fails once and the
driver requires the exact `SMOKE:ATTEMPT:2` result, which proves a second
activity task is delivered by the live server/Core path. The
fifth workflow returns a deterministic `Workflow` error with
`non_retryable=true`; the driver checks that typed terminal outcome instead of
treating every workflow as a success. The
retry-policy constructor, JSON representation, and Core conversion remain
separately covered by synthetic tests. Linux CI run
[`29187733405`](https://github.com/mfow/ocaml-temporal/actions/runs/29187733405)
for commit `b895d3c` passed the dedicated Temporal/PostgreSQL integration job,
so that run is the live evidence for this retry delivery.
That run provides evidence for these concrete happy paths only. It does not
provide live proof of retry timeouts, activity-level non-retryable error-type
classification, cancellation, replay, or recovery behavior.

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
Rust protocol tests. The live driver does not yet issue a cancellation request;
that is deliberately a separate acceptance scenario because it must keep an
execution outstanding while exercising shutdown and terminal observation.

## Coverage matrix

| Capability or scenario | Current local evidence | Real server evidence today | Remaining live boundary |
| --- | --- | --- | --- |
| Typed workflow/activity definitions, helper composition, and codecs | **Verified (synthetic only).** `make test-unit`; [`test/unit/test_definition.ml`](../../test/unit/test_definition.ml), [`test/unit/test_codec.ml`](../../test/unit/test_codec.ml), and [`test/unit/test_workflow_authoring.ml`](../../test/unit/test_workflow_authoring.ml). | **Verified (live success path).** The worker registers ordinary OCaml definitions and the driver decodes their typed results. | Failure and codec-rejection cases need dedicated live scenarios. |
| Direct-style workflow execution, suspension, futures, and deterministic replay | **Verified (synthetic only).** `make test-runtime`; [`test/runtime/test_scheduler.ml`](../../test/runtime/test_scheduler.ml), [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml), and native execution tests. | **Verified (live success path).** Timer, activity, and child waits suspend and later resume smoke workflows through real polling and completion. | Replay and unusual scheduling paths remain synthetic-only. |
| OCaml/Rust JSON bridge validation and payload boundaries | **Verified (synthetic only).** `make test-bridge`; OCaml protocol tests under [`test/bridge/`](../../test/bridge/) and Rust protocol tests under [`rust/core-bridge/tests/`](../../rust/core-bridge/tests/) validate closed records, ownership, rejection, and normalization. | **Verified (live success path).** Happy-path client, workflow, and activity records cross both public processes. | Malformed and uncommon record variants need dedicated live fault injection if they become supported scenarios. |
| PostgreSQL, Temporal Server, namespace, and Core lifecycle | Focused supervisor/bridge lifecycle tests cover invalid and repeated transitions. | **Verified (live success path).** The fixture starts the stack, waits for health, runs [`test/integration/test_core_lifecycle.ml`](../../test/integration/test_core_lifecycle.ml), and cleans the project. | Upgrade, persistence, and production-topology coverage are separate concerns. |
| A workflow starts, runs, and returns a terminal result | Synthetic activation and native adapter tests cover command construction and terminal handling. | **Verified (live success and typed-failure paths).** The driver retains exact workflow/run handles and waits through `Temporal.Client`; five top-level workflows are started before the first wait. Four return exact payloads, and `smoke.non_retryable_failure` returns a typed non-retryable workflow failure. | Cancelled, timed-out, terminated, and continued-as-new outcomes remain untested live. |
| Durable timers and wake-up | **Verified (synthetic only).** [`test/runtime/test_activation.ml`](../../test/runtime/test_activation.ml) covers zero-duration behavior, timer scheduling, and timer resolution. | **Verified (live success path).** `smoke.timer_then_activity` and the child in `smoke.parent_awaits_child` each wait for a short durable timer before returning. | Timer cancellation, unusual durations, and replay need separate live assertions. |
| Remote activity task polling and completion | **Verified (synthetic only).** [`test/runtime/test_native_activity_execution.ml`](../../test/runtime/test_native_activity_execution.ml), worker execution tests, and activity bridge tests cover typed task/completion conversion and lease retention. | **Verified for the live success path, including one retry.** `smoke.mock_transform` and `smoke.retry_once` both cross the real worker poll/completion path; CI run [`29187733405`](https://github.com/mfow/ocaml-temporal/actions/runs/29187733405) confirms the retry activity succeeds on its second attempt. | Activity timeout, heartbeat, asynchronous completion, and non-retryable failure are **Planned — later expansion**. |
| Activity retry policy and retry delivery | **Verified (synthetic only).** [`test/unit/test_activity_retry_policy.ml`](../../test/unit/test_activity_retry_policy.ml), [`test/runtime/test_activity_retry_policy.ml`](../../test/runtime/test_activity_retry_policy.ml), OCaml/Rust protocol tests, and Core conversion tests validate the immutable policy, exact coefficient bits, and malformed-input rejection without a server. | **Verified (one live success-after-retry path).** `smoke.activity_retry` schedules `smoke.retry_once` with an explicit two-attempt policy; the first implementation call returns a retryable activity failure, the second returns `SMOKE:ATTEMPT:2`, and the one-shot driver asserts that exact result in CI run [`29187733405`](https://github.com/mfow/ocaml-temporal/actions/runs/29187733405). | Retry backoff under larger intervals, non-retryable error-type matching, timeout-triggered retries, and retry behavior across worker restart remain **Planned — later expansion**. |
| Multiple operations scheduled before awaiting | **Verified (synthetic only).** Scheduler and activation tests cover completion ordering, first-error behavior, and cancellation semantics. | **Verified (live success path).** `smoke.fan_out` creates two mock-activity futures before its first wait and checks the ordered combined result. | `race`, cancellation, and explicit server-history ordering assertions are **Planned — later expansion**. |
| Typed workflow failures and non-success terminal outcomes | **Verified (synthetic and one live workflow-failure path).** Error, protocol, client, worker, and runtime tests check typed rejection and terminal state handling without exceptions for expected failures. | **Verified (one live non-retryable workflow failure).** `smoke.non_retryable_failure` returns a deterministic `Temporal.Error.t` with category `Workflow`, `non_retryable=true`, and the stable message prefix `intentional terminal workflow failure`; the driver requires that exact typed classification. | Live cancellation, timeout, termination, continued-as-new, and child-failure outcomes remain **Planned — later expansion**. |
| Exact-run client cancellation and graceful shutdown with outstanding work | **Verified (synthetic only).** The public mock client, supervisor, OCaml bridge protocol, and Rust protocol tests validate exact run identity, bounded request handling, positive acknowledgement, stable request IDs, and typed `Cancelled` observation. | The live gate performs clean shutdown after its successful and typed-failure work completes but does not yet cancel an outstanding execution. | Issue cancellation for a still-running execution, observe its `Cancelled` result, and then prove graceful teardown: **Planned — later expansion.** |
| Child-workflow start, acknowledgement, and terminal resolution | **Verified (synthetic only).** Focused Rust and OCaml tests cover ordered start/resolution, failures, duplicate sequences, and lease retirement. | **Verified (live success path).** `smoke.parent_awaits_child` calls `Temporal.Child_workflow.execute`; its registered child waits on a timer, and the driver asserts the parent's exact `SMOKE:CHILD` result. | Child start failure, cancellation, retry, non-success terminal outcomes, replay, and recovery remain **Planned — later expansion**. |
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
worker and one-shot assertion driver, asserts terminal results (including the
retry attempt marker and typed non-retryable failure), prints useful failure
logs, and cleans the Compose
project and PostgreSQL volume. A green `make verify` alone is not live workflow
evidence, and the green two-binary gate must not be generalized to unlisted
terminal, child failure/cancellation, retry timeout, or recovery scenarios.
