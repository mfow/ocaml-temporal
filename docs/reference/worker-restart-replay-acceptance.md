# Worker restart and replay acceptance design

**Status: design only.** This document describes the next real-Temporal
acceptance scenario. It does not claim that worker restart, replay, or sticky
cache behavior has been verified yet. The existing
[`make test-temporal-integration`](../../Makefile) gate remains the source of
the current live evidence; this design must be implemented and pass in CI
before the coverage matrix is changed.

## What this scenario proves

The current two-process smoke test proves that a worker can execute a workflow
to completion while it remains alive. The next scenario must prove a narrower
recovery contract:

1. an OCaml test-driver starts one workflow through `Temporal.Client` and waits
   for that exact workflow/run pair;
2. the OCaml worker receives the initial activation and records a durable timer
   before it is stopped;
3. a fresh instance of the same OCaml worker is started with the same
   namespace and task queue;
4. Temporal/Core delivers the pending execution to the new worker, which
   replays the recorded history, waits for the timer, runs the follow-up mock
   activity, and completes the workflow; and
5. the one-shot driver asserts the exact terminal payload and exits nonzero if
   any result, terminal class, or timeout is unexpected.

This is a worker-SDK test, not a client-only test. A worker process that merely
starts, or a driver that merely receives a workflow ID, is never a pass.

## Roles remain asymmetric

The restart test keeps the topology used by the existing acceptance fixture.

| Process | Responsibility | What it must not do |
| --- | --- | --- |
| `smoke-driver` | One-shot OCaml test runner. Starts the workflow, waits for its exact run, and checks the expected terminal result. | Register a worker, poll task queues, execute workflow code, or manufacture a success value. |
| `smoke-worker` generation 1 | Long-lived OCaml Temporal worker. Registers the workflow/activity definitions and processes the initial activation. | Start the test workflow or decide whether the assertion passed. |
| `smoke-worker` generation 2 | The replacement long-lived OCaml worker. Uses the same registration, namespace, task queue, and library, but a new process and native Core graph. | Reuse generation 1's native handles or process-local continuations. |
| Compose/Make controller | Orchestrates the stop/start boundary and collects diagnostic evidence. | Produce workflow results, bypass the public OCaml API, or act as a second worker. |

The controller may run `docker compose stop`/`up` and use a fixture-only
Temporal history query to know when the first timer has been recorded. It is
not an application worker and does not change the user's rule that the OCaml
container which starts workflows is just a test runner asserting a result.

## Proposed workflow

The fixture should add a deterministic workflow named
`smoke.worker_restart_replay`. Its body should contain only replay-safe SDK
operations and should be equivalent to:

```ocaml
Temporal.Workflow.define ~name:"smoke.worker_restart_replay"
  ~input:Temporal.Codec.string ~output:Temporal.Codec.string
  (fun seed ->
    match Temporal.Workflow.sleep (Temporal.Duration.of_ms 5_000L) with
    | Error error -> Error error
    | Ok () ->
        Temporal.Activity.execute restart_transform
          (seed ^ ":after-replay"))
```

The exact API spelling may change with the public workflow surface, but the
behavioral contract must remain:

- the initial workflow task emits one durable timer before the worker is
  stopped;
- the timer is long enough that the controller can observe `TimerStarted` and
  stop generation 1 without racing the timer firing;
- the follow-up activity is not scheduled until the timer fires, so its live
  task must be handled by generation 2; and
- the activity returns a deterministic value derived only from its input. The
  worker generation may be written to a diagnostic log by the activity, but it
  must never be included in the workflow result or used to choose commands.

For input `smoke`, the driver must require the exact completed payload
`SMOKE:AFTER-REPLAY`. It must reject failed, cancelled, terminated, timed-out,
continued-as-new, or differently encoded results through the existing typed
`result` path. The fixed workflow ID should be
`two-binary-worker-restart-replay`; the driver must retain and wait on the run
ID returned by `Client.start` rather than looking up a later run by workflow ID.

## Synchronization protocol

The restart boundary needs evidence that generation 1 actually committed the
timer. A fixed sleep in the controller is not sufficient: it can stop the
worker before the first workflow task, or after the timer has already fired.
The implementation should use this bounded sequence:

1. `test-temporal-worker-restart` invokes the same clean-stack setup as the
   existing integration target. `temporal-clean` runs before startup and in an
   exit trap.
2. The controller starts Temporal/PostgreSQL, runs the normal lifecycle check,
   and starts generation 1 of `smoke-worker`.
3. The one-shot driver starts the named workflow and writes its existing
   metadata-only `accepted` phase (workflow ID and run ID, never payload
   bytes). It then waits normally; it must not exit while the workflow is
   pending.
4. The controller waits for a stable history observation showing, in order,
   `WorkflowExecutionStarted`, the initial `WorkflowTaskCompleted`, and
   `TimerStarted` for the exact run ID. The query must use a machine-readable
   Temporal/admin-tools response or a fixture helper with an explicitly tested
   parser; scraping human-formatted output is not acceptable. A timeout here is
   a failed initial dispatch, not permission to guess that the timer exists.
5. The controller stops generation 1 and asserts that the container exits
   within its configured graceful-shutdown bound. It records the container ID
   and worker logs before replacing the container. The driver must still be
   waiting and the workflow must still be open.
6. The controller starts generation 2 with the same task queue and
   registrations, a fresh readiness marker, and a distinct diagnostic worker
   identity (for example, a generation suffix). It waits for
   `Worker.create` readiness before allowing the driver to finish.
7. The controller waits for a replay marker from generation 2 and for the
   follow-up activity marker. Only then does it collect the driver's exit code.
   The marker is diagnostic evidence; the driver's exact result remains the
   acceptance oracle.
8. On success, the controller verifies the ordered history and the exact
   driver assertion, then runs `temporal-clean`. On failure, it first preserves
   the driver output, both worker generations' bounded logs, and the workflow
   history metadata, then removes the containers and PostgreSQL volume.

The driver remains one process and one assertion runner throughout this
sequence. Restarting the worker is external lifecycle orchestration, not a
second OCaml worker hidden inside the test client.

## Replay evidence and limits

The internal semantic activation already retains Core's `is_replaying` bit,
history length, run ID, and cache-removal metadata. The future fixture should
emit a privacy-safe diagnostic record when it receives the post-restart
activation, containing only:

```text
phase=replay
generation=2
workflow_id=two-binary-worker-restart-replay
run_id=<exact-started-run-id>
is_replaying=true
history_length=<positive-value>
```

The initial activation must record `is_replaying=false`; the post-restart
activation for the same run must record `is_replaying=true`. Payload bytes,
workflow inputs, and activity output must not be written to logs. If the
worker cannot provide this marker, a passing terminal result may be reported
as **worker restarted and continued**, but it must not be presented as live
proof of replay. The coverage document should stay red for replay until the
marker and its focused tests exist.

A worker restart is not the same thing as a Temporal `RemoveFromCache`
activation. Losing an in-memory sticky execution while replacing a worker is
expected; it does not prove that the server sent an explicit cache-eviction
job or that the OCaml eviction acknowledgement path is correct. Sticky-cache
eviction therefore remains a separate scenario requiring an observed
`remove_from_cache` activation and an empty completion. This design must not
fold that claim into the restart result.

## Exact pass criteria

The acceptance command may exit zero only when all of the following are true:

- the stack passed PostgreSQL schema, Temporal frontend, namespace, and Core
  lifecycle readiness checks;
- generation 1 became healthy and the exact driver run was accepted;
- the exact run's history contained `TimerStarted` before generation 1 stopped;
- generation 1 stopped through the public worker shutdown path within the
  grace period, with no second worker overlapping it on the same test queue;
- generation 2 became healthy and emitted the same run ID with
  `is_replaying=true` before the timer-result activity completed;
- the history contains the ordered timer firing, activity scheduling and
  completion, and workflow completion events for that run; event IDs may vary,
  but their order and run identity must not;
- `smoke-driver` returned zero only after `Client.wait` reported
  `Completed` for that exact run and its decoded payload equaled
  `SMOKE:AFTER-REPLAY`; and
- teardown removed the Compose project and its PostgreSQL data volume.

Any missing marker, wrong run ID, unexpected terminal variant, activity
failure, timeout, stale worker readiness file, or retained volume is a failed
test. A green worker health check alone is never sufficient evidence.

## Failure evidence

The failure trap should print bounded, metadata-only evidence before cleanup:

- the driver's phase log, including start/wait boundaries, run ID, terminal
  class, typed error kind, and latency;
- generation 1 and generation 2 worker phase logs, worker identities, process
  exit status, and replay markers;
- the machine-readable history summary for the exact workflow/run pair;
- `docker compose ps` and the last 200 lines of PostgreSQL, Temporal Server,
  schema, and worker logs; and
- a check that the driver was still waiting when generation 1 stopped.

The history summary should distinguish at least these failures without
guessing:

| Evidence | Likely boundary |
| --- | --- |
| No `TimerStarted` before stop | Initial worker dispatch or completion failed. |
| Timer exists, but generation 1 does not stop | Worker shutdown/drain or native wait lifecycle bug. |
| Generation 2 is healthy, but no `is_replaying=true` marker | Replacement worker did not receive/replay the pending execution, or replay instrumentation is missing. |
| Replay marker exists, but no activity completion | Timer resolution, activity scheduling, or generation-2 dispatch failed. |
| Activity completes, but driver reports a non-completed/mismatched result | Workflow determinism, payload decoding, or exact-run waiting failed. |
| History belongs to another run or is missing | Driver/run identity or server persistence/query synchronization failed. |
| Cleanup leaves a PostgreSQL volume | Fixture lifecycle is incorrect; the result must not be accepted. |

Do not delete the diagnostic history before these checks run. Cleanup still
must remove the named volume in both success and failure paths; preserving the
volume for a later run would make replay evidence non-repeatable.

## Determinism and ownership constraints

- Workflow code may use only replay-safe SDK operations. It must not read
  environment variables, wall-clock time, randomness, files, sockets, or
  process-global mutable state to select a command or result.
- The activity may record a generation marker for diagnostics because activity
  code is outside deterministic workflow replay, but its returned payload must
  depend only on its typed input.
- Generation 2 must construct a new public `Temporal.Worker` and new private
  supervisor/Core graph. No Rust handle, OCaml continuation, or workflow
  scheduler may cross the process boundary.
- The driver owns only its client and exact-run handles. It must shut down the
  client after the assertion even when a typed failure occurs.
- The worker stop path must use `Temporal.Worker.shutdown`; sending SIGKILL or
  deleting a live container before graceful shutdown would test process loss,
  not the SDK's lifecycle contract. A separate crash-recovery scenario may
  deliberately use an ungraceful stop later.

## Implementation sequence

The implementation should land as a separate acceptance slice, in this order:

1. Add the deterministic workflow and a test-only activity diagnostic marker;
   add unit/runtime tests proving that the workflow emits the same timer and
   activity commands when replayed with identical activation history.
2. Add a bounded, machine-readable history synchronizer and generation-aware
   worker readiness/replay markers to the fixture. Keep all payload logging
   disabled.
3. Add a dedicated Make target and CI job (one supported OCaml version) that
   starts the driver in the background, performs the controlled worker
   replacement, waits for the exact assertion, and always removes the
   PostgreSQL volume.
4. Run the acceptance repeatedly from a clean stack, including a failure-path
   run that proves diagnostics appear before cleanup. Verify the target on both
   Linux `amd64` and `arm64` when the existing live job's platform policy
   permits it; native Windows/macOS jobs must not run this Linux Compose test.
5. Only after a green CI run, update
   [`live-acceptance-coverage.md`](live-acceptance-coverage.md),
   [`feature-coverage.md`](feature-coverage.md), and `docs/progress.md` with
   the commit and run URL. Until then, leave restart/replay marked planned.

This sequencing keeps a passing terminal result from being mistaken for
replay evidence and keeps the current one-shot driver/long-lived-worker
contract intact.
