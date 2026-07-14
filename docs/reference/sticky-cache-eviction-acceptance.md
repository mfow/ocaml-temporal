# Sticky-cache eviction acceptance design

**Status: implemented and contract-tested; live GitHub Actions verification is
pending.** This document describes a narrowly scoped real-server acceptance
test for Temporal Core's `CacheFull` cache-removal path. It is intentionally
separate from the worker restart/replay test: replacing a process loses its
memory, whereas a `CacheFull` removal is a command that Temporal Core sends to
a running worker and that the worker must acknowledge correctly.

Run the fast, Docker-free protocol check with:

```sh
make test-temporal-worker-cache-eviction-contract
```

Run the full PostgreSQL, Temporal Server, and two-OCaml-binary test with:

```sh
make test-temporal-worker-cache-eviction
```

The second command removes its Compose project and PostgreSQL volume both
before startup and on every exit path. It is therefore a disposable acceptance
fixture, not an example of how an application should manage a production
Temporal database.

## What a sticky cache is

A Temporal worker keeps recent workflow executions in a local *sticky cache*.
That cache holds the language runtime's in-memory workflow state between
workflow tasks. It is not Temporal Server state and it is not a workflow's
durable history. If a worker needs to make room for another execution, Core can
send a `RemoveFromCache` activation with reason `CacheFull` for a cached run.

The OCaml runtime must then do two things in the right order:

1. discard the old in-memory scheduler, continuations, and pending workflow
   state for that run; and
2. send an **empty** completion for the removal activation and retain all
   ownership until Core accepts that completion.

The server can later deliver that same run again. Its history then creates a
fresh OCaml scheduler, rather than resuming the discarded continuations. This
is a key replay and memory-lifetime boundary: reusing the old scheduler would
both violate replay safety and risk retaining workflow state after eviction.

## Fixture topology

The scenario deliberately uses two OCaml executables, both linked against the
same public `temporal-sdk` library.

| Component | Role | It does not do |
| --- | --- | --- |
| `smoke-cache-eviction-worker` | A dedicated public `Temporal.Worker` with `max_cached_workflows = 1`. It registers only `smoke.sticky_cache_eviction`. | Start test workflows or decide whether the test passed. |
| `smoke-cache-eviction-driver` | A one-shot public `Temporal.Client`. It starts the target run, waits for the controller release, starts a pressure run, then waits for the original exact run and checks its result. | Register a worker, poll a task queue, or execute workflow code. |
| Shell controller | Starts/stops Compose services, queries machine-readable Temporal history, releases cache pressure only at the timer boundary, and validates payload-free evidence. | Manufacture a workflow result or acknowledge worker activations. |
| Temporal Server + PostgreSQL | Persist durable execution history and deliver workflow tasks. | Know about the fixture's diagnostic files. |

Keeping the worker separate from the driver matters. A process that starts and
also executes the workflow could appear to pass without proving that a public
OCaml client and a public OCaml worker communicate through the real server.

## Exact sequence

The workflow body is deliberately small and deterministic:

```ocaml
Temporal.Workflow.define ~name:"smoke.sticky_cache_eviction"
  ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
    match Temporal.Workflow.sleep (Temporal.Duration.of_ms 20_000L) with
    | Error error -> Error error
    | Ok () -> Ok ("SMOKE:CACHE:EVICTION:" ^ String.uppercase_ascii seed))
```

It uses a durable Temporal timer rather than wall-clock I/O. The controller
uses this ordered sequence:

1. Start a fresh Temporal/PostgreSQL stack and the dedicated one-entry worker.
2. The OCaml driver starts target workflow
   `two-binary-sticky-cache-eviction-target` with input `target`, writes only
   its workflow ID and returned run ID to an atomic marker, and remains waiting
   on that exact handle.
3. The controller confirms the exact run through `temporal workflow describe`
   and normalizes `temporal workflow show --output json`. Before it releases
   pressure, history must contain `TimerStarted` but neither `TimerFired` nor
   a terminal workflow event.
4. The controller atomically writes `release`. The driver starts a second
   `-pressure` workflow on the same one-entry worker cache and records only
   its own identity marker.
5. Core removes the target's sticky state with `RemoveFromCache(CacheFull)`.
   The private adapter retires the empty completion lease, and only after Core
   returns success does its private acknowledgement observer record the event.
6. Core delivers the target again. The fresh activation must be marked
   `is_replaying = true`; the old scheduler is never reused.
7. The target timer fires and the driver checks the exact result
   `SMOKE:CACHE:EVICTION:TARGET` through `Temporal.Client.wait` on the original
   handle. The controller then validates terminal history and shuts the worker
   down through the public worker API before deleting all Compose volumes.

The target's timer deliberately leaves enough time for the history query and
pressure start. It is not an assertion based on sleeping in the controller:
the controller refuses to release pressure until it has observed the durable
`TimerStarted` history boundary.

## Evidence document

The worker writes one private, payload-free JSON file only when the dedicated
test environment variables are set. Normal applications do not allocate this
observer or perform its file I/O. Its schema is
[`cache-eviction-diagnostics.schema.json`](../schemas/acceptance/cache-eviction-diagnostics.schema.json).

For one target run it must have exactly this shape:

```json
{
  "workflow_id": "two-binary-sticky-cache-eviction-target",
  "run_id": "<exact Temporal run ID>",
  "records": [
    {"phase": "initial", "is_replaying": false, "history_length": "..."},
    {"phase": "cache_full_acknowledged", "empty_completion": true},
    {"phase": "replay", "is_replaying": true, "history_length": "..."}
  ]
}
```

`history_length` is decimal text rather than a JSON number so tooling cannot
round a signed 64-bit Temporal count. The document intentionally excludes
workflow inputs, result payloads, task tokens, timestamps, server errors, and
native handle data.

The middle record is more meaningful than a generic eviction log. The
workflow adapter adds it only from its post-acceptance callback, after it has
successfully submitted the empty completion for the exact pending `CacheFull`
removal and retired the lease. If that submission fails and is retried, the
record is not written early. If it fails permanently, the test fails rather
than calling an attempted acknowledgement evidence.

## Pass conditions and limits

The live controller exits zero only when all of these are true:

- the dedicated worker became healthy after public `Worker.create`;
- the driver started the configured target and waited on its exact run ID;
- normalized initial history proves a pending timer for that exact run;
- the pressure workflow was started only after that boundary;
- the diagnostic has exactly initial, accepted-`CacheFull`, and replay records
  in that order for the same run;
- the replay history length is positive;
- the target's terminal history preserves the complete initial history prefix,
  then contains `TimerFired` and `WorkflowExecutionCompleted`; and
- the driver received exactly the deterministic target result and the cleanup
  removed the project volumes.

The schema validates each JSON record. The shell validator adds the
cross-document requirements that JSON Schema cannot express, including exact
workflow/run identity, history-prefix preservation, event ordering, and
absence of a fired timer in the initial snapshot. Its checked-in fixtures make
those assertions testable without Docker. Before the pressure workflow is
started, the controller invokes that same validator in its `initial` stage;
the fast contract exercises the identical pending-timer predicate so the
polling gate cannot silently diverge from the final evidence checks.

This does **not** prove crash recovery, worker restart, every cache-removal
reason, or arbitrary sticky-cache performance. Worker restart has its own
[acceptance design](worker-restart-replay-acceptance.md). This fixture proves
only the specific `CacheFull` acknowledgement and fresh replay path needed to
guard the OCaml worker's cache-eviction lifecycle.
