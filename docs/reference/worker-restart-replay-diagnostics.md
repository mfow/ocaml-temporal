# Worker restart/replay diagnostic contract

**Status: offline contract only.** This document defines the small,
payload-free records that the future real-Temporal restart test will use to
coordinate a worker replacement. The current repository does not yet expose
the worker activation callback needed to produce a live `is_replaying=true`
record, and it does not yet have a machine-readable Temporal history adapter.
`make test-temporal-worker-restart` therefore validates fixtures and rejection
paths only. A passing command here is not evidence that a worker restarted or
that Temporal replayed an execution.

## Why there is a normalized document

The Temporal CLI's normal history output is intended for people, not a stable
controller protocol. A controller must never grep that display text to decide
whether it is safe to stop generation 1. Instead, a future small adapter (for
example, a Temporal API client in the admin-tools image) will convert the
server response into the normalized JSON document described by
[`restart-replay-history.schema.json`](../schemas/acceptance/restart-replay-history.schema.json).
The adapter must preserve the original event IDs and order, omit payload bytes,
and reject duplicate or unknown JSON fields before invoking the validator.

The normalized history document contains only the workflow ID, the exact run ID
returned by `Client.start`, and a bounded list of event IDs and event type
names. It is a projection, not a replacement for Temporal history: event
attributes are intentionally excluded because the acceptance test only needs
ordering and identity at this synchronization boundary. Event IDs are
canonical decimal strings rather than JSON numbers. This avoids the precision
loss that JavaScript-style JSON parsers introduce for Temporal's signed 64-bit
values.

The separate worker diagnostics document (`diagnostics.json`) carries each
generation's `history_length`, also as a canonical decimal string. Its shape is
defined by
[`restart-replay-diagnostics.schema.json`](../schemas/acceptance/restart-replay-diagnostics.schema.json),
not by the normalized history schema.

## Validation stages

The validator is
[`validate-restart-replay.sh`](../../test/integration/temporal/scripts/validate-restart-replay.sh).
It takes `--stage initial` or `--stage terminal`:

```sh
validate-restart-replay.sh \
  --history history.json \
  --initial-history initial-history.json \
  --workflow-id two-binary-worker-restart-replay \
  --run-id <exact-start-run-id> \
  --stage terminal
```

The initial stage requires the ordered subsequence
`WorkflowExecutionStarted`, `WorkflowTaskCompleted`, and `TimerStarted`. It
also rejects `TimerFired`, activity, and terminal events, so the controller
cannot mistake an already-completed workflow for a safe pending-timer
boundary. The terminal stage takes the initial document through
`--initial-history`; it validates that document with the same initial-stage
rules and requires the terminal document to begin with the exact same ordered
event prefix. Matching workflow and run IDs alone is not enough because a
history adapter that returned a different event list for the same IDs could
otherwise turn an unrelated completion into restart/replay evidence.

The terminal stage requires the ordered subsequence
`WorkflowExecutionStarted`, `WorkflowTaskCompleted`, `TimerStarted`,
`TimerFired`, `ActivityTaskScheduled`, `ActivityTaskCompleted`, and
`WorkflowExecutionCompleted`, with the workflow-completed event last. Other
known Temporal scheduling/started events may appear between those boundaries.
Event IDs must be positive canonical decimal strings in strict ascending order
in both documents, and the document's workflow/run IDs must exactly match the
driver record. The JSON Schemas describe each document independently; the
validator enforces this cross-document prefix relationship because JSON Schema
cannot compare two separate files.

## Replay diagnostics

The worker diagnostics document follows
[`restart-replay-diagnostics.schema.json`](../schemas/acceptance/restart-replay-diagnostics.schema.json):

```json
{
  "workflow_id": "two-binary-worker-restart-replay",
  "run_id": "<exact-start-run-id>",
  "records": [
    {"phase": "initial", "generation": 1, "is_replaying": false, "history_length": "5"},
    {"phase": "replay", "generation": 2, "is_replaying": true, "history_length": "5"}
  ]
}
```

The first record proves only that generation 1 observed the execution. The
second record is required for a replay claim: it must use generation 2, repeat
the exact run identity, set `is_replaying` to `true`, and report a positive
history length. The record contains no input, output, payload, timestamp, or
process identifier. The validator also checks that each record has exactly the
documented fields and that generation/flag combinations cannot be swapped.

The eventual worker log may retain the design document's human-readable
`phase=replay generation=2 ...` marker for bounded diagnostics, but the
controller should convert it to this JSON shape with a strict parser before
making an acceptance decision. If the worker cannot provide the replay bit, a
successful terminal result may be described as **worker restarted and
continued** only; it must not be reported as replay evidence.

## Controller lifecycle evidence

The controller also needs to prove that it replaced a worker while the exact
workflow run was still pending. That evidence is separate from the history
projection and the worker replay records: history describes what Temporal
recorded, diagnostics describe what a worker observed, and the controller
document describes what the test harness did. Its closed, ordered shape is
defined by
[`restart-replay-controller.schema.json`](../schemas/acceptance/restart-replay-controller.schema.json).

The checked-in fixture contains exactly eleven records, in this order:

1. `stack_ready` proves that no project volume was left by a previous run and
   that Temporal passed its health check.
2. `driver_accepted` records the workflow ID and the exact run ID returned by
   `Client.start`.
3. `history_checked` at `initial` records that the pending run reached the
   timer boundary before replacement.
4. `driver_waiting` proves that the one-shot assertion process had not exited
   before the worker was replaced.
5. `generation_one_stopped` records a graceful generation-1 shutdown and its
   container identity.
6. `generation_one_removed` proves that the stopped container was removed,
   rather than silently reused by Compose.
7. `generation_two_ready` records a different container and a fresh
   generation-aware readiness observation.
8. `replay_observed` records generation 2 seeing the same run with
   `is_replaying=true` and a positive history length.
9. `history_checked` at `terminal` records that the same run reached the
   required completion event sequence.
10. `driver_completed` proves that the driver received the expected completed
    result.
11. `postgres_volume_removed` proves that teardown removed the test data.

Every record has a closed field set and a successful status. The validator
cross-checks the workflow/run identity, requires generation-2 container
replacement, and requires zero project volumes at both the start and end. It
does not create a live Temporal observation: `controller.json` is currently an
offline contract fixture that a future Compose controller must produce from
real process, history, replay, and cleanup observations. A passing fixture
check therefore remains necessary contract coverage, not live restart/replay
evidence.

## Local gate and limits

Run the offline contract check with:

```sh
make test-temporal-worker-restart
```

This target does not start Docker, PostgreSQL, Temporal Server, an OCaml
worker, or a client. It runs positive fixtures and negative cases through the
same validators that a future controller will call. It proves the
schema-level identity/order/replay rules and the ordered lifecycle contract,
but nothing about a running stack. The real acceptance target still needs a
worker activation diagnostic hook, a strict Temporal history adapter,
generation-aware readiness markers, graceful worker replacement, and a failure
trap that removes the PostgreSQL volume. Those requirements remain in
[`worker-restart-replay-acceptance.md`](worker-restart-replay-acceptance.md).
