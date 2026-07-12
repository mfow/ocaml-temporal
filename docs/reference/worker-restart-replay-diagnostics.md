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

The document contains only the workflow ID, the exact run ID returned by
`Client.start`, and a bounded list of event IDs and event type names. It is a
projection, not a replacement for Temporal history: event attributes are
intentionally excluded because the acceptance test only needs ordering and
identity at this synchronization boundary. Event IDs and
`history_length` are canonical decimal strings rather than JSON numbers. This
avoids the precision loss that JavaScript-style JSON parsers introduce for
Temporal's signed 64-bit values.

## Validation stages

The validator is
[`validate-restart-replay.sh`](../../test/integration/temporal/scripts/validate-restart-replay.sh).
It takes `--stage initial` or `--stage terminal`:

```sh
validate-restart-replay.sh \
  --history history.json \
  --workflow-id two-binary-worker-restart-replay \
  --run-id <exact-start-run-id> \
  --stage initial
```

The initial stage requires the ordered subsequence
`WorkflowExecutionStarted`, `WorkflowTaskCompleted`, and `TimerStarted`. It
also rejects `TimerFired`, activity, and terminal events, so the controller
cannot mistake an already-completed workflow for a safe pending-timer
boundary.

The terminal stage requires the ordered subsequence
`WorkflowExecutionStarted`, `WorkflowTaskCompleted`, `TimerStarted`,
`TimerFired`, `ActivityTaskScheduled`, `ActivityTaskCompleted`, and
`WorkflowExecutionCompleted`, with the workflow-completed event last. Other
known Temporal scheduling/started events may appear between those boundaries.
Event IDs must be positive canonical decimal strings in strict ascending
numeric order, and the
document's workflow/run IDs must exactly match the driver record.

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

## Local gate and limits

Run the offline contract check with:

```sh
make test-temporal-worker-restart
```

This target does not start Docker, PostgreSQL, Temporal Server, an OCaml
worker, or a client. It runs positive fixtures and negative cases through the
same validator that a future controller will call. It proves the schema-level
identity/order/replay rules and nothing more. The real acceptance target still
needs a worker activation diagnostic hook, a strict Temporal history adapter,
generation-aware readiness markers, graceful worker replacement, and a
failure trap that removes the PostgreSQL volume. Those requirements remain in
[`worker-restart-replay-acceptance.md`](worker-restart-replay-acceptance.md).
