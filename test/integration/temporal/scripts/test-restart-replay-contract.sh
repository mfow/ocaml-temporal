#!/bin/sh
set -eu

# This is the Docker-free regression gate for the restart/replay acceptance
# contract. It exercises the same validator that a future Compose controller
# will call, including rejection paths for identity mismatches, premature timer
# completion, malformed ordering, and missing replay evidence.

root=$(CDPATH= cd -- "$(dirname "$0")/../../../.." && pwd)
fixture="$root/test/integration/temporal/fixtures/restart-replay"
validator="$root/test/integration/temporal/scripts/validate-restart-replay.sh"
workflow_id=two-binary-worker-restart-replay
run_id=11111111-1111-4111-8111-111111111111

[ -x "$validator" ]
[ -r "$fixture/history.initial.json" ]
[ -r "$fixture/history.terminal.json" ]
[ -r "$fixture/diagnostics.json" ]

# Runs one deliberately invalid invocation and fails the test if it is
# accepted. Keeping the negative assertion as a helper makes each rejection
# case below read as the invariant it protects.
expect_failure() {
  if "$@" >/dev/null 2>&1; then
    echo "expected command to fail: $*" >&2
    exit 1
  fi
}

"$validator" \
  --history "$fixture/history.initial.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage initial >/dev/null

"$validator" \
  --history "$fixture/history.terminal.json" \
  --initial-history "$fixture/history.initial.json" \
  --diagnostics "$fixture/diagnostics.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage terminal \
  --require-replay >/dev/null

# Terminal validation cannot be used without retaining the initial snapshot;
# otherwise the cross-document event-prefix check would be bypassed.
expect_failure "$validator" \
  --history "$fixture/history.terminal.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage terminal

expect_failure "$validator" \
  --history "$fixture/history.terminal.json" \
  --initial-history "$fixture/history.initial.json" \
  --workflow-id "$workflow_id" \
  --run-id wrong-run-id \
  --stage terminal

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

# Keep the validator's configurable jq executable path quoted end to end. A
# path containing spaces is valid on the host and catches command substitutions
# that accidentally split [JQ_BIN] after the structural checks have passed.
jq_path=$(command -v jq)
space_jq="$tmp/jq wrapper"
ln -s "$jq_path" "$space_jq"
JQ_BIN="$space_jq" "$validator" \
  --history "$fixture/history.terminal.json" \
  --initial-history "$fixture/history.initial.json" \
  --diagnostics "$fixture/diagnostics.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage terminal \
  --require-replay >/dev/null

# A terminal event before the timer is observed must not satisfy the initial
# stop boundary. jq is used only to create an ephemeral negative fixture; it is
# not part of the production worker or the future Temporal history adapter.
jq '.events += [{"event_id": "6", "type": "TimerFired"}]' \
  "$fixture/history.initial.json" >"$tmp/history-fired.json"
expect_failure "$validator" \
  --history "$tmp/history-fired.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage initial

jq '.records[1].is_replaying = false' \
  "$fixture/diagnostics.json" >"$tmp/diagnostics-without-replay.json"
expect_failure "$validator" \
  --history "$fixture/history.terminal.json" \
  --initial-history "$fixture/history.initial.json" \
  --diagnostics "$tmp/diagnostics-without-replay.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage terminal \
  --require-replay

jq '.records[1].history_length = "9223372036854775808"' \
  "$fixture/diagnostics.json" >"$tmp/diagnostics-out-of-range.json"
expect_failure "$validator" \
  --history "$fixture/history.terminal.json" \
  --initial-history "$fixture/history.initial.json" \
  --diagnostics "$tmp/diagnostics-out-of-range.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage terminal \
  --require-replay

jq '.events[10].event_id = "9"' \
  "$fixture/history.terminal.json" >"$tmp/history-nonmonotonic.json"
expect_failure "$validator" \
  --history "$tmp/history-nonmonotonic.json" \
  --initial-history "$fixture/history.initial.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage terminal

# JSON numbers are not used for event IDs, but the decimal-string contract
# still has to reject values outside Temporal's signed 64-bit range.
jq '.events[4].event_id = "9223372036854775808"' \
  "$fixture/history.initial.json" >"$tmp/history-out-of-range.json"
expect_failure "$validator" \
  --history "$tmp/history-out-of-range.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage initial

# A failed workflow event inserted between the timer and activity must not be
# hidden by a later successful-looking completion event.
jq '.events = .events[0:5]
  + [{"event_id": "6", "type": "WorkflowExecutionFailed"}]
  + (.events[5:] | map(.event_id = ((.event_id | tonumber) + 1 | tostring)))' \
  "$fixture/history.terminal.json" >"$tmp/history-early-terminal.json"
expect_failure "$validator" \
  --history "$tmp/history-early-terminal.json" \
  --initial-history "$fixture/history.initial.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage terminal

# The baseline must be the exact initial event prefix, not merely a valid
# pending-timer history for the same workflow/run identity. This mutation keeps
# the baseline valid on its own while changing one already-observed event.
jq '.events[2].type = "WorkflowTaskScheduled"' \
  "$fixture/history.initial.json" >"$tmp/history-prefix-mismatch.json"
"$validator" \
  --history "$tmp/history-prefix-mismatch.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage initial >/dev/null
expect_failure "$validator" \
  --history "$fixture/history.terminal.json" \
  --initial-history "$tmp/history-prefix-mismatch.json" \
  --workflow-id "$workflow_id" \
  --run-id "$run_id" \
  --stage terminal

echo 'restart/replay contract: ok'
