#!/bin/sh
set -eu

# Fast, Docker-free regression gate for the sticky-cache acceptance contract.
# It exercises the same strict validator used by the live controller and also
# protects the separate OCaml driver/worker roles that make cache pressure
# meaningful rather than a synthetic worker restart.

root=$(CDPATH= cd -- "$(dirname "$0")/../../../.." && pwd)
fixture="$root/test/integration/temporal/fixtures/cache-eviction"
validator="$root/test/integration/temporal/scripts/validate-cache-eviction.sh"
schema="$root/docs/schemas/acceptance/cache-eviction-diagnostics.schema.json"
workflow_id=two-binary-sticky-cache-eviction-target
run_id=11111111-1111-4111-8111-111111111111

[ -r "$validator" ]
[ -r "$schema" ]
[ -r "$fixture/diagnostics.json" ]
[ -r "$fixture/history.initial.json" ]
[ -r "$fixture/history.terminal.json" ]

# A successful invocation is the positive contract. The schema is parsed here
# as well, while the shell validator supplies the cross-document constraints
# that Draft 2020-12 cannot represent.
jq -e . "$schema" >/dev/null
sh "$validator" \
  --diagnostics "$fixture/diagnostics.json" \
  --initial-history "$fixture/history.initial.json" \
  --terminal-history "$fixture/history.terminal.json" \
  --workflow-id "$workflow_id" --run-id "$run_id" >/dev/null

# The live controller must establish this stage before it releases cache
# pressure. Exercise that exact shared validator here so a jq scoping mistake
# cannot turn a valid pending timer into a live-only false negative.
sh "$validator" --stage initial \
  --initial-history "$fixture/history.initial.json" \
  --workflow-id "$workflow_id" --run-id "$run_id" >/dev/null

# Runs a negative assertion without printing an expected failure into normal
# test output. Every mutation below corresponds to an invariant that the live
# controller must reject before claiming a real CacheFull/replay observation.
expect_failure() {
  if "$@" >/dev/null 2>&1; then
    echo "expected command to fail: $*" >&2
    exit 1
  fi
}

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

# The live controller consumes normalized files, so its validator must enforce
# the normalizer's closed event-name projection even when called directly.
jq '.events[1].type = "NotARealEvent"' "$fixture/history.initial.json" \
  >"$tmp/history-unknown-event-type.json"
expect_failure sh "$validator" --stage initial \
  --initial-history "$tmp/history-unknown-event-type.json" \
  --workflow-id "$workflow_id" --run-id "$run_id"

jq '.records[1].empty_completion = false' "$fixture/diagnostics.json" \
  >"$tmp/ack-not-empty.json"
expect_failure sh "$validator" \
  --diagnostics "$tmp/ack-not-empty.json" \
  --initial-history "$fixture/history.initial.json" \
  --terminal-history "$fixture/history.terminal.json" \
  --workflow-id "$workflow_id" --run-id "$run_id"

jq '.records[2].is_replaying = false' "$fixture/diagnostics.json" \
  >"$tmp/not-replay.json"
expect_failure sh "$validator" \
  --diagnostics "$tmp/not-replay.json" \
  --initial-history "$fixture/history.initial.json" \
  --terminal-history "$fixture/history.terminal.json" \
  --workflow-id "$workflow_id" --run-id "$run_id"

jq '.records[2].history_length = "0"' "$fixture/diagnostics.json" \
  >"$tmp/empty-replay-history.json"
expect_failure sh "$validator" \
  --diagnostics "$tmp/empty-replay-history.json" \
  --initial-history "$fixture/history.initial.json" \
  --terminal-history "$fixture/history.terminal.json" \
  --workflow-id "$workflow_id" --run-id "$run_id"

jq '.events += [{"event_id":"6","type":"TimerFired"}]' \
  "$fixture/history.initial.json" >"$tmp/initial-timer-fired.json"
expect_failure sh "$validator" --stage initial \
  --initial-history "$tmp/initial-timer-fired.json" \
  --workflow-id "$workflow_id" --run-id "$run_id"
expect_failure sh "$validator" \
  --diagnostics "$fixture/diagnostics.json" \
  --initial-history "$tmp/initial-timer-fired.json" \
  --terminal-history "$fixture/history.terminal.json" \
  --workflow-id "$workflow_id" --run-id "$run_id"

jq '.events += [{"event_id":"6","type":"WorkflowExecutionCompleted"}]' \
  "$fixture/history.initial.json" >"$tmp/initial-terminal.json"
expect_failure sh "$validator" --stage initial \
  --initial-history "$tmp/initial-terminal.json" \
  --workflow-id "$workflow_id" --run-id "$run_id"

jq '.events[4].type = "WorkflowTaskScheduled"' \
  "$fixture/history.terminal.json" >"$tmp/history-prefix-changed.json"
expect_failure sh "$validator" \
  --diagnostics "$fixture/diagnostics.json" \
  --initial-history "$fixture/history.initial.json" \
  --terminal-history "$tmp/history-prefix-changed.json" \
  --workflow-id "$workflow_id" --run-id "$run_id"

# Keep the protocol topology explicit: the driver starts/waits through the
# public client while the other binary creates/runs the one-entry public worker.
require_text() {
  path=$1
  needle=$2
  if ! grep -F -- "$needle" "$path" >/dev/null; then
    echo "cache-eviction contract is missing: $needle ($path)" >&2
    exit 1
  fi
}

require_absent() {
  path=$1
  needle=$2
  if grep -F -- "$needle" "$path" >/dev/null; then
    echo "cache-eviction role is not isolated: $needle ($path)" >&2
    exit 1
  fi
}

worker="$root/test/integration/temporal/worker/cache_eviction_worker.ml"
driver="$root/test/integration/temporal/driver/cache_eviction_driver.ml"
definitions="$root/test/integration/temporal/common/smoke_definitions.ml"
compose="$root/test/integration/temporal/compose.yaml"
makefile="$root/Makefile"
controller="$root/test/integration/temporal/scripts/run-cache-eviction-live.sh"

require_text "$worker" 'Worker.Options.make ~max_cached_workflows:1'
require_text "$worker" '~max_concurrent_workflow_task_polls:2'
require_text "$worker" 'Worker.workflow Definitions.sticky_cache_eviction'
require_text "$worker" 'Worker.run worker'
require_text "$worker" 'Worker.shutdown worker'
require_absent "$worker" 'Client.start'
require_text "$driver" 'module Client = Temporal.Client'
require_text "$driver" 'Client.start client ~workflow:Definitions.sticky_cache_eviction'
require_text "$driver" 'Client.wait target'
require_text "$driver" 'cache-eviction driver failed kind=%s'
require_absent "$driver" 'Worker.create'
require_absent "$driver" 'Error.message error'
require_text "$definitions" 'let sticky_cache_eviction ='
require_text "$definitions" 'Temporal.Workflow.sleep (Temporal.Duration.of_ms 20_000L)'
require_text "$compose" 'smoke-cache-eviction-worker:'
require_text "$compose" 'smoke-cache-eviction-driver:'
require_text "$compose" 'cache_eviction_worker.exe'
require_text "$compose" 'cache_eviction_driver.exe'
require_text "$makefile" 'test-temporal-worker-cache-eviction-live:'
require_text "$makefile" 'down --volumes --remove-orphans'
require_text "$makefile" '"$(SMOKE_CACHE_EVICTION_INITIAL_HISTORY).raw"'
require_text "$makefile" '"$(SMOKE_CACHE_EVICTION_INITIAL_HISTORY).describe.json"'
require_text "$controller" 'print_driver_log()'
require_text "$controller" 'driver_log_printed=false'
require_text "$controller" 'tail -c 65536 "$driver_log"'
require_absent "$controller" 'cat "$driver_log"'
require_text "$controller" 'scratch_dir=$(mktemp -d /tmp/ocaml-temporal-cache-eviction.XXXXXX)'
require_text "$controller" 'cleanup_scratch()'
require_text "$controller" 'raw_history="$scratch_dir/history.json"'
require_text "$controller" 'describe_file="$scratch_dir/describe.json"'
require_absent "$controller" 'ulimit -f'
require_text "$controller" '--output json >"$raw_history" 2>/dev/null'
require_text "$controller" 'is_bounded_file "$raw_history" 8388608'
require_text "$controller" 'smoke-cache-eviction-driver >"$driver_log" 2>&1 &'
require_text "$controller" '--output json >"$describe_file" 2>/dev/null'
require_text "$controller" 'is_bounded_file "$describe_file" 1048576'
require_text "$controller" 'is_bounded_file "$diagnostics_file" 1048576'
require_absent "$controller" 'raw_history="${destination}.raw"'
require_absent "$controller" 'describe_file="${terminal_history}.describe.json"'
