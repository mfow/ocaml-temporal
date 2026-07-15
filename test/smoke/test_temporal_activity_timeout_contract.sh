#!/bin/sh
set -eu

# This contract reads only checked-in source, so it protects the timeout-retry
# acceptance shape when Docker, Temporal Server, and PostgreSQL are unavailable.
# Only the live Compose run can prove that the late first completion is ignored
# after Temporal starts a retry; this script checks that the fixture still makes
# that server-visible distinction.
root=${1:-.}
. "$root/test/smoke/source_contract_helpers.sh"
fixture="$root/test/integration/temporal"
definitions="$fixture/common/smoke_definitions.ml"
driver="$fixture/driver/smoke_driver.ml"
worker="$fixture/worker/smoke_worker.ml"

# Checks for a source file and exact fragment so a partial registration cannot
# silently leave the driver assertion without the worker implementation.
require_file() {
  if [ ! -f "$1" ]; then
    echo "activity timeout acceptance contract is missing: $1" >&2
    exit 1
  fi
}

# Checks one source file for a required literal and reports the missing
# contract fragment with its path. Keeping this helper literal-based avoids
# pretending that a Docker-free check has observed Temporal's timeout state
# machine.
require_text() {
  path=$1
  needle=$2
  if ! grep -F -- "$needle" "$path" >/dev/null; then
    echo "activity timeout acceptance contract is missing: $needle ($path)" >&2
    exit 1
  fi
}

require_file "$definitions"
require_file "$driver"
require_file "$worker"

# The first callback returns a success only after sleeping past the exact
# start-to-close lease. The second callback has a distinct marker, and the
# workflow supplies a bounded retry policy; together these prevent an ordinary
# application failure from masquerading as timeout-triggered retry coverage.
require_text "$definitions" \
  'let timeout_retry_start_to_close_timeout = Temporal.Duration.of_ms 500L'
require_text "$definitions" \
  'let timeout_retry_first_attempt_sleep_seconds = 6.0'
require_ocaml_binding_tokens "$definitions" timeout_retry_policy \
  '~initial_interval:(Temporal.Duration.of_ms 7_000L)
   ~backoff_coefficient:1.0
   ~maximum_interval:(Temporal.Duration.of_ms 7_000L)
   ~maximum_attempts:2 ()' \
  'activity timeout acceptance contract'
require_text "$definitions" 'let timeout_retry_attempts = Atomic.make 0'
require_text "$definitions" \
  'Temporal.Activity.define ~name:"smoke.timeout_retry"'
require_text "$definitions" 'Unix.sleepf timeout_retry_first_attempt_sleep_seconds'
require_text "$definitions" 'Ok "SMOKE:TIMEOUT:ATTEMPT:1"'
require_text "$definitions" \
  'Ok ("SMOKE:TIMEOUT:RETRIED:" ^ String.uppercase_ascii input)'
require_text "$definitions" 'match timeout_retry_policy with'
require_ocaml_binding_tokens "$definitions" activity_timeout_retry \
  'Temporal.Activity.execute
   ~start_to_close_timeout:timeout_retry_start_to_close_timeout
   ~retry_policy:policy ~do_not_eagerly_execute:true
   timeout_retry_activity seed' \
  'activity timeout acceptance contract'

# The worker must own both the workflow registration and executable activity;
# otherwise a driver-only source assertion could pass without a real poll or
# completion task crossing the worker boundary.
require_text "$worker" 'Worker.workflow Definitions.activity_timeout_retry'
require_text "$worker" 'Worker.activity Definitions.timeout_retry_activity'

# The driver starts this run after the short-heartbeat scenario reaches its
# terminal retry result, retains its exact handle, and compares the
# server-delivered second-attempt marker through Client.wait. The sequencing is
# intentional: the activity adapter serializes polling, callbacks, heartbeats,
# and completions, so a six-second callback must not hold the lane while the
# heartbeat scenario still has a 500ms lease outstanding.
require_text "$driver" 'two-binary-activity-timeout-retry'
require_text "$driver" \
  'let* timeout_retry_result = wait_workflow timeout_retry_handle'
require_text "$driver" 'require_completed "smoke.activity_timeout_retry"'
require_text "$driver" 'SMOKE:TIMEOUT:RETRIED:SMOKE'

timeout_start_line=$(grep -n -F 'let* timeout_retry_handle =' "$driver" \
  | head -n 1 | cut -d: -f1)
heartbeat_result_line=$(grep -n -F \
  'let* heartbeat_retry_result = wait_workflow heartbeat_retry_handle' "$driver" \
  | head -n 1 | cut -d: -f1)
heartbeat_assertion_line=$(grep -n -F \
  'require_completed "smoke.activity_heartbeat_retry"' "$driver" \
  | head -n 1 | cut -d: -f1)
if [ -z "$timeout_start_line" ] || [ -z "$heartbeat_result_line" ] \
  || [ -z "$heartbeat_assertion_line" ] \
  || [ "$timeout_start_line" -le "$heartbeat_assertion_line" ]; then
  echo "timeout-retry workflow must start after the heartbeat retry result" >&2
  exit 1
fi

# Preserve the fan-out contract for every other top-level scenario: these
# workflow requests must all be accepted before the first terminal wait. This
# keeps the acceptance test focused on concurrent scheduling while the timeout
# scenario is the one explicit exception required by the single-lane adapter.
first_terminal_wait_line=$(grep -n -F \
  'let* fan_result = wait_workflow fan_handle' "$driver" \
  | head -n 1 | cut -d: -f1)
for workflow_id in \
  two-binary-fan-out \
  two-binary-timer-then-activity \
  two-binary-activity-retry \
  two-binary-activity-heartbeat-retry \
  two-binary-parent-awaits-child \
  two-binary-parent-awaits-failed-child \
  two-binary-parent-cancels-child \
  two-binary-non-retryable-failure \
  two-binary-long-running-cancellation
do
  start_line=$(grep -n -F "id:\"$workflow_id\"" "$driver" \
    | head -n 1 | cut -d: -f1)
  if [ -z "$start_line" ] || [ -z "$first_terminal_wait_line" ] \
    || [ "$start_line" -ge "$first_terminal_wait_line" ]; then
    echo "$workflow_id must start before the first terminal wait" >&2
    exit 1
  fi
done

# Preserve the two-process boundary while this scenario is expanded: only the
# worker registers/executes callbacks, and only the driver starts and waits.
if grep -F 'Worker.create' "$driver" >/dev/null \
  || grep -F 'Worker.run' "$driver" >/dev/null; then
  echo "timeout-retry driver must not become a worker" >&2
  exit 1
fi
if grep -F 'Client.start' "$worker" >/dev/null \
  || grep -F 'Client.wait' "$worker" >/dev/null; then
  echo "timeout-retry worker must not become a client driver" >&2
  exit 1
fi

echo "temporal activity timeout acceptance contract: ok"
