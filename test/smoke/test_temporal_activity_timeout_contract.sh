#!/bin/sh
set -eu

# This contract reads only checked-in source, so it protects the timeout-retry
# acceptance shape when Docker, Temporal Server, and PostgreSQL are unavailable.
# Only the live Compose run can prove that the late first completion is ignored
# after Temporal starts a retry; this script checks that the fixture still makes
# that server-visible distinction.
root=${1:-.}
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
require_text "$definitions" 'let timeout_retry_attempts = Atomic.make 0'
require_text "$definitions" \
  'Temporal.Activity.define ~name:"smoke.timeout_retry"'
require_text "$definitions" 'Unix.sleepf timeout_retry_first_attempt_sleep_seconds'
require_text "$definitions" 'Ok "SMOKE:TIMEOUT:ATTEMPT:1"'
require_text "$definitions" \
  'Ok ("SMOKE:TIMEOUT:RETRIED:" ^ String.uppercase_ascii input)'
require_text "$definitions" 'let activity_timeout_retry ='
require_text "$definitions" \
  '~start_to_close_timeout:timeout_retry_start_to_close_timeout'
require_text "$definitions" '~retry_policy:policy ~do_not_eagerly_execute:true'
require_text "$definitions" '~maximum_attempts:2 ()'

# The worker must own both the workflow registration and executable activity;
# otherwise a driver-only source assertion could pass without a real poll or
# completion task crossing the worker boundary.
require_text "$worker" 'Worker.workflow Definitions.activity_timeout_retry'
require_text "$worker" 'Worker.activity Definitions.timeout_retry_activity'

# The driver starts this run before its first wait, retains its exact handle,
# and compares the server-delivered second-attempt marker through Client.wait.
require_text "$driver" 'two-binary-activity-timeout-retry'
require_text "$driver" \
  'let* timeout_retry_result = wait_workflow timeout_retry_handle'
require_text "$driver" 'require_completed "smoke.activity_timeout_retry"'
require_text "$driver" 'SMOKE:TIMEOUT:RETRIED:SMOKE'

timeout_start_line=$(grep -n -F 'let* timeout_retry_handle =' "$driver" \
  | head -n 1 | cut -d: -f1)
first_wait_line=$(grep -n -F 'let* fan_result = wait_workflow' "$driver" \
  | head -n 1 | cut -d: -f1)
if [ -z "$timeout_start_line" ] || [ -z "$first_wait_line" ] \
  || [ "$timeout_start_line" -ge "$first_wait_line" ]; then
  echo "timeout-retry workflow must start before the first terminal wait" >&2
  exit 1
fi

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
