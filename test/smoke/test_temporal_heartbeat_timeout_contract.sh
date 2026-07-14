#!/bin/sh
set -eu

# This contract reads only checked-in source, so it protects the
# heartbeat-timeout acceptance shape when Docker, Temporal Server, and
# PostgreSQL are unavailable. Only the live Compose run can prove that the
# server retried an activity that stopped heartbeating before its longer
# start-to-close lease expired.
root=${1:-.}
fixture="$root/test/integration/temporal"
definitions="$fixture/common/smoke_definitions.ml"
driver="$fixture/driver/smoke_driver.ml"
worker="$fixture/worker/smoke_worker.ml"

# Checks that each source file required by the two-process scenario exists.
require_file() {
  if [ ! -f "$1" ]; then
    echo "heartbeat-timeout acceptance contract is missing: $1" >&2
    exit 1
  fi
}

# Checks one source file for a required fixture fragment. This deliberately
# does not claim to observe Temporal's retry state machine without Docker.
require_text() {
  path=$1
  needle=$2
  if ! grep -F -- "$needle" "$path" >/dev/null; then
    echo "heartbeat-timeout acceptance contract is missing: $needle ($path)" >&2
    exit 1
  fi
}

require_file "$definitions"
require_file "$driver"
require_file "$worker"

# The first callback returns late without sending a heartbeat, but its
# start-to-close lease is longer than the delay. The second marker and empty
# retry details make an application-error retry or start-to-close timeout an
# insufficient substitute for this scenario.
require_text "$definitions" \
  'let heartbeat_timeout_retry_start_to_close_timeout ='
require_text "$definitions" \
  'let heartbeat_timeout_retry_first_attempt_sleep_seconds = 6.0'
require_text "$definitions" 'let heartbeat_timeout_retry_policy ='
require_text "$definitions" \
  '~initial_interval:(Temporal.Duration.of_ms 7_000L)'
require_text "$definitions" \
  '~maximum_interval:(Temporal.Duration.of_ms 7_000L) ~maximum_attempts:2 ()'
require_text "$definitions" \
  'let heartbeat_timeout_retry_attempts = Atomic.make 0'
require_text "$definitions" \
  'Temporal.Activity.define_with_context ~name:"smoke.heartbeat_timeout_retry"'
require_text "$definitions" \
  'Unix.sleepf heartbeat_timeout_retry_first_attempt_sleep_seconds'
require_text "$definitions" \
  'Ok "SMOKE:HEARTBEAT_TIMEOUT:ATTEMPT:1"'
require_text "$definitions" \
  'match Temporal.Activity.Context.details context'
require_text "$definitions" \
  'Ok ("SMOKE:HEARTBEAT_TIMEOUT:RETRIED:" ^ String.uppercase_ascii input)'
require_text "$definitions" 'let activity_heartbeat_timeout_retry ='
require_text "$definitions" \
  '~start_to_close_timeout:heartbeat_timeout_retry_start_to_close_timeout'
require_text "$definitions" '~heartbeat_timeout'
require_text "$definitions" '~retry_policy:policy ~do_not_eagerly_execute:true'

# The worker must register the workflow and context-aware activity so this
# test crosses the real worker boundary rather than stopping at source text.
require_text "$worker" \
  'Worker.workflow Definitions.activity_heartbeat_timeout_retry'
require_text "$worker" \
  'Worker.activity Definitions.heartbeat_timeout_retry_activity'

# The driver starts and waits for the exact workflow handle through the public
# client, and it does so after the separate start-to-close timeout scenario has
# completed so the serialized activity lane cannot hide the cause of retry.
require_text "$driver" 'two-binary-activity-heartbeat-timeout-retry'
require_text "$driver" \
  'let* heartbeat_timeout_retry_result ='
require_text "$driver" \
  'require_completed "smoke.activity_heartbeat_timeout_retry"'
require_text "$driver" 'SMOKE:HEARTBEAT_TIMEOUT:RETRIED:SMOKE'

heartbeat_timeout_start_line=$(grep -n -F \
  'let* heartbeat_timeout_retry_handle =' "$driver" \
  | head -n 1 | cut -d: -f1)
timeout_assertion_line=$(grep -n -F \
  'require_completed "smoke.activity_timeout_retry"' "$driver" \
  | head -n 1 | cut -d: -f1)
if [ -z "$heartbeat_timeout_start_line" ] || [ -z "$timeout_assertion_line" ] \
  || [ "$heartbeat_timeout_start_line" -le "$timeout_assertion_line" ]; then
  echo "heartbeat-timeout workflow must start after the start-to-close retry" >&2
  exit 1
fi

# Keep the process roles independent: only the worker registers definitions,
# while only the driver starts and waits for the exact run.
if grep -F 'Worker.create' "$driver" >/dev/null \
  || grep -F 'Worker.run' "$driver" >/dev/null; then
  echo "heartbeat-timeout driver must not become a worker" >&2
  exit 1
fi
if grep -F 'Client.start' "$worker" >/dev/null \
  || grep -F 'Client.wait' "$worker" >/dev/null; then
  echo "heartbeat-timeout worker must not become a client driver" >&2
  exit 1
fi

echo "temporal heartbeat-timeout acceptance contract: ok"
