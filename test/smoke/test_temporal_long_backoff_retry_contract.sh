#!/bin/sh
set -eu

# This source-only contract protects the live long-backoff retry shape when
# Docker or Temporal Server is unavailable. Only the Compose run can prove the
# server-owned retry delay; these checks prevent a fast local retry or an
# unregistered fixture from masquerading as that evidence.
root=${1:-.}
fixture="$root/test/integration/temporal"
definitions="$fixture/common/smoke_definitions.ml"
driver="$fixture/driver/smoke_driver.ml"
worker="$fixture/worker/smoke_worker.ml"

# Reports the missing boundary with its source path instead of allowing a
# partial source edit to silently remove the acceptance scenario.
require_text() {
  path=$1
  needle=$2
  if ! grep -F -- "$needle" "$path" >/dev/null; then
    echo "long-backoff retry acceptance contract is missing: $needle ($path)" >&2
    exit 1
  fi
}

for path in "$definitions" "$driver" "$worker"; do
  if [ ! -r "$path" ]; then
    echo "long-backoff retry acceptance contract is missing: $path" >&2
    exit 1
  fi
done

# The policy must wait long enough to distinguish a server-scheduled retry from
# the existing 100ms ordinary retry path, while remaining bounded for CI.
require_text "$definitions" 'let long_backoff_retry_policy ='
require_text "$definitions" \
  '~initial_interval:(Temporal.Duration.of_ms 2_000L)'
require_text "$definitions" \
  '~maximum_interval:(Temporal.Duration.of_ms 2_000L)'
require_text "$definitions" '~maximum_attempts:2 ()'

# The activity records the first attempt time in worker-local state and rejects
# an unexpectedly immediate second attempt. The exact result marker then proves
# that the delayed retry crossed the real Temporal/Core activity boundary.
require_text "$definitions" 'let long_backoff_retry_attempts = Atomic.make 0'
require_text "$definitions" 'let long_backoff_retry_first_attempt_at = Atomic.make 0.0'
require_text "$definitions" 'let long_backoff_retry_minimum_delay_seconds = 1.0'
require_text "$definitions" \
  'Temporal.Activity.define ~name:"smoke.long_backoff_retry"'
require_text "$definitions" 'Atomic.set long_backoff_retry_first_attempt_at'
require_text "$definitions" '~category:`Activity'
require_text "$definitions" \
  'elapsed < long_backoff_retry_minimum_delay_seconds'
require_text "$definitions" \
  'SMOKE:BACKOFF:RETRIED:'
require_text "$definitions" 'let activity_long_backoff_retry ='
require_text "$definitions" '~do_not_eagerly_execute:true'

# Both executable sides must own their normal responsibilities: the worker
# registers the workflow/activity, and the separate driver starts and waits on
# the exact run through the public client.
require_text "$worker" \
  'Worker.workflow Definitions.activity_long_backoff_retry'
require_text "$worker" \
  'Worker.activity Definitions.long_backoff_retry_activity'
require_text "$driver" 'id:"two-binary-activity-long-backoff-retry"'
require_text "$driver" 'let* long_backoff_retry_result ='
require_text "$driver" 'wait_workflow long_backoff_retry_handle'
require_text "$driver" \
  'require_completed "smoke.activity_long_backoff_retry"'
require_text "$driver" 'SMOKE:BACKOFF:RETRIED:SMOKE'

# Start this scenario before the first terminal wait so the acceptance still
# proves concurrent top-level scheduling rather than a local sequential call.
first_terminal_wait_line=$(grep -n -F \
  'let* fan_result = wait_workflow fan_handle' "$driver" \
  | head -n 1 | cut -d: -f1)
start_line=$(grep -n -F \
  'id:"two-binary-activity-long-backoff-retry"' "$driver" \
  | head -n 1 | cut -d: -f1)
if [ -z "$start_line" ] || [ -z "$first_terminal_wait_line" ] \
  || [ "$start_line" -ge "$first_terminal_wait_line" ]; then
  echo "long-backoff retry workflow must start before the first terminal wait" >&2
  exit 1
fi

# Preserve the independent two-process topology while the fixture grows.
if grep -F 'Worker.create' "$driver" >/dev/null \
  || grep -F 'Worker.run' "$driver" >/dev/null; then
  echo "long-backoff retry driver must not become a worker" >&2
  exit 1
fi
if grep -F 'Client.start' "$worker" >/dev/null \
  || grep -F 'Client.wait' "$worker" >/dev/null; then
  echo "long-backoff retry worker must not become a client driver" >&2
  exit 1
fi

echo "temporal long-backoff retry acceptance contract: ok"
