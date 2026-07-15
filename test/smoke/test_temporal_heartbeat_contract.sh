#!/bin/sh
set -eu

# This contract deliberately reads only checked-in source. It protects the
# heartbeat acceptance scenario in environments where Docker, Temporal
# Server, and PostgreSQL are unavailable; the companion Dune test executes the
# real contextual activity callback with an in-memory context. Neither test
# claims that a local fake observed server-managed retry delivery.
root=${1:-.}
fixture="$root/test/integration/temporal"
definitions="$fixture/common/smoke_definitions.ml"
driver="$fixture/driver/smoke_driver.ml"
worker="$fixture/worker/smoke_worker.ml"

# Checks that a source file required by the acceptance fixture is present. A
# missing fixture is a setup error and should fail before any text assertions.
require_file() {
  if [ ! -f "$1" ]; then
    echo "heartbeat acceptance contract is missing: $1" >&2
    exit 1
  fi
}

# Checks for an exact source fragment so a refactor cannot silently remove a
# heartbeat invariant while leaving the surrounding fixture files present.
require_text() {
  path=$1
  needle=$2
  if ! grep -F -- "$needle" "$path" >/dev/null; then
    echo "heartbeat acceptance contract is missing: $needle ($path)" >&2
    exit 1
  fi
}

require_file "$definitions"
require_file "$driver"
require_file "$worker"

# The shared definition must prove all four server-visible facts: one typed
# progress detail, the exact heartbeat timeout, an intentional retryable
# failure, and a second attempt that can succeed only after receiving the
# detail and timeout from its new activity context.
require_text "$definitions" 'let heartbeat_timeout = Temporal.Duration.of_ms 500L'
require_text "$definitions" 'let heartbeat_progress_detail = "SMOKE:HEARTBEAT:PROGRESS:1"'
require_text "$definitions" 'Temporal.Activity.define_with_context ~name:"smoke.heartbeat_retry"'
require_text "$definitions" 'Temporal.Activity.Context.heartbeat_timeout context'
require_text "$definitions" 'Temporal.Activity.Context.details context'
require_text "$definitions" 'Temporal.Activity.Context.heartbeat context Temporal.Codec.string'
require_text "$definitions" 'Unix.sleepf 0.1'
require_text "$definitions" '~category:`Activity'
require_text "$definitions" 'intentional retry after recording an activity heartbeat'
require_text "$definitions" '| [ detail ] ->'
require_text "$definitions" 'let* progress = Temporal.Codec.decode Temporal.Codec.string detail'
require_text "$definitions" 'Ok ("SMOKE:HEARTBEAT:RETRIED:" ^ String.uppercase_ascii input)'
require_text "$definitions" 'let activity_heartbeat_retry ='
require_text "$definitions" 'Temporal.Activity.execute ~heartbeat_timeout'
require_text "$definitions" '~retry_policy:policy'
require_text "$definitions" 'heartbeat_retry_activity seed'
require_text "$definitions" '~maximum_attempts:2 ()'

# The worker owns both implementation registrations. A source-only check here
# catches a refactor that leaves the driver assertion in place but quietly
# removes the heartbeat activity or workflow from the long-lived worker.
require_text "$worker" 'Worker.workflow Definitions.activity_heartbeat_retry'
require_text "$worker" 'Worker.activity Definitions.heartbeat_retry_activity'
require_text "$worker" 'Definitions.clear_cancellation_ready_file cancellation_ready_file'

# The driver must start the heartbeat workflow as one of the independent
# top-level runs, retain its exact handle, and assert its result through the
# public client wait. Keeping the assertions in the driver prevents a worker
# that merely reports readiness from satisfying the acceptance test.
require_text "$driver" 'two-binary-activity-heartbeat-retry'
require_text "$driver" 'let* heartbeat_retry_result = wait_workflow heartbeat_retry_handle'
require_text "$driver" 'require_completed "smoke.activity_heartbeat_retry"'
require_text "$driver" 'SMOKE:HEARTBEAT:RETRIED:SMOKE'
require_text "$driver" 'Definitions.clear_cancellation_ready_file cancellation_ready_file'

# The heartbeat start must occur before the first terminal wait. This is a
# small ordering assertion, not a parser: it catches accidental serialization
# that would hide whether heartbeat retries can overlap other top-level runs.
heartbeat_start_line=$(grep -n -F 'let* heartbeat_retry_handle =' "$driver" | head -n 1 | cut -d: -f1)
first_wait_line=$(grep -n -F 'let* fan_result = wait_workflow' "$driver" | head -n 1 | cut -d: -f1)
if [ -z "$heartbeat_start_line" ] || [ -z "$first_wait_line" ] \
  || [ "$heartbeat_start_line" -ge "$first_wait_line" ]; then
  echo "heartbeat workflow must start before the first terminal wait" >&2
  exit 1
fi

# Keep the two-process boundary explicit. The driver is client-only, while
# the worker is the only process allowed to register definitions and run the
# native worker loop.
if grep -F 'Worker.create' "$driver" >/dev/null \
  || grep -F 'Worker.run' "$driver" >/dev/null; then
  echo "heartbeat driver must not become a worker" >&2
  exit 1
fi
if grep -F 'Client.start' "$worker" >/dev/null \
  || grep -F 'Client.wait' "$worker" >/dev/null; then
  echo "heartbeat worker must not become a client driver" >&2
  exit 1
fi

echo "temporal heartbeat acceptance contract: ok"
