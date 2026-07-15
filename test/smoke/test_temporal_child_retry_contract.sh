#!/bin/sh
set -eu

# This contract protects the shape of the live child-retry acceptance fixture
# when Docker or Temporal Server is unavailable. Only the Compose run can prove
# that the server retried the child execution; these checks ensure that a
# future edit cannot silently replace that boundary with an activity-only retry
# or a local parent loop.
root=${1:-.}
fixture="$root/test/integration/temporal"
definitions="$fixture/common/smoke_definitions.ml"
driver="$fixture/driver/smoke_driver.ml"
worker="$fixture/worker/smoke_worker.ml"

# Checks that each source file required by the two-process scenario exists.
require_file() {
  if [ ! -f "$1" ]; then
    echo "child-retry acceptance contract is missing: $1" >&2
    exit 1
  fi
}

# Checks one source file for a required fixture fragment. This deliberately
# does not claim to observe Temporal's durable retry state machine without the
# live Compose environment.
require_text() {
  path=$1
  needle=$2
  if ! grep -F -- "$needle" "$path" >/dev/null; then
    echo "child-retry acceptance contract is missing: $needle ($path)" >&2
    exit 1
  fi
}

require_file "$definitions"
require_file "$driver"
require_file "$worker"

# The activity may fail only once, and its own policy is limited to one
# attempt. The separate child policy must be the only retry boundary under
# test, with the second marker proving that the retried child succeeded.
require_text "$definitions" \
  'let child_activity_no_retry_policy ='
require_text "$definitions" \
  '~maximum_interval:(Temporal.Duration.of_ms 100L)'
require_text "$definitions" '~maximum_attempts:1 ()'
require_text "$definitions" \
  'let child_retry_attempts = Atomic.make 0'
require_text "$definitions" \
  'Temporal.Activity.define ~name:"smoke.child_retry_once"'
require_text "$definitions" \
  'intentional transient child failure'
require_text "$definitions" \
  'SMOKE:CHILD_RETRY:ATTEMPT:%d'
require_text "$definitions" \
  'let child_retryable_failure ='
require_text "$definitions" \
  'match child_activity_no_retry_policy with'
require_text "$definitions" \
  'Temporal.Activity.execute ~retry_policy:policy child_retry_activity'
require_text "$definitions" 'seed'
require_text "$definitions" \
  'intentional retryable child workflow failure'
require_text "$definitions" \
  'let child_retry_policy ='
require_text "$definitions" \
  '~maximum_interval:(Temporal.Duration.of_ms 100L)'
require_text "$definitions" '~maximum_attempts:2 ()'
require_text "$definitions" \
  'let parent_retries_child ='
require_text "$definitions" \
  'Temporal.Child_workflow.execute ~retry_policy:policy'

# Both child workflow definitions and the activity must be registered in the
# worker. Otherwise the driver could start a workflow without exercising the
# intended child/activity task path.
require_text "$worker" \
  'Worker.workflow Definitions.child_retryable_failure'
require_text "$worker" \
  'Worker.workflow Definitions.parent_retries_child'
require_text "$worker" \
  'Worker.activity Definitions.child_retry_activity'

# The driver starts the parent before its first terminal wait and asserts the
# second-attempt marker through the public client handle.
require_text "$driver" \
  'id:"two-binary-parent-retries-child"'
require_text "$driver" \
  'let* child_retry_result = wait_workflow child_retry_handle'
require_text "$driver" \
  'require_completed "smoke.parent_retries_child"'
require_text "$driver" \
  'SMOKE:CHILD_RETRY:ATTEMPT:2'

first_terminal_wait_line=$(grep -n -F \
  'let* fan_result = wait_workflow fan_handle' "$driver" \
  | head -n 1 | cut -d: -f1)
start_line=$(grep -n -F \
  'id:"two-binary-parent-retries-child"' "$driver" \
  | head -n 1 | cut -d: -f1)
if [ -z "$start_line" ] || [ -z "$first_terminal_wait_line" ] \
  || [ "$start_line" -ge "$first_terminal_wait_line" ]; then
  echo "child-retry workflow must start before the first terminal wait" >&2
  exit 1
fi

# Preserve the two-process boundary: only the worker registers definitions,
# while only the driver starts and waits for the exact run.
if grep -F 'Worker.create' "$driver" >/dev/null \
  || grep -F 'Worker.run' "$driver" >/dev/null; then
  echo "child-retry driver must not become a worker" >&2
  exit 1
fi
if grep -F 'Client.start' "$worker" >/dev/null \
  || grep -F 'Client.wait' "$worker" >/dev/null; then
  echo "child-retry worker must not become a client driver" >&2
  exit 1
fi

echo "temporal child-retry acceptance contract: ok"
