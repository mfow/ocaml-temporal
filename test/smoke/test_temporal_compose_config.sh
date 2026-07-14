#!/bin/sh
set -eu

# This test deliberately inspects Compose's normalized model instead of the
# source YAML. That catches invalid interpolation and dependency/profile
# combinations before the much slower live integration smoke pulls images.
root=${1:-$(CDPATH="" cd -- "$(dirname "$0")/../.." && pwd)}
fixture="$root/test/integration/temporal"
compose_file="$fixture/compose.yaml"
rendered=$(mktemp)
trap 'rm -f "$rendered"' EXIT HUP INT TERM

docker compose --project-directory "$fixture" --file "$compose_file" \
  --project-name ocaml-temporal-integration --profile temporal config >"$rendered"

# Reports a focused failure when an invariant is absent from Compose's
# normalized output, keeping this shell test dependency-free and portable.
require_text() {
  needle=$1
  if ! grep -F -- "$needle" "$rendered" >/dev/null; then
    echo "normalized Compose model is missing: $needle" >&2
    exit 1
  fi
}

require_text 'postgres:16.13-bookworm@sha256:472efd9a66f2b2f1a5aeb18b28de74332e6ef88c2b93a1a5d812fb6db67a5f60'
require_text 'temporalio/server:1.31.0@sha256:b021b3b58c3f169634cdbb0451fcc0e69e8190b40454323362c7c52bbd4ff7b9'
require_text 'temporalio/admin-tools:1.31.0@sha256:3e68adcd54195a7c1222e99f2dbc32a4fdbf44ad69e3bb48e21e85c4bf417c2e'
require_text 'condition: service_healthy'
require_text 'condition: service_completed_successfully'
require_text 'temporal-postgres-data:'
require_text 'temporal-network:'
require_text 'pg_isready'
require_text 'nc -z localhost 7233'
require_text 'smoke-worker:'
require_text 'smoke-driver:'
require_text 'smoke-restart-driver:'
require_text 'TEMPORAL_TWO_BINARY_LIVE: "1"'
require_text 'smoke_worker.exe'
require_text 'smoke_driver.exe'
require_text 'restart_driver.exe'
require_text '--build-dir=/workspace/_build/smoke-worker'
require_text '--build-dir=/workspace/_build/smoke-driver'
require_text '--build-dir=/workspace/_build/smoke-restart-driver'
require_text 'SMOKE_DRIVER_TIMEOUT_SECONDS: "300"'
require_text 'SMOKE_CANCELLATION_READY_FILE: /workspace/test/integration/temporal/.cancellation-ready'
require_text 'SMOKE_WORKER_STOPPED_FILE: /workspace/test/integration/temporal/.worker-stopped'
require_text '--kill-after=10s'
expected_uid=${HOST_UID:-1000}
expected_gid=${HOST_GID:-1000}
if ! grep -F -- "user: ${expected_uid}:${expected_gid}" "$rendered" >/dev/null \
  && ! grep -F -- "user: \"${expected_uid}:${expected_gid}\"" "$rendered" >/dev/null; then
  echo "normalized Compose model is missing the configured service user: \
${expected_uid}:${expected_gid}" >&2
  exit 1
fi
if ! grep -F 'user: "${HOST_UID:-1000}:${HOST_GID:-1000}"' "$compose_file" >/dev/null; then
  echo "smoke services must inherit the invoking host UID/GID" >&2
  exit 1
fi
require_text 'SMOKE_WORKER_READY_FILE'
require_text 'test -s /tmp/ocaml-temporal-two-binary-worker.ready'
require_text 'stop_grace_period: 30s'

# The two-binary fixture must keep the heartbeat and timeout-retry scenarios in
# the shared definitions module and must preserve the two process roles. These
# source-level assertions are intentionally small: they catch an accidentally
# removed registration, client assertion, worker loop, or executable definition
# without requiring Docker, Temporal Server, or a built native bridge. The
# actual payload/detail and timeout semantics remain covered by the OCaml and
# Rust protocol/runtime tests and by the live Compose job when its environment
# is available.
require_source_text() {
  path=$1
  needle=$2
  if ! grep -F -- "$needle" "$path" >/dev/null; then
    echo "two-binary acceptance source is missing: $needle ($path)" >&2
    exit 1
  fi
}

# A role must not quietly acquire the other process's responsibility. Keeping
# this negative check beside the positive role assertions makes a future
# refactor fail closed if somebody replaces the independent driver with a
# worker that happens to start workflows locally, or replaces the worker with
# a client that only waits for results.
require_source_absent() {
  path=$1
  needle=$2
  if grep -F -- "$needle" "$path" >/dev/null; then
    echo "two-binary acceptance role is not isolated: $needle ($path)" >&2
    exit 1
  fi
}

definitions="$fixture/common/smoke_definitions.ml"
driver="$fixture/driver/smoke_driver.ml"
worker="$fixture/worker/smoke_worker.ml"
driver_dune="$fixture/driver/dune"
worker_dune="$fixture/worker/dune"

# The driver is an independent OCaml test client. It must use the public
# client operations to start, cancel, and await exact workflow executions;
# merely sharing workflow definitions or naming a second executable would not
# prove that it exercises the server as a client process.
require_source_text "$driver_dune" '(name smoke_driver)'
require_source_text "$driver_dune" 'temporal-sdk'
require_source_text "$driver_dune" 'temporal_two_binary_smoke_common'
require_source_text "$driver" 'module Client = Temporal.Client'
require_source_text "$driver" 'Client.start client ~workflow'
require_source_text "$driver" 'Client.cancel ~request_id:'
require_source_text "$driver" 'Client.wait handle'
require_source_absent "$driver" 'Worker.create'
require_source_absent "$driver" 'Worker.run'

# The worker is the separate implementation process. Its source must create
# the public Worker, register the shared definitions, run the native loop, and
# shut it down; a client-only executable cannot satisfy this contract.
require_source_text "$worker_dune" '(name smoke_worker)'
require_source_text "$worker_dune" 'temporal-sdk'
require_source_text "$worker_dune" 'temporal_two_binary_smoke_common'
require_source_text "$worker" 'module Worker = Temporal.Worker'
require_source_text "$worker" 'Worker.create ~target_url ~namespace'
require_source_text "$worker" 'Worker.run worker'
require_source_text "$worker" 'Worker.shutdown worker'
require_source_text "$worker" 'let publish_stopped path'
require_source_text "$worker" 'publish_stopped stopped_file'
require_source_absent "$worker" 'Client.start'
require_source_absent "$worker" 'Client.wait'

require_source_text "$definitions" 'Temporal.Activity.define_with_context ~name:"smoke.heartbeat_retry"'
require_source_text "$definitions" 'Temporal.Activity.Context.heartbeat_timeout'
require_source_text "$definitions" 'Temporal.Activity.Context.heartbeat context'
require_source_text "$definitions" 'let activity_heartbeat_retry ='
require_source_text "$driver" 'two-binary-activity-heartbeat-retry'
require_source_text "$driver" 'SMOKE:HEARTBEAT:RETRIED:SMOKE'
require_source_text "$worker" 'Worker.workflow Definitions.activity_heartbeat_retry'
require_source_text "$worker" 'Worker.activity Definitions.heartbeat_retry_activity'

require_source_text "$definitions" 'let timeout_retry_start_to_close_timeout ='
require_source_text "$definitions" \
  'Temporal.Activity.define ~name:"smoke.timeout_retry"'
require_source_text "$definitions" 'Unix.sleepf timeout_retry_first_attempt_sleep_seconds'
require_source_text "$definitions" 'let activity_timeout_retry ='
require_source_text "$definitions" \
  '~start_to_close_timeout:timeout_retry_start_to_close_timeout'
require_source_text "$definitions" '~do_not_eagerly_execute:true'
require_source_text "$driver" 'two-binary-activity-timeout-retry'
require_source_text "$driver" 'SMOKE:TIMEOUT:RETRIED:SMOKE'
require_source_text "$worker" 'Worker.workflow Definitions.activity_timeout_retry'
require_source_text "$worker" 'Worker.activity Definitions.timeout_retry_activity'

require_source_text "$definitions" 'let long_backoff_retry_policy ='
require_source_text "$definitions" \
  '~initial_interval:(Temporal.Duration.of_ms 2_000L)'
require_source_text "$definitions" \
  'Temporal.Activity.define ~name:"smoke.long_backoff_retry"'
require_source_text "$definitions" \
  'let activity_long_backoff_retry ='
require_source_text "$driver" 'two-binary-activity-long-backoff-retry'
require_source_text "$driver" 'SMOKE:BACKOFF:RETRIED:SMOKE'
require_source_text "$worker" \
  'Worker.workflow Definitions.activity_long_backoff_retry'
require_source_text "$worker" \
  'Worker.activity Definitions.long_backoff_retry_activity'

require_source_text "$definitions" \
  'Temporal.Activity.define_with_context ~name:"smoke.heartbeat_timeout_retry"'
require_source_text "$definitions" \
  'let heartbeat_timeout_retry_start_to_close_timeout ='
require_source_text "$definitions" \
  'Unix.sleepf heartbeat_timeout_retry_first_attempt_sleep_seconds'
require_source_text "$definitions" \
  'match Temporal.Activity.Context.details context'
require_source_text "$definitions" \
  'let activity_heartbeat_timeout_retry ='
require_source_text "$driver" 'two-binary-activity-heartbeat-timeout-retry'
require_source_text "$driver" 'SMOKE:HEARTBEAT_TIMEOUT:RETRIED:SMOKE'
require_source_text "$worker" \
  'Worker.workflow Definitions.activity_heartbeat_timeout_retry'
require_source_text "$worker" \
  'Worker.activity Definitions.heartbeat_timeout_retry_activity'

require_source_text "$definitions" \
  'Temporal.Activity.Retry_policy.make'
require_source_text "$definitions" \
  '~non_retryable_error_types:[ "activity" ] ()'
require_source_text "$definitions" \
  'Temporal.Activity.define ~name:"smoke.non_retryable_activity"'
require_source_text "$definitions" \
  'Temporal.Codec.encode Temporal.Codec.string'
require_source_text "$definitions" \
  'Temporal.Codec.decode Temporal.Codec.string detail'
require_source_text "$definitions" \
  'let activity_non_retryable_failure ='
require_source_text "$driver" \
  'two-binary-activity-non-retryable-failure'
require_source_text "$driver" \
  'SMOKE:ACTIVITY_NON_RETRYABLE:OBSERVED'
require_source_text "$worker" \
  'Worker.workflow Definitions.activity_non_retryable_failure'
require_source_text "$worker" \
  'Worker.activity Definitions.non_retryable_activity'

# Child retry is a distinct live boundary from activity retry. The shared
# Compose contract keeps the child workflow's one-attempt activity policy, the
# separate two-attempt child policy, and the exact second-attempt marker tied
# to both executable registrations.
require_source_text "$definitions" 'let child_activity_no_retry_policy ='
require_source_text "$definitions" \
  '~maximum_interval:(Temporal.Duration.of_ms 100L) ~maximum_attempts:1 ()'
require_source_text "$definitions" \
  'Temporal.Activity.define ~name:"smoke.child_retry_once"'
require_source_text "$definitions" 'let child_retryable_failure ='
require_source_text "$definitions" \
  'match child_activity_no_retry_policy with'
require_source_text "$definitions" 'let child_retry_policy ='
require_source_text "$definitions" 'let parent_retries_child ='
require_source_text "$definitions" \
  'Temporal.Child_workflow.execute ~retry_policy:policy'
require_source_text "$driver" 'two-binary-parent-retries-child'
require_source_text "$driver" 'SMOKE:CHILD_RETRY:ATTEMPT:2'
require_source_text "$worker" \
  'Worker.workflow Definitions.parent_retries_child'
require_source_text "$worker" \
  'Worker.activity Definitions.child_retry_activity'

# Child failure and cancellation are intentionally separate parent workflows.
# The source contract keeps both cases in the two-binary fixture without
# pretending that a Docker-backed Temporal run has been observed locally.
require_source_text "$definitions" 'let child_non_retryable_failure ='
require_source_text "$definitions" 'let parent_awaits_failed_child ='
require_source_text "$definitions" 'let child_long_running ='
require_source_text "$definitions" 'let parent_cancels_child ='
require_source_text "$definitions" 'Child_workflow.Wait_cancellation_requested'
require_source_text "$definitions" 'Temporal.Child_workflow.cancel ~reason:'
require_source_text "$driver" 'two-binary-parent-awaits-failed-child'
require_source_text "$driver" 'two-binary-parent-cancels-child'
require_source_text "$driver" 'require_non_retryable_child_failure'
require_source_text "$driver" 'SMOKE:CHILD:CANCELLED'
require_source_text "$worker" 'Worker.workflow Definitions.parent_awaits_failed_child'
require_source_text "$worker" 'Worker.workflow Definitions.parent_cancels_child'

# Child start rejection is kept as a separate live boundary: the parent uses
# the already-running top-level cancellation ID, checks Core's typed metadata,
# and exposes a stable marker only after the rejection has crossed the bridge.
require_source_text "$definitions" \
  'let child_start_conflict_id = "two-binary-long-running-cancellation"'
require_source_text "$definitions" \
  'let parent_observes_child_start_failure ='
require_source_text "$definitions" \
  'Temporal.Child_workflow.execute ~id:child_start_conflict_id'
require_source_text "$definitions" 'view.category = `Child_workflow'
require_source_text "$definitions" 'SMOKE:CHILD:START_FAILED'
require_source_text "$driver" \
  'two-binary-parent-observes-child-start-failure'
require_source_text "$driver" 'SMOKE:CHILD:START_FAILED'
require_source_text "$worker" \
  'Worker.workflow Definitions.parent_observes_child_start_failure'

makefile="$root/Makefile"
if ! grep -F 'temporal workflow describe' "$makefile" >/dev/null; then
  echo "failure diagnostics must use the official Temporal CLI workflow describe command" >&2
  exit 1
fi
if ! grep -F 'up --force-recreate --detach --build --wait smoke-worker' "$makefile" >/dev/null; then
  echo "worker startup must recreate the container before relying on its /tmp readiness marker" >&2
  exit 1
fi
if ! grep -E 'run .*--no-deps smoke-driver' "$makefile" >/dev/null; then
  echo "the starter/assertion binary must run independently without creating another worker" >&2
  exit 1
fi
if ! grep -F 'down --volumes --remove-orphans' "$makefile" >/dev/null; then
  echo "integration cleanup must remove the PostgreSQL data volume" >&2
  exit 1
fi
if ! grep -F 'trap cleanup EXIT' "$makefile" >/dev/null; then
  echo "integration cleanup must run from an exit trap on success and failure" >&2
  exit 1
fi
if ! grep -F '$(MAKE) temporal-clean;' "$makefile" >/dev/null \
  || ! grep -F '$(MAKE) temporal-clean || true;' "$makefile" >/dev/null; then
  echo "integration setup and its failure trap must both invoke temporal-clean" >&2
  exit 1
fi
for target in temporal-start temporal-start-worker temporal-run-driver temporal-inspect-smoke temporal-stop-worker temporal-health temporal-status temporal-logs temporal-stop temporal-clean test-temporal-worker-readiness-contract test-temporal-worker-stop-contract test-temporal-two-binary test-temporal-integration test-temporal-worker-restart test-temporal-worker-restart-live; do
  if ! grep -E "^${target}:" "$makefile" >/dev/null; then
    echo "Makefile does not define required target: $target" >&2
    exit 1
  fi
done

if ! grep -F 'schema_version' "$makefile" >/dev/null; then
  echo "temporal-health must verify the initialized Temporal SQL schema" >&2
  exit 1
fi

if ! grep -F 'OCAML_IMAGE=$(OCAML_IMAGE) HOST_UID=$(HOST_UID) HOST_GID=$(HOST_GID)' "$makefile" >/dev/null; then
  echo "Temporal Compose commands must propagate the OCaml image and host UID/GID" >&2
  exit 1
fi

# The expensive live stack belongs in exactly one standalone CI job. It is
# deliberately not copied into the OCaml compiler/architecture matrix.
workflow="$root/.github/workflows/build.yml"
if [ "$(grep -Fc '  temporal-integration:' "$workflow")" -ne 1 ]; then
  echo "GitHub Actions must define one standalone Temporal integration job" >&2
  exit 1
fi
require_workflow_text() {
  needle=$1
  if ! grep -F -- "$needle" "$workflow" >/dev/null; then
    echo "GitHub Actions Temporal integration job is missing: $needle" >&2
    exit 1
  fi
}
require_workflow_text 'name: Temporal/PostgreSQL integration smoke (OCaml 5.5)'
require_workflow_text 'OCAML_VERSION: "5.5"'
require_workflow_text 'make test-temporal-integration'
require_workflow_text 'make test-temporal-worker-restart'
require_workflow_text 'make --silent cargo-metadata'

require_absent() {
  path=$1
  if [ -e "$path" ]; then
    echo "Temporal integration fixture must not remain at legacy path: $path" >&2
    exit 1
  fi
}

require_absent "$root/compose.yaml"
require_absent "$root/config/temporal"
require_absent "$root/scripts/check-temporal-stack.sh"
require_absent "$root/scripts/setup-temporal-postgres.sh"
test -x "$fixture/scripts/setup-temporal-postgres.sh"
test -x "$fixture/scripts/check-temporal-stack.sh"
test -x "$fixture/scripts/validate-restart-replay-identity.sh"

if ! grep -F 'test/integration/temporal' "$makefile" >/dev/null; then
  echo "root Make entrypoints must select the nested Compose fixture" >&2
  exit 1
fi
