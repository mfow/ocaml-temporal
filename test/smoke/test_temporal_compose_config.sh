#!/bin/sh
set -eu

# This test deliberately inspects Compose's normalized model instead of the
# source YAML. That catches invalid interpolation and dependency/profile
# combinations before the much slower live integration smoke pulls images.
root=${1:-$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)}
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
require_text 'TEMPORAL_TWO_BINARY_LIVE: "1"'
require_text 'smoke_worker.exe'
require_text 'smoke_driver.exe'
require_text '--build-dir=/workspace/_build/smoke-worker'
require_text '--build-dir=/workspace/_build/smoke-driver'
require_text 'SMOKE_DRIVER_TIMEOUT_SECONDS: "120"'
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

# The two-binary fixture must keep the heartbeat scenario in the shared
# definitions module and must preserve the two process roles. These
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
require_source_text "$driver_dune" 'temporal_two_binary_smoke_common'
require_source_text "$driver" 'module Client = Temporal.Client'
require_source_text "$driver" 'Client.start client ~workflow'
require_source_text "$driver" 'Client.cancel ~request_id:'
require_source_text "$driver" 'Client.wait handle'

# The worker is the separate implementation process. Its source must create
# the public Worker, register the shared definitions, run the native loop, and
# shut it down; a client-only executable cannot satisfy this contract.
require_source_text "$worker_dune" '(name smoke_worker)'
require_source_text "$worker_dune" 'temporal_two_binary_smoke_common'
require_source_text "$worker" 'module Worker = Temporal.Worker'
require_source_text "$worker" 'Worker.create ~target_url ~namespace'
require_source_text "$worker" 'Worker.run worker'
require_source_text "$worker" 'Worker.shutdown worker'
require_source_text "$worker" 'let publish_stopped path'
require_source_text "$worker" 'publish_stopped stopped_file'

require_source_text "$definitions" 'Temporal.Activity.define_with_context ~name:"smoke.heartbeat_retry"'
require_source_text "$definitions" 'Temporal.Activity.Context.heartbeat_timeout'
require_source_text "$definitions" 'Temporal.Activity.Context.heartbeat context'
require_source_text "$definitions" 'let activity_heartbeat_retry ='
require_source_text "$driver" 'two-binary-activity-heartbeat-retry'
require_source_text "$driver" 'SMOKE:HEARTBEAT:RETRIED:SMOKE'
require_source_text "$worker" 'Worker.workflow Definitions.activity_heartbeat_retry'
require_source_text "$worker" 'Worker.activity Definitions.heartbeat_retry_activity'

makefile="$root/Makefile"
if ! grep -F 'temporal workflow describe' "$makefile" >/dev/null; then
  echo "failure diagnostics must use the official Temporal CLI workflow describe command" >&2
  exit 1
fi
for target in temporal-start temporal-start-worker temporal-run-driver temporal-inspect-smoke temporal-stop-worker temporal-health temporal-status temporal-logs temporal-stop temporal-clean test-temporal-worker-readiness-contract test-temporal-worker-stop-contract test-temporal-two-binary test-temporal-integration; do
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
require_workflow_text 'run: make test-temporal-integration'
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

if ! grep -F 'test/integration/temporal' "$makefile" >/dev/null; then
  echo "root Make entrypoints must select the nested Compose fixture" >&2
  exit 1
fi
