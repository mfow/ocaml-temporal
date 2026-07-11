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

makefile="$root/Makefile"
for target in temporal-start temporal-health temporal-status temporal-logs temporal-stop temporal-clean test-temporal-integration; do
  if ! grep -E "^${target}:" "$makefile" >/dev/null; then
    echo "Makefile does not define required target: $target" >&2
    exit 1
  fi
done

if ! grep -F 'schema_version' "$makefile" >/dev/null; then
  echo "temporal-health must verify the initialized Temporal SQL schema" >&2
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
