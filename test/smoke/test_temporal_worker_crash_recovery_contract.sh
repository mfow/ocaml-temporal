#!/bin/sh
set -eu

# This Docker-free contract protects the crash-recovery mode of the live
# restart controller. The live target must replace a worker with SIGKILL,
# record the non-graceful exit, and validate replay using the same exact-run
# history and terminal-result checks as the graceful restart gate.
root=${1:-$(CDPATH="" cd -- "$(dirname "$0")/../.." && pwd)}
makefile="$root/Makefile"
controller_validator="$root/test/integration/temporal/scripts/validate-restart-replay-controller.sh"
fixture="$root/test/integration/temporal/fixtures/restart-replay/controller.json"
temporary_directory=$(mktemp -d "${TMPDIR:-/tmp}/ocaml-temporal-worker-crash.XXXXXX")
trap 'rm -rf "$temporary_directory"' EXIT HUP INT TERM

# Reports a focused failure when a required crash-mode source invariant is
# absent. These checks keep the CI-only Docker orchestration from silently
# regressing to a graceful stop while the ordinary restart test still passes.
require_source() {
  needle=$1
  if ! grep -F -- "$needle" "$makefile" >/dev/null; then
    echo "crash recovery source is missing: $needle" >&2
    exit 1
  fi
}

# Invokes the shared controller validator and fails if an intentionally invalid
# document is accepted. The validator is the same executable used by the live
# Makefile target, so this contract exercises its mode-specific rejection path.
expect_failure() {
  if "$@" >/dev/null 2>&1; then
    echo "expected command to fail: $*" >&2
    exit 1
  fi
}

[ -r "$controller_validator" ]
[ -r "$fixture" ]
command -v jq >/dev/null 2>&1 || {
  echo "jq is required for the crash recovery contract" >&2
  exit 1
}

require_source 'test-temporal-worker-crash-recovery:'
require_source 'TEMPORAL_WORKER_RESTART_MODE=crash $(MAKE) test-temporal-worker-restart-live'
require_source 'docker kill --signal KILL'
require_source 'generation_one_exit_code'
require_source 'if [ -e "$$SMOKE_WORKER_STOPPED_FILE" ]'
require_source 'replacement_mode'
require_source 'generation_one_replaced'

# The existing fixture remains the graceful baseline. Its new explicit mode
# field proves that the shared schema is closed and that the default validator
# behavior remains backward-compatible for the original acceptance path.
sh "$controller_validator" --controller "$fixture" \
  --workflow-id two-binary-worker-restart-replay \
  --run-id 11111111-1111-4111-8111-111111111111 \
  --replacement-mode graceful >/dev/null

# Derive a payload-free crash controller record from the checked-in baseline.
# The changed fields model only what the live Docker controller observes after
# SIGKILL: exit status 137 and no graceful worker marker.
crash_controller="$temporary_directory/controller-crash.json"
jq '.events[4] = (.events[4]
  | .step = "generation_one_replaced"
  | .replacement_mode = "crash"
  | .exit_code = 137
  | .shutdown_marker = false)' \
  "$fixture" >"$crash_controller"

sh "$controller_validator" --controller "$crash_controller" \
  --workflow-id two-binary-worker-restart-replay \
  --run-id 11111111-1111-4111-8111-111111111111 \
  --replacement-mode crash >/dev/null
invalid_crash_controller="$temporary_directory/controller-invalid-crash.json"
jq '.events[4].exit_code = 0 | .events[4].shutdown_marker = true' \
  "$crash_controller" >"$invalid_crash_controller"
expect_failure sh "$controller_validator" --controller "$invalid_crash_controller" \
  --workflow-id two-binary-worker-restart-replay \
  --run-id 11111111-1111-4111-8111-111111111111 \
  --replacement-mode crash
expect_failure sh "$controller_validator" --controller "$crash_controller" \
  --workflow-id two-binary-worker-restart-replay \
  --run-id 11111111-1111-4111-8111-111111111111 \
  --replacement-mode graceful
expect_failure sh "$controller_validator" --controller "$fixture" \
  --workflow-id two-binary-worker-restart-replay \
  --run-id 11111111-1111-4111-8111-111111111111 \
  --replacement-mode crash

echo "temporal worker crash recovery contract: ok"
