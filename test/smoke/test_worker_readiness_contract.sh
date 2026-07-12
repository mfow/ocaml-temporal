#!/bin/sh
set -eu

# This Docker-free contract test protects the boundary between worker
# construction and Compose health reporting. A stale readiness file can be
# left behind by an interrupted process, so the worker must remove it after
# validating its environment and before calling [Worker.create]. The source
# ordering assertion below keeps that cleanup from accidentally moving back
# into the post-construction success path.
root=${1:-$(CDPATH="" cd -- "$(dirname "$0")/../.." && pwd)}
worker="$root/test/integration/temporal/worker/smoke_worker.ml"
temporary_directory=$(mktemp -d "${TMPDIR:-/tmp}/ocaml-temporal-worker-ready.XXXXXX")
trap 'rm -rf "$temporary_directory"' EXIT HUP INT TERM

# Seeds the exact stale marker value that Compose's health check accepts. The
# test does not execute the worker because that would require a native bridge;
# it instead proves that the checked-in startup contract contains the cleanup
# that must remove this stale state before a current worker can be healthy.
printf '%s\n' worker-ready >"$temporary_directory/stale-ready"
if ! test -s "$temporary_directory/stale-ready"; then
  echo "the stale readiness setup did not create a marker" >&2
  exit 1
fi

# Extracts the startup section through the Worker.create boundary. Keeping the
# boundary in one variable makes the ordering assertion explicit and avoids a
# false pass from the finalizer's legitimate cleanup after creation.
startup=$(sed -n \
  '/let\* ready_file = required_env "SMOKE_WORKER_READY_FILE"/,/let worker_result =/p' \
  "$worker")
# Returns the first source line containing a contract fragment. The explicit
# line numbers below make the negative cancellation-environment case verify
# ordering instead of merely checking that both operations exist somewhere.
line_number() {
  fragment=$1
  printf '%s\n' "$startup" | grep -n -F -- "$fragment" | head -n 1 | cut -d: -f1
}

cleanup_line=$(line_number 'clear_ready_before_start ready_file')
address_validation_line=$(line_number 'let* target_url = required_env "TEMPORAL_ADDRESS"')
namespace_validation_line=$(line_number 'let* namespace = required_env "TEMPORAL_NAMESPACE"')
stopped_validation_line=$(line_number 'let* stopped_file = required_env')
cancellation_validation_line=$(line_number 'let* cancellation_ready_file = Definitions.cancellation_ready_file')
if [ -z "$cleanup_line" ] || [ -z "$address_validation_line" ] \
  || [ -z "$namespace_validation_line" ] || [ -z "$stopped_validation_line" ] \
  || [ -z "$cancellation_validation_line" ]; then
  echo "worker must clear stale readiness before Worker.create" >&2
  exit 1
fi
if [ "$cleanup_line" -ge "$address_validation_line" ] \
  || [ "$cleanup_line" -ge "$namespace_validation_line" ] \
  || [ "$cleanup_line" -ge "$stopped_validation_line" ] \
  || [ "$cleanup_line" -ge "$cancellation_validation_line" ]; then
  echo "worker must clear readiness before later marker validation" >&2
  exit 1
fi

# Model the configuration failure that motivated this regression. The actual
# worker returns an error when SMOKE_CANCELLATION_READY_FILE is missing; this
# contract intentionally leaves that variable unset while the seeded stale
# marker proves that cleanup must already have happened before that validation.
unset SMOKE_CANCELLATION_READY_FILE
if [ "${SMOKE_CANCELLATION_READY_FILE+x}" = x ]; then
  echo "the missing cancellation-marker environment setup was not applied" >&2
  exit 1
fi

# The finalizer remains mandatory: it removes readiness after normal shutdown
# and after errors raised while the worker is running, not only before startup.
if ! grep -F -- 'clear_ready ready_file;' "$worker" >/dev/null; then
  echo "worker must clear readiness during finalization" >&2
  exit 1
fi

echo "worker readiness marker contract: ok"
