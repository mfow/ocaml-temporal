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
if ! printf '%s\n' "$startup" | grep -F -- 'clear_ready_before_start ready_file' >/dev/null; then
  echo "worker must clear stale readiness before Worker.create" >&2
  exit 1
fi

# The finalizer remains mandatory: it removes readiness after normal shutdown
# and after errors raised while the worker is running, not only before startup.
if ! grep -F -- 'clear_ready ready_file;' "$worker" >/dev/null; then
  echo "worker must clear readiness during finalization" >&2
  exit 1
fi

echo "worker readiness marker contract: ok"
