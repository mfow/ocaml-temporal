#!/bin/sh
set -eu

# This contract test models the stale-log case without requiring Docker. It
# leaves a previous run's successful log line in place, then proves that the
# current run is rejected until its separate shutdown marker is published.
root=${1:-$(CDPATH="" cd -- "$(dirname "$0")/../.." && pwd)}
checker="$root/test/integration/temporal/scripts/check-worker-stop-marker.sh"
temporary_directory=$(mktemp -d "${TMPDIR:-/tmp}/ocaml-temporal-worker-stop.XXXXXX")
trap 'rm -rf "$temporary_directory"' EXIT HUP INT TERM

printf '%s\n' 'two-binary worker stopped cleanly' >"$temporary_directory/stale.log"
if ! grep -F -- 'two-binary worker stopped cleanly' "$temporary_directory/stale.log" \
    >/dev/null; then
  echo "the stale-log setup did not contain the previous success marker" >&2
  exit 1
fi
if "$checker" "$temporary_directory/worker-stopped" \
    >"$temporary_directory/missing-marker.out" 2>&1; then
  echo "stale aggregate logs must not satisfy worker-stop validation" >&2
  cat "$temporary_directory/missing-marker.out" >&2
  exit 1
fi

printf '%s\n' worker-stopped >"$temporary_directory/worker-stopped"
"$checker" "$temporary_directory/worker-stopped"
echo "worker stop marker contract: ok"
