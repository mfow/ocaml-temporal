#!/bin/sh
set -eu

# Protects the focused live cache-eviction gate without requiring Docker. The
# contract checks the public configuration seam, the separate client driver,
# the one-slot controller, and the closed payload-free marker shape used after
# a real Core RemoveFromCache activation.
root=${1:-$(CDPATH="" cd -- "$(dirname "$0")/../.." && pwd)}
makefile="$root/Makefile"
worker="$root/test/integration/temporal/worker/smoke_worker.ml"
driver="$root/test/integration/temporal/driver/cache_eviction_driver.ml"
driver_dune="$root/test/integration/temporal/driver/dune"
fixture="$root/test/integration/temporal/fixtures/cache-eviction/marker.json"
temporary_directory=$(mktemp -d "${TMPDIR:-/tmp}/ocaml-temporal-cache-eviction.XXXXXX")
trap 'rm -rf "$temporary_directory"' EXIT HUP INT TERM

require_source() {
  path=$1
  needle=$2
  if ! grep -F -- "$needle" "$path" >/dev/null; then
    echo "cache eviction contract is missing: $needle ($path)" >&2
    exit 1
  fi
}

require_source "$makefile" 'test-temporal-worker-cache-eviction:'
require_source "$makefile" 'SMOKE_WORKER_MAX_CACHED_WORKFLOWS=1'
require_source "$makefile" 'SMOKE_WORKER_CACHE_EVICTION_FILE='
require_source "$makefile" 'SMOKE_REPLAY_WORKFLOW_ID=two-binary-cache-eviction-a'
require_source "$makefile" 'smoke-cache-eviction-driver'
require_source "$worker" 'Worker.create ?max_cached_workflows'
require_source "$worker" 'Worker.workflow Definitions.cache_eviction'
require_source "$driver_dune" '(name cache_eviction_driver)'
require_source "$driver" 'wait_for_marker'
require_source "$driver" 'two-binary-cache-eviction-a'
require_source "$driver" 'two-binary-cache-eviction-b'
require_source "$driver" 'Client.cancel ~request_id ~reason:'
require_source "$driver" 'Client.wait first'
require_source "$driver" 'Client.wait second'
require_source "$root/lib/public/worker.mli" '?max_cached_workflows:int'
require_source "$root/lib/runtime/native_worker_execution.ml" 'cache_removal_reason'

command -v jq >/dev/null 2>&1 || {
  echo "jq is required for the cache eviction contract" >&2
  exit 1
}

jq -e '
  type == "object"
  and (keys | sort) == ["reason", "run_id", "workflow_id"]
  and (.workflow_id == "two-binary-cache-eviction-a")
  and (.run_id | type == "string" and length > 0)
  and (.reason == "cache_full")
' "$fixture" >/dev/null

invalid="$temporary_directory/invalid-marker.json"
jq '.reason = "lang_requested"' "$fixture" >"$invalid"
if jq -e '.reason == "cache_full"' "$invalid" >/dev/null 2>&1; then
  echo "cache eviction marker contract accepted the wrong eviction reason" >&2
  exit 1
fi

echo "temporal worker cache eviction contract: ok"
