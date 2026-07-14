#!/bin/sh
set -eu

# Runs the real sticky-cache CacheFull acceptance scenario. This controller is
# intentionally outside workflow code: it coordinates two OCaml binaries and
# inspects Temporal's machine-readable history, while the workflow itself uses
# only deterministic SDK operations. It succeeds only after the worker's
# private diagnostic proves Core accepted the required empty eviction
# completion and a fresh replay of the same run later completed.

root=$(CDPATH= cd -- "$(dirname "$0")/../../../.." && pwd)
fixture="$root/test/integration/temporal"
compose_file="$fixture/compose.yaml"
make_bin=${MAKE:-make}
project=${TEMPORAL_COMPOSE_PROJECT:-ocaml-temporal-integration}
ocaml_image=${OCAML_IMAGE:-ocaml/opam:debian-12-ocaml-5.2}
host_uid=${HOST_UID:-$(id -u)}
host_gid=${HOST_GID:-$(id -g)}
driver_timeout=${TEMPORAL_DRIVER_TIMEOUT_SECONDS:-300}
workflow_id=two-binary-sticky-cache-eviction-target

# Calls the public Makefile boundary with the exact Compose identity shared by
# direct controller calls below. Keeping the assignments explicit prevents an
# overridden OCaml image or project name from starting one stack and querying
# another.
make_target() {
  "$make_bin" -C "$root" \
    TEMPORAL_COMPOSE_PROJECT="$project" \
    OCAML_IMAGE="$ocaml_image" HOST_UID="$host_uid" HOST_GID="$host_gid" \
    TEMPORAL_DRIVER_TIMEOUT_SECONDS="$driver_timeout" "$@"
}

# Uses the same checked-in Compose file as Make for the few operations that
# need a background driver PID or a machine-readable Temporal CLI response.
compose() {
  OCAML_IMAGE="$ocaml_image" HOST_UID="$host_uid" HOST_GID="$host_gid" \
    SMOKE_DRIVER_TIMEOUT_SECONDS="$driver_timeout" \
    docker compose --project-directory "$fixture" --file "$compose_file" \
      --project-name "$project" --profile temporal "$@"
}

# Writes a controller marker through rename so the bind-mounted driver sees a
# complete release token or no token. No workflow-controlled data is written.
publish_release() {
  release_tmp="${release_file}.tmp.$$"
  printf 'release\n' >"$release_tmp"
  mv "$release_tmp" "$release_file"
  release_tmp=''
}

# Emits bounded failure diagnostics before the obligatory volume-removing
# teardown. The one-shot driver log and Compose logs are useful for diagnosis;
# history files are intentionally not printed because raw Temporal history can
# contain application payloads.
driver_pid=''
driver_container="${project}-smoke-cache-eviction-driver"
release_tmp=''
cleanup_driver() {
  if [ -n "$driver_pid" ] && kill -0 "$driver_pid" 2>/dev/null; then
    kill -TERM "$driver_pid" 2>/dev/null || true
    wait "$driver_pid" 2>/dev/null || true
  fi
  driver_pid=''
  docker rm -f "$driver_container" >/dev/null 2>&1 || true
  compose rm --stop --force smoke-cache-eviction-driver >/dev/null 2>&1 || true
}

cleanup() {
  status=$?
  trap - EXIT HUP INT TERM
  [ -z "$release_tmp" ] || rm -f "$release_tmp"
  cleanup_driver
  if [ "$status" -ne 0 ]; then
    cat "$driver_log" 2>/dev/null || true
    make_target temporal-logs || true
  fi
  make_target temporal-clean || true
  exit "$status"
}

trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

diagnostics_file="$fixture/.cache-eviction-diagnostics.json"
accepted_file="$fixture/.cache-eviction-accepted"
release_file="$fixture/.cache-eviction-release"
pressure_file="$fixture/.cache-eviction-pressure"
result_file="$fixture/.cache-eviction-result"
driver_log="$fixture/.cache-eviction-driver.log"
initial_history="$fixture/.cache-eviction-history.initial.json"
terminal_history="$fixture/.cache-eviction-history.terminal.json"
normalizer="$fixture/scripts/normalize-history.sh"
identity_validator="$fixture/scripts/validate-restart-replay-identity.sh"
validator="$fixture/scripts/validate-cache-eviction.sh"

# Normalizes only event IDs and type names after each CLI query. The raw JSON
# is short-lived and removed by cleanup; the saved acceptance evidence remains
# payload-free.
query_history() {
  destination=$1
  raw_history="${destination}.raw"
  if ! compose run --rm --no-deps temporal-admin-tools \
    temporal workflow show --workflow-id "$workflow_id" --run-id "$run_id" \
    --namespace temporal-sdk-test --output json >"$raw_history" 2>/dev/null; then
    return 1
  fi
  sh "$normalizer" --workflow-id "$workflow_id" --run-id "$run_id" \
    --output "$destination" <"$raw_history"
}

# Establishes the target's durable timer before pressure is allowed. It avoids
# guessing from a sleep: the target must have a TimerStarted event, no timer
# fire, and no terminal event when the second execution is started.
initial_history_is_pending() {
  jq -e '
    [.events[].type] as $types
    | ($types | index("WorkflowExecutionStarted") != null)
      and ($types | index("WorkflowTaskCompleted") != null)
      and ($types | index("TimerStarted") != null)
      and ($types | index("TimerFired") == null)
      and (["WorkflowExecutionCompleted", "WorkflowExecutionFailed",
            "WorkflowExecutionCanceled", "WorkflowExecutionTerminated",
            "WorkflowExecutionTimedOut", "WorkflowExecutionContinuedAsNew"]
           | all(.[] as $terminal | ($types | index($terminal) == null)))
  ' "$initial_history" >/dev/null
}

# This exactly mirrors the schema/validator's three required private records,
# but is usable while the target timer is still pending. It makes a malformed
# or unrelated diagnostic file fail before the driver result is accepted.
cache_evidence_is_complete() {
  [ -s "$diagnostics_file" ] && jq -e \
    --arg workflow_id "$workflow_id" --arg run_id "$run_id" '
      type == "object"
      and (keys | sort) == ["records", "run_id", "workflow_id"]
      and .workflow_id == $workflow_id and .run_id == $run_id
      and ([.records[] | .phase]
           == ["initial", "cache_full_acknowledged", "replay"])
      and .records[0].is_replaying == false
      and .records[1].empty_completion == true
      and .records[2].is_replaying == true
      and (.records[2].history_length | type == "string" and . != "0")
    ' "$diagnostics_file" >/dev/null 2>&1
}

# Stops immediately if the independent client exits before the awaited marker.
# A marker alone is insufficient because a stale host file is removed before
# startup and every marker still comes from the current driver process.
wait_for_marker() {
  marker=$1
  description=$2
  for attempt in $(seq 1 120); do
    if [ -s "$marker" ]; then return 0; fi
    if ! kill -0 "$driver_pid" 2>/dev/null; then
      cat "$driver_log" 2>/dev/null || true
      echo "cache-eviction driver exited before $description" >&2
      return 1
    fi
    sleep 1
  done
  echo "cache-eviction driver did not publish $description" >&2
  return 1
}

# Begin with a volume-removing cleanup so neither a previous PostgreSQL data
# volume nor a previous diagnostic can affect this run's evidence.
make_target temporal-clean
rm -f "$diagnostics_file" "$accepted_file" "$release_file" "$pressure_file" \
  "$result_file" "$driver_log" "$initial_history" "$terminal_history" \
  "${initial_history}.raw" "${terminal_history}.raw" \
  "${terminal_history}.describe.json"

make_target temporal-start
make_target temporal-start-cache-eviction-worker
docker rm -f "$driver_container" >/dev/null 2>&1 || true
compose run --build --rm --name "$driver_container" --no-deps \
  smoke-cache-eviction-driver >"$driver_log" 2>&1 &
driver_pid=$!

wait_for_marker "$accepted_file" "the target workflow identity"
accepted_workflow_id=$(sed -n 's/^workflow_id=//p' "$accepted_file" | sed -n '1p')
run_id=$(sed -n 's/^run_id=//p' "$accepted_file" | sed -n '1p')
[ "$accepted_workflow_id" = "$workflow_id" ] || {
  echo "cache-eviction driver reported an unexpected workflow ID" >&2
  exit 1
}
[ -n "$run_id" ] || {
  echo "cache-eviction driver marker has no run ID" >&2
  exit 1
}

describe_file="${terminal_history}.describe.json"
compose run --rm --no-deps temporal-admin-tools \
  temporal workflow describe --workflow-id "$workflow_id" --run-id "$run_id" \
  --namespace temporal-sdk-test --output json >"$describe_file"
sh "$identity_validator" --input "$describe_file" --workflow-id "$workflow_id" \
  --run-id "$run_id" >/dev/null

initial_ready=false
for attempt in $(seq 1 120); do
  if query_history "$initial_history" && initial_history_is_pending; then
    initial_ready=true
    break
  fi
  sleep 1
done
[ "$initial_ready" = true ] || {
  echo "cache-eviction target never reached its pending timer boundary" >&2
  exit 1
}

publish_release
wait_for_marker "$pressure_file" "the cache-pressure workflow identity"
pressure_workflow_id=$(sed -n 's/^workflow_id=//p' "$pressure_file" | sed -n '1p')
pressure_run_id=$(sed -n 's/^run_id=//p' "$pressure_file" | sed -n '1p')
[ "$pressure_workflow_id" = "${workflow_id}-pressure" ] && [ -n "$pressure_run_id" ] || {
  echo "cache-eviction driver reported malformed cache pressure" >&2
  exit 1
}

evidence_ready=false
for attempt in $(seq 1 120); do
  if cache_evidence_is_complete; then
    evidence_ready=true
    break
  fi
  if ! kill -0 "$driver_pid" 2>/dev/null && [ ! -s "$result_file" ]; then
    cat "$driver_log" 2>/dev/null || true
    echo "cache-eviction driver exited before CacheFull evidence" >&2
    exit 1
  fi
  sleep 1
done
[ "$evidence_ready" = true ] || {
  echo "worker never published accepted CacheFull replay evidence" >&2
  exit 1
}

wait_for_marker "$result_file" "the target result"
wait "$driver_pid"
driver_pid=''
grep -Fqx 'completed' "$result_file"

terminal_ready=false
for attempt in $(seq 1 120); do
  if query_history "$terminal_history" && sh "$validator" \
    --diagnostics "$diagnostics_file" --initial-history "$initial_history" \
    --terminal-history "$terminal_history" --workflow-id "$workflow_id" \
    --run-id "$run_id" >/dev/null; then
    terminal_ready=true
    break
  fi
  sleep 1
done
[ "$terminal_ready" = true ] || {
  echo "cache-eviction terminal history did not satisfy the exact contract" >&2
  exit 1
}

make_target temporal-stop-cache-eviction-worker
compose down --volumes --remove-orphans >/dev/null
remaining_volumes=$(docker volume ls -q \
  --filter "label=com.docker.compose.project=$project" | wc -l | tr -d ' ')
[ "$remaining_volumes" -eq 0 ] || {
  echo "cache-eviction cleanup retained Compose volumes" >&2
  exit 1
}

# The Make cleanup also removes short-lived raw history and diagnostic files.
# The CI job's zero exit status is the durable test record; failure output is
# deliberately retained only long enough for the failure trap above.
trap - EXIT HUP INT TERM
make_target temporal-clean
