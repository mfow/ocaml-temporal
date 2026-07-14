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
scratch_dir=''
driver_log=''
driver_log_printed=false

diagnostics_file="$fixture/.cache-eviction-diagnostics.json"
accepted_file="$fixture/.cache-eviction-accepted"
release_file="$fixture/.cache-eviction-release"
pressure_file="$fixture/.cache-eviction-pressure"
result_file="$fixture/.cache-eviction-result"
legacy_driver_log="$fixture/.cache-eviction-driver.log"
initial_history="$fixture/.cache-eviction-history.initial.json"
terminal_history="$fixture/.cache-eviction-history.terminal.json"
normalizer="$fixture/scripts/normalize-history.sh"
identity_validator="$fixture/scripts/validate-restart-replay-identity.sh"
validator="$fixture/scripts/validate-cache-eviction.sh"
# Supported POSIX shells measure [ulimit -f] blocks differently (512 bytes on
# Linux dash and 1024 bytes on macOS sh). These values therefore cap raw output
# at no more than 8 MiB, 1 MiB, and 64 MiB respectively on either platform;
# exact post-write byte checks below remain the portable contract.
history_response_limit_blocks=8192
describe_response_limit_blocks=1024
# This log includes an initial Compose image build as well as runtime output.
# Keep its finite cap comfortably above normal cold-build verbosity while still
# preventing a runaway client/BuildKit stream from exhausting host storage.
driver_log_limit_blocks=65536

# Prints only the final 64 KiB of the one-shot driver log. Failure output must
# remain useful on a hosted runner, but a line-count cap alone would still let
# one application-controlled line make CI output arbitrarily large.
print_driver_log() {
  [ "$driver_log_printed" = false ] || return 0
  driver_log_printed=true
  tail -c 65536 "$driver_log" 2>/dev/null || true
}

cleanup_driver() {
  if [ -n "$driver_pid" ] && kill -0 "$driver_pid" 2>/dev/null; then
    kill -TERM "$driver_pid" 2>/dev/null || true
    wait "$driver_pid" 2>/dev/null || true
  fi
  driver_pid=''
  docker rm -f "$driver_container" >/dev/null 2>&1 || true
  compose rm --stop --force smoke-cache-eviction-driver >/dev/null 2>&1 || true
}

# Removes the private directory that can temporarily contain unprojected
# Temporal responses. It is intentionally outside the bind-mounted checkout:
# an uncatchable process death can leave only an OS-temporary file, never an
# application payload in repository evidence.
cleanup_scratch() {
  [ -z "$scratch_dir" ] && return 0
  if ! rm -rf "$scratch_dir"; then
    echo "cache-eviction scratch cleanup failed" >&2
    return 1
  fi
  scratch_dir=''
}

cleanup() {
  status=$?
  trap - EXIT HUP INT TERM
  if [ -n "$release_tmp" ] && ! rm -f "$release_tmp"; then
    echo "cache-eviction release-token cleanup failed" >&2
    [ "$status" -ne 0 ] || status=1
  fi
  cleanup_driver
  if [ "$status" -ne 0 ]; then
    print_driver_log
    make_target temporal-logs || true
  fi
  if ! cleanup_scratch; then
    [ "$status" -ne 0 ] || status=1
  fi
  if ! make_target temporal-clean; then
    echo "cache-eviction Compose cleanup failed" >&2
    [ "$status" -ne 0 ] || status=1
  fi
  exit "$status"
}

trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

# Allocates the only location that may hold raw CLI JSON. A fixed path below
# [/tmp] avoids trusting [TMPDIR], which a caller could otherwise point back at
# the bind-mounted repository. [mktemp] creates this directory with restrictive
# permissions; [chmod] makes that requirement explicit for unusual platforms.
if ! scratch_dir=$(mktemp -d /tmp/ocaml-temporal-cache-eviction.XXXXXX); then
  echo "cache-eviction could not create private scratch storage" >&2
  exit 1
fi
if ! chmod 700 "$scratch_dir"; then
  echo "cache-eviction could not secure private scratch storage" >&2
  exit 1
fi
driver_log="$scratch_dir/driver.log"

# Tests a response size without emitting its contents. The live fixture needs
# only a small history/describe document; rejecting unusually large input keeps
# a malformed server response from consuming unbounded parser resources.
is_bounded_file() {
  path=$1
  maximum_bytes=$2
  [ -f "$path" ] && [ -r "$path" ] || return 1
  bytes=$(wc -c <"$path" | tr -d '[:space:]') || return 1
  case "$bytes" in
    ''|*[!0-9]*) return 1 ;;
  esac
  [ "$bytes" -le "$maximum_bytes" ]
}

# Normalizes only event IDs and type names after each CLI query. The raw JSON
# exists only in private scratch storage and is removed after every attempt;
# the saved acceptance evidence in the repository remains payload-free.
query_history() {
  destination=$1
  raw_history="$scratch_dir/history.json"
  if ! (
    ulimit -f "$history_response_limit_blocks" || exit 125
    compose run --rm --no-deps temporal-admin-tools \
      temporal workflow show --workflow-id "$workflow_id" --run-id "$run_id" \
      --namespace temporal-sdk-test --output json
  ) >"$raw_history" 2>/dev/null; then
    rm -f "$raw_history" || true
    return 1
  fi
  if ! is_bounded_file "$raw_history" 8388608; then
    rm -f "$raw_history" || true
    return 1
  fi
  if ! sh "$normalizer" --workflow-id "$workflow_id" --run-id "$run_id" \
    --output "$destination" <"$raw_history" >/dev/null 2>&1; then
    rm -f "$raw_history" || true
    return 1
  fi
  rm -f "$raw_history"
}

# Establishes the target's durable timer before pressure is allowed. The same
# validator is exercised by the Docker-free contract, so this polling path
# cannot drift from the checked cache-eviction invariants.
initial_history_is_pending() {
  sh "$validator" --stage initial --initial-history "$initial_history" \
    --workflow-id "$workflow_id" --run-id "$run_id" >/dev/null 2>&1
}

# This exactly mirrors the schema/validator's three required private records,
# but is usable while the target timer is still pending. It makes a malformed
# or unrelated diagnostic file fail before the driver result is accepted.
cache_evidence_is_complete() {
  [ -s "$diagnostics_file" ] && is_bounded_file "$diagnostics_file" 1048576 \
    && jq -e \
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
      print_driver_log
      echo "cache-eviction driver exited before $description" >&2
      return 1
    fi
    sleep 1
  done
  echo "cache-eviction driver did not publish $description" >&2
  return 1
}

# Begin by purging the legacy raw artifacts independently of Compose teardown.
# Old controller versions wrote these unprojected CLI documents in the checkout;
# removal must not depend on a Docker failure path reaching Make's cleanup rule.
if ! rm -f "$diagnostics_file" "$accepted_file" "$release_file" "$pressure_file" \
  "$result_file" "$legacy_driver_log" "$initial_history" "$terminal_history" \
  "${initial_history}.raw" "${terminal_history}.raw" \
  "${initial_history}.describe.json" "${terminal_history}.describe.json"; then
  echo "cache-eviction could not purge legacy acceptance artifacts" >&2
  exit 1
fi
make_target temporal-clean

make_target temporal-start
make_target temporal-start-cache-eviction-worker
docker rm -f "$driver_container" >/dev/null 2>&1 || true
(
  ulimit -f "$driver_log_limit_blocks" || exit 125
  compose run --build --rm --name "$driver_container" --no-deps \
    smoke-cache-eviction-driver
) >"$driver_log" 2>&1 &
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

describe_file="$scratch_dir/describe.json"
if ! (
  ulimit -f "$describe_response_limit_blocks" || exit 125
  compose run --rm --no-deps temporal-admin-tools \
    temporal workflow describe --workflow-id "$workflow_id" --run-id "$run_id" \
    --namespace temporal-sdk-test --output json
) >"$describe_file" 2>/dev/null; then
  rm -f "$describe_file" || true
  echo "cache-eviction workflow identity lookup failed" >&2
  exit 1
fi
if ! is_bounded_file "$describe_file" 1048576 \
  || ! sh "$identity_validator" --input "$describe_file" \
    --workflow-id "$workflow_id" --run-id "$run_id" >/dev/null 2>&1; then
  rm -f "$describe_file" || true
  echo "cache-eviction workflow identity did not match the accepted run" >&2
  exit 1
fi
if ! rm -f "$describe_file"; then
  echo "cache-eviction could not remove private identity response" >&2
  exit 1
fi

initial_ready=false
for attempt in $(seq 1 120); do
  if query_history "$initial_history" && initial_history_is_pending; then
    initial_ready=true
    break
  fi
  sleep 1
done
if [ "$initial_ready" != true ]; then
  # Polling suppresses expected intermediate states. Re-run once without
  # suppression so a final failure names the bounded contract violation rather
  # than leaving only a generic timeout in the CI log.
  sh "$validator" --stage initial --initial-history "$initial_history" \
    --workflow-id "$workflow_id" --run-id "$run_id" || true
  echo "cache-eviction target never reached its pending timer boundary" >&2
  exit 1
fi

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
    print_driver_log
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
if [ "$terminal_ready" != true ]; then
  # As above, expose the validator's bounded reason only after the retry budget
  # is exhausted. It reads normalized history and schema-shaped diagnostics,
  # never raw Temporal history or workflow payloads.
  sh "$validator" --diagnostics "$diagnostics_file" \
    --initial-history "$initial_history" --terminal-history "$terminal_history" \
    --workflow-id "$workflow_id" --run-id "$run_id" || true
  echo "cache-eviction terminal history did not satisfy the exact contract" >&2
  exit 1
fi

make_target temporal-stop-cache-eviction-worker
compose down --volumes --remove-orphans >/dev/null
remaining_volumes=$(docker volume ls -q \
  --filter "label=com.docker.compose.project=$project" | wc -l | tr -d ' ')
[ "$remaining_volumes" -eq 0 ] || {
  echo "cache-eviction cleanup retained Compose volumes" >&2
  exit 1
}

# The EXIT trap removes private scratch and the Compose stack together. The CI
# job's zero exit status is the durable test record; no raw CLI document is
# retained in the repository after either a success or a recoverable failure.
exit 0
