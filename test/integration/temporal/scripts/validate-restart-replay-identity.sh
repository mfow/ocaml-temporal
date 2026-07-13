#!/bin/sh
set -eu

# Validates the exact workflow/run identity returned by Temporal's machine-
# readable [workflow describe] response. History output does not carry the
# current run identity in every CLI shape, so the live restart controller keeps
# this check separate from its payload-free history normalizer.

usage() {
  echo "usage: validate-restart-replay-identity.sh --input FILE --workflow-id ID --run-id ID" >&2
  exit 2
}

fail() {
  echo "restart/replay identity validation failed: $*" >&2
  exit 1
}

input=''
workflow_id=''
run_id=''
while [ "$#" -gt 0 ]; do
  case "$1" in
    --input)
      [ "$#" -ge 2 ] || usage
      input=$2
      shift 2
      ;;
    --workflow-id)
      [ "$#" -ge 2 ] || usage
      workflow_id=$2
      shift 2
      ;;
    --run-id)
      [ "$#" -ge 2 ] || usage
      run_id=$2
      shift 2
      ;;
    --help|-h)
      usage
      ;;
    *)
      usage
      ;;
  esac
done

[ -n "$input" ] || usage
[ -n "$workflow_id" ] || usage
[ -n "$run_id" ] || usage
[ -r "$input" ] || fail "describe response is not readable: $input"

jq_bin=${JQ_BIN:-jq}
command -v "$jq_bin" >/dev/null 2>&1 || fail "jq is required (set JQ_BIN to its path)"

# Temporal CLI releases have used camelCase and snake_case spellings for the
# protobuf-JSON envelope, but the execution identifiers are always the same
# two strings. We accept only those documented envelope variants and compare
# both values exactly; a response for another run is never good enough.
if ! "$jq_bin" -e \
  --arg expected_workflow "$workflow_id" \
  --arg expected_run "$run_id" \
  '
    def field($object; $camel; $snake):
      ($object[$camel] // $object[$snake]);
    . as $root
    | ($root.workflowExecutionInfo // $root.workflow_execution_info) as $info
    | ($info | type == "object")
    | ($info.execution // $info.workflow_execution) as $execution
    | ($execution | type == "object")
    | (field($execution; "workflowId"; "workflow_id") == $expected_workflow)
    and (field($execution; "runId"; "run_id") == $expected_run)
  ' "$input" >/dev/null; then
  fail "describe response does not identify the expected workflow/run"
fi

printf 'restart_replay_identity workflow_id=%s run_id=%s status=ok\n' \
  "$workflow_id" "$run_id"
