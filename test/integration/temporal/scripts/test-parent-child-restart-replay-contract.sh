#!/bin/sh
set -eu

# Exercises the closed restart-replay evidence protocol without contacting a
# Temporal server. It creates one private directory under /tmp and removes it
# on every exit path, so local review and CI leave no build or test artifacts.

# Stops the contract with a stable diagnostic that does not print workflow data.
fail() {
  printf '%s\n' "parent-child restart-replay contract failed: $*" >&2
  exit 1
}

# Asserts that a malformed input is rejected by a closed validator boundary.
expect_failure() {
  label=$1
  shift
  if "$@" >/dev/null 2>&1; then
    fail "expected rejection: $label"
  fi
}

# Compares parsed JSON rather than formatting, preserving readable fixtures.
assert_json_equal() {
  actual=$1
  expected=$2
  "$jq_bin" -S . "$actual" >"$tmp/actual.json" || fail "invalid actual JSON"
  "$jq_bin" -S . "$expected" >"$tmp/expected.json" || fail "invalid expected JSON"
  cmp -s "$tmp/actual.json" "$tmp/expected.json" \
    || fail "normalizer result differs from fixture: $expected"
}

# Checks a schema's handwritten signed-64 expression at its maximum boundary.
assert_signed_64_pattern() {
  schema=$1
  definition=$2
  "$jq_bin" -e \
    --arg definition "$definition" \
    --arg valid '9223372036854775807' \
    --arg invalid '9223372036854775808' \
    '."$defs"[$definition].pattern as $pattern
     | ($valid | test($pattern))
       and ($invalid | test($pattern) | not)' \
    "$schema" >/dev/null \
    || fail "incorrect signed-64 boundary: $schema#$definition"
}

[ "$#" -eq 0 ] || fail "this contract takes no arguments"

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/../../../.." && pwd)
fixture_dir="$repo_root/test/integration/temporal/fixtures/parent-child-restart-replay"
schema_dir="$repo_root/docs/schemas/acceptance"
normalizer="$script_dir/normalize-parent-child-restart-replay-history.sh"
history_validator="$script_dir/validate-parent-child-restart-replay.sh"
controller_validator="$script_dir/validate-parent-child-restart-replay-controller.sh"
jq_bin=jq
set +u
if [ -n "$JQ_BIN" ]; then
  jq_bin=$JQ_BIN
fi
set -u

command -v "$jq_bin" >/dev/null 2>&1 || fail "jq is required"
for executable in "$normalizer" "$history_validator" "$controller_validator"; do
  [ -x "$executable" ] || fail "required executable is missing: $executable"
done

tmp=$(mktemp -d /tmp/ocaml-temporal-parent-child-restart-replay.XXXXXX) \
  || fail "could not create a temporary contract directory"
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

parent_workflow='two-binary-parent-child-restart'
parent_run='11111111-1111-4111-8111-111111111111'
child_workflow='two-binary-parent-child-restart-child-smoke'
child_run='22222222-2222-4222-8222-222222222222'
child_type='smoke.parent_child_restart_child'
parent_initial="$fixture_dir/parent.history.initial.json"
parent_post="$fixture_dir/parent.history.post-removal.json"
parent_terminal="$fixture_dir/parent.history.terminal.json"
child_initial="$fixture_dir/child.history.initial.json"
child_post="$fixture_dir/child.history.post-removal.json"
child_terminal="$fixture_dir/child.history.terminal.json"
diagnostics_initial="$fixture_dir/diagnostics.initial.json"
diagnostics_terminal="$fixture_dir/diagnostics.json"
controller="$fixture_dir/controller.json"

# Runs the three stage-specific relationship checks with their fixed evidence
# prerequisites. The distinct functions make negative tests concise and avoid
# accidental omission of a required snapshot.
check_initial() {
  "$history_validator" \
    --stage initial \
    --parent-history "$1" \
    --child-history "$2" \
    --diagnostics "$3" \
    --parent-workflow-id "$parent_workflow" \
    --parent-run-id "$parent_run" \
    --child-workflow-id "$child_workflow" \
    --child-run-id "$child_run"
}

check_post() {
  "$history_validator" \
    --stage post-removal \
    --parent-history "$1" \
    --child-history "$2" \
    --diagnostics "$3" \
    --parent-initial-history "$parent_initial" \
    --child-initial-history "$child_initial" \
    --parent-workflow-id "$parent_workflow" \
    --parent-run-id "$parent_run" \
    --child-workflow-id "$child_workflow" \
    --child-run-id "$child_run"
}

check_terminal() {
  "$history_validator" \
    --stage terminal \
    --parent-history "$1" \
    --child-history "$2" \
    --diagnostics "$3" \
    --parent-initial-history "$parent_initial" \
    --child-initial-history "$child_initial" \
    --parent-post-removal-history "$parent_post" \
    --child-post-removal-history "$child_post" \
    --parent-workflow-id "$parent_workflow" \
    --parent-run-id "$parent_run" \
    --child-workflow-id "$child_workflow" \
    --child-run-id "$child_run"
}

check_controller() {
  "$controller_validator" \
    --controller "$1" \
    --parent-workflow-id "$parent_workflow" \
    --parent-run-id "$parent_run" \
    --child-workflow-id "$child_workflow" \
    --child-run-id "$child_run" \
    --initiated-event-id 5
}

# All committed fixtures must be syntactically valid before cross-file
# assertions could turn a syntax defect into an opaque relationship error.
for json in \
  "$parent_initial" "$parent_post" "$parent_terminal" \
  "$child_initial" "$child_post" "$child_terminal" \
  "$diagnostics_initial" "$diagnostics_terminal" "$controller" \
  "$schema_dir/parent-child-restart-replay-history.schema.json" \
  "$schema_dir/parent-child-restart-replay-diagnostics.schema.json" \
  "$schema_dir/parent-child-restart-replay-controller.schema.json"; do
  "$jq_bin" -e . "$json" >/dev/null || fail "invalid JSON: $json"
done

check_initial "$parent_initial" "$child_initial" "$diagnostics_initial" >/dev/null
check_post "$parent_post" "$child_post" "$diagnostics_initial" >/dev/null
check_terminal "$parent_terminal" "$child_terminal" "$diagnostics_terminal" >/dev/null
check_controller "$controller" >/dev/null

# Keep every cross-link internally consistent while substituting a different
# child type. The rejection therefore proves the fixed live workflow type is
# asserted, rather than merely checking that parent and child agree with each
# other on an arbitrary type string.
"$jq_bin" '.events[4].child_workflow_type = "wrong.child.type"
           | .events[5].child_workflow_type = "wrong.child.type"' \
  "$parent_initial" >"$tmp/parent-initial-wrong-type.json"
"$jq_bin" '.events[0].workflow_type = "wrong.child.type"' \
  "$child_initial" >"$tmp/child-initial-wrong-type.json"
expect_failure "unexpected child workflow type" \
  check_initial "$tmp/parent-initial-wrong-type.json" "$tmp/child-initial-wrong-type.json" "$diagnostics_initial"

# These compact raw CLI documents exercise the adapter's payload-eliding
# projection while retaining every identity required for the protocol join.
"$jq_bin" -n \
  --arg parent_workflow "$parent_workflow" \
  --arg parent_run "$parent_run" \
  --arg child_workflow "$child_workflow" \
  --arg child_run "$child_run" \
  --arg child_type "$child_type" \
  '{
    workflowExecution: {workflowId: $parent_workflow, runId: $parent_run},
    history: {events: [
      {eventId: "1", eventType: "EVENT_TYPE_WORKFLOW_EXECUTION_STARTED",
       workflowExecutionStartedEventAttributes: {workflowId: $parent_workflow}},
      {eventId: "2", eventType: "EVENT_TYPE_WORKFLOW_TASK_SCHEDULED"},
      {eventId: "3", eventType: "EVENT_TYPE_WORKFLOW_TASK_STARTED"},
      {eventId: "4", eventType: "EVENT_TYPE_WORKFLOW_TASK_COMPLETED"},
      {eventId: "5", eventType: "EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED",
       startChildWorkflowExecutionInitiatedEventAttributes: {
         workflowId: $child_workflow, workflowType: {name: $child_type}}},
      {eventId: "6", eventType: "EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED",
       childWorkflowExecutionStartedEventAttributes: {
         workflowExecution: {workflowId: $child_workflow, runId: $child_run},
         workflowType: {name: $child_type}, initiatedEventId: "5"}},
      {eventId: "7", eventType: "EVENT_TYPE_WORKFLOW_TASK_SCHEDULED"},
      {eventId: "8", eventType: "EVENT_TYPE_WORKFLOW_TASK_STARTED"},
      {eventId: "9", eventType: "EVENT_TYPE_WORKFLOW_TASK_COMPLETED"}
    ]}
  }' >"$tmp/raw-parent.json"

"$jq_bin" -n \
  --arg parent_workflow "$parent_workflow" \
  --arg parent_run "$parent_run" \
  --arg child_workflow "$child_workflow" \
  --arg child_run "$child_run" \
  --arg child_type "$child_type" \
  '{
    workflowExecution: {workflowId: $child_workflow, runId: $child_run},
    history: {events: [
      {eventId: "1", eventType: "EVENT_TYPE_WORKFLOW_EXECUTION_STARTED",
       workflowExecutionStartedEventAttributes: {
         workflowId: $child_workflow, workflowType: {name: $child_type},
         parentWorkflowExecution: {workflowId: $parent_workflow, runId: $parent_run},
         parentInitiatedEventId: "5"}},
      {eventId: "2", eventType: "EVENT_TYPE_WORKFLOW_TASK_SCHEDULED"},
      {eventId: "3", eventType: "EVENT_TYPE_WORKFLOW_TASK_STARTED"},
      {eventId: "4", eventType: "EVENT_TYPE_WORKFLOW_TASK_COMPLETED"},
      {eventId: "5", eventType: "EVENT_TYPE_TIMER_STARTED"}
    ]}
  }' >"$tmp/raw-child.json"

normalize_parent() {
  "$normalizer" \
    --role parent \
    --workflow-id "$parent_workflow" \
    --run-id "$parent_run" \
    --counterpart-workflow-id "$child_workflow" \
    --counterpart-run-id "$child_run" \
    --output "$2" <"$1"
}

normalize_child() {
  "$normalizer" \
    --role child \
    --workflow-id "$child_workflow" \
    --run-id "$child_run" \
    --counterpart-workflow-id "$parent_workflow" \
    --counterpart-run-id "$parent_run" \
    --output "$2" <"$1"
}

normalize_parent "$tmp/raw-parent.json" "$tmp/normalized-parent.json"
assert_json_equal "$tmp/normalized-parent.json" "$parent_initial"
normalize_child "$tmp/raw-child.json" "$tmp/normalized-child.json"
assert_json_equal "$tmp/normalized-child.json" "$child_initial"

# Older CLI output can use snake_case; accepting it is limited to exactly one
# spelling for each field so a duplicate representation remains a hard error.
"$jq_bin" '
  .workflow_execution = {workflow_id: .workflowExecution.workflowId,
                         run_id: .workflowExecution.runId}
  | del(.workflowExecution)
  | .history.events[0].workflow_execution_started_event_attributes =
      (.history.events[0].workflowExecutionStartedEventAttributes
       | {workflow_id: .workflowId,
          workflow_type: {name: .workflowType.name},
          parent_workflow_execution: {
            workflow_id: .parentWorkflowExecution.workflowId,
            run_id: .parentWorkflowExecution.runId
          },
          parent_initiated_event_id: .parentInitiatedEventId})
  | del(.history.events[0].workflowExecutionStartedEventAttributes)
' "$tmp/raw-child.json" >"$tmp/raw-child-snake.json"
normalize_child "$tmp/raw-child-snake.json" "$tmp/normalized-child-snake.json"
assert_json_equal "$tmp/normalized-child-snake.json" "$child_initial"

# A failed projection must not replace a previous proof document at the output
# path, since a controller may poll it while requesting a fresh projection.
cp "$tmp/normalized-parent.json" "$tmp/normalized-parent-before-failure.json"
"$jq_bin" --arg bad_run '33333333-3333-4333-8333-333333333333' \
  '.history.events[5].childWorkflowExecutionStartedEventAttributes.workflowExecution.runId = $bad_run' \
  "$tmp/raw-parent.json" >"$tmp/raw-parent-wrong-run.json"
expect_failure "wrong child run" \
  normalize_parent "$tmp/raw-parent-wrong-run.json" "$tmp/normalized-parent.json"
cmp -s "$tmp/normalized-parent.json" "$tmp/normalized-parent-before-failure.json" \
  || fail "failed normalization replaced known-good output"

"$jq_bin" '.history.events[1].eventType = "EVENT_TYPE_UNSUPPORTED_PROTOCOL_EVENT"' \
  "$tmp/raw-parent.json" >"$tmp/raw-parent-unknown-event.json"
expect_failure "unknown Temporal event" \
  normalize_parent "$tmp/raw-parent-unknown-event.json" "$tmp/normalizer-error.json"

"$jq_bin" '.history.events[1].eventId = 2' \
  "$tmp/raw-parent.json" >"$tmp/raw-parent-numeric-id.json"
expect_failure "numeric event ID" \
  normalize_parent "$tmp/raw-parent-numeric-id.json" "$tmp/normalizer-error.json"

# The pre-replacement child may have server-only progress, but TimerFired must
# be accompanied by WorkflowTaskScheduled and must not contain worker progress.
"$jq_bin" 'del(.events[6])' "$child_post" >"$tmp/child-post-timer-only.json"
expect_failure "TimerFired without task scheduling" \
  check_post "$parent_post" "$tmp/child-post-timer-only.json" "$diagnostics_initial"

"$jq_bin" '.events += [{event_id: "8", type: "WorkflowTaskStarted"}]' \
  "$child_post" >"$tmp/child-post-worker-event.json"
expect_failure "worker event before replacement completion" \
  check_post "$parent_post" "$tmp/child-post-worker-event.json" "$diagnostics_initial"

"$jq_bin" '.records[0].history_length = "0"' \
  "$diagnostics_initial" >"$tmp/diagnostics-zero.json"
expect_failure "zero diagnostic history length" \
  check_initial "$parent_initial" "$child_initial" "$tmp/diagnostics-zero.json"

"$jq_bin" '.records[2].history_length = "8"' \
  "$diagnostics_terminal" >"$tmp/diagnostics-regression.json"
expect_failure "replay history regression" \
  check_terminal "$parent_terminal" "$child_terminal" "$tmp/diagnostics-regression.json"

"$jq_bin" '.events[4].child_workflow_type = "wrong.child.type"' \
  "$parent_terminal" >"$tmp/parent-terminal-broken-prefix.json"
expect_failure "terminal prefix mutation" \
  check_terminal "$tmp/parent-terminal-broken-prefix.json" "$child_terminal" "$diagnostics_terminal"

"$jq_bin" \
  '.events[7] as $parent_post | .events[7] = .events[8] | .events[8] = $parent_post' \
  "$controller" >"$tmp/controller-swapped.json"
expect_failure "post-removal snapshot order" check_controller "$tmp/controller-swapped.json"

# Schema expressions and executable helpers agree on signed-64 limits. The
# schema expresses a code-point bound; this test additionally verifies that
# the executable gate rejects an identifier over 4096 UTF-8 bytes.
assert_signed_64_pattern "$schema_dir/parent-child-restart-replay-history.schema.json" event_id
assert_signed_64_pattern "$schema_dir/parent-child-restart-replay-diagnostics.schema.json" positive_signed_64
assert_signed_64_pattern "$schema_dir/parent-child-restart-replay-controller.schema.json" event_id
assert_signed_64_pattern "$schema_dir/parent-child-restart-replay-controller.schema.json" positive_signed_64

overlong_identifier=$("$jq_bin" -nr '[range(0; 2049) | "é"] | join("")')
"$jq_bin" -nr --arg value "$overlong_identifier" \
  '$value | length == 2049 and utf8bytelength == 4098' >/dev/null \
  || fail "multibyte identifier fixture does not exceed byte cap"
validate_overlong_identifier() {
  "$history_validator" \
    --stage initial \
    --parent-history "$parent_initial" \
    --child-history "$child_initial" \
    --diagnostics "$diagnostics_initial" \
    --parent-workflow-id "$overlong_identifier" \
    --parent-run-id "$parent_run" \
    --child-workflow-id "$child_workflow" \
    --child-run-id "$child_run"
}
expect_failure "identifier over 4096 UTF-8 bytes" validate_overlong_identifier

if "$jq_bin" -e --arg zero '0' \
  '."$defs".positive_signed_64.pattern as $pattern | ($zero | test($pattern))' \
  "$schema_dir/parent-child-restart-replay-diagnostics.schema.json" >/dev/null; then
  fail "diagnostics schema permits zero history length"
fi

printf '%s\n' 'parent-child restart-replay offline contract passed'
