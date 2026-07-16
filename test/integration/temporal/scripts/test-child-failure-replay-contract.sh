#!/bin/sh
set -eu

# Exercises the child-failure-after-replay acceptance contract without Docker.
# Existing successful parent/child snapshots are transformed into the exact
# failure outcome, then the same validators used by the live gate prove that
# identifiers, replay prefixes, failure event order, and controller outcome
# remain closed and mutually consistent.

root=$(CDPATH='' cd -- "$(dirname "$0")/../../../.." && pwd)
fixtures="$root/test/integration/temporal/fixtures/parent-child-restart-replay"
validator="$root/test/integration/temporal/scripts/validate-parent-child-restart-replay.sh"
controller_validator="$root/test/integration/temporal/scripts/validate-parent-child-restart-replay-controller.sh"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/ocaml-temporal-child-failure.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

parent_workflow_id=two-binary-parent-child-failure-replay
parent_run_id=11111111-1111-4111-8111-111111111111
child_workflow_id=two-binary-parent-child-failure-replay-child-smoke
child_run_id=22222222-2222-4222-8222-222222222222
child_workflow_type=smoke.parent_child_failure_replay_child

# Rewrite only stable identity fields. Event IDs and history lengths are kept
# unchanged so this remains a contract test, not a second implementation of
# Temporal history normalization.
rewrite_parent='.
  | .workflow_id=$parent_workflow
  | .run_id=$parent_run
  | .events |= map(
      if has("child_workflow_id") then
        .child_workflow_id=$child_workflow
        | .child_workflow_type=$child_type
      else . end)'
rewrite_child='.
  | .workflow_id=$child_workflow
  | .run_id=$child_run
  | .events[0].workflow_type=$child_type
  | .events[0].parent_workflow_id=$parent_workflow
  | .events[0].parent_run_id=$parent_run'

rewrite_json() {
  input=$1
  output=$2
  filter=$3
  jq --arg parent_workflow "$parent_workflow_id" \
    --arg parent_run "$parent_run_id" \
    --arg child_workflow "$child_workflow_id" \
    --arg child_run "$child_run_id" \
    --arg child_type "$child_workflow_type" \
    "$filter" "$input" >"$output"
}

rewrite_json "$fixtures/parent.history.initial.json" "$tmp/parent.initial.json" "$rewrite_parent"
rewrite_json "$fixtures/parent.history.post-removal.json" "$tmp/parent.post.json" "$rewrite_parent"
rewrite_json "$fixtures/parent.history.terminal.json" "$tmp/parent.terminal.json" \
  "$rewrite_parent | .events[9].type = \"ChildWorkflowExecutionFailed\""
rewrite_json "$fixtures/child.history.initial.json" "$tmp/child.initial.json" "$rewrite_child"
rewrite_json "$fixtures/child.history.post-removal.json" "$tmp/child.post.json" "$rewrite_child"
rewrite_json "$fixtures/child.history.terminal.json" "$tmp/child.terminal.json" \
  "$rewrite_child | .events[-1].type = \"WorkflowExecutionFailed\""

rewrite_json "$fixtures/diagnostics.initial.json" "$tmp/diagnostics.initial.json" \
  '.parent.workflow_id=$parent_workflow | .parent.run_id=$parent_run
   | .child.workflow_id=$child_workflow | .child.run_id=$child_run'
rewrite_json "$fixtures/diagnostics.json" "$tmp/diagnostics.json" \
  '.parent.workflow_id=$parent_workflow | .parent.run_id=$parent_run
   | .child.workflow_id=$child_workflow | .child.run_id=$child_run'
rewrite_json "$fixtures/controller.json" "$tmp/controller.json" \
  '.parent_workflow_id=$parent_workflow | .parent_run_id=$parent_run
   | .child_workflow_id=$child_workflow | .child_run_id=$child_run
   | .events[1].workflow_id=$parent_workflow | .events[1].run_id=$parent_run
   | .events[2].workflow_id=$child_workflow | .events[2].run_id=$child_run
   | .events[2].parent_workflow_id=$parent_workflow | .events[2].parent_run_id=$parent_run
   | .events[12].workflow_id=$parent_workflow | .events[12].run_id=$parent_run
   | .events[12].outcome="child_failure_recovered"'

common="--parent-workflow-id $parent_workflow_id --parent-run-id $parent_run_id --child-workflow-id $child_workflow_id --child-run-id $child_run_id --diagnostics $tmp/diagnostics.initial.json --outcome failure --child-workflow-type $child_workflow_type"
# shellcheck disable=SC2086
sh "$validator" --stage initial --parent-history "$tmp/parent.initial.json" --child-history "$tmp/child.initial.json" $common >/dev/null
# shellcheck disable=SC2086
sh "$validator" --stage post-removal --parent-history "$tmp/parent.post.json" --child-history "$tmp/child.post.json" $common --parent-initial-history "$tmp/parent.initial.json" --child-initial-history "$tmp/child.initial.json" >/dev/null
# shellcheck disable=SC2086
sh "$validator" --stage terminal --parent-history "$tmp/parent.terminal.json" --child-history "$tmp/child.terminal.json" --diagnostics "$tmp/diagnostics.json" --outcome failure --child-workflow-type "$child_workflow_type" --parent-workflow-id "$parent_workflow_id" --parent-run-id "$parent_run_id" --child-workflow-id "$child_workflow_id" --child-run-id "$child_run_id" --parent-initial-history "$tmp/parent.initial.json" --child-initial-history "$tmp/child.initial.json" --parent-post-removal-history "$tmp/parent.post.json" --child-post-removal-history "$tmp/child.post.json" >/dev/null
sh "$controller_validator" --controller "$tmp/controller.json" --parent-workflow-id "$parent_workflow_id" --parent-run-id "$parent_run_id" --child-workflow-id "$child_workflow_id" --child-run-id "$child_run_id" --initiated-event-id 5 --expected-outcome child_failure_recovered >/dev/null

# The success contract must not accidentally accept a failure history.
if sh "$validator" --stage terminal --parent-history "$tmp/parent.terminal.json" --child-history "$tmp/child.terminal.json" --diagnostics "$tmp/diagnostics.json" --parent-workflow-id "$parent_workflow_id" --parent-run-id "$parent_run_id" --child-workflow-id "$child_workflow_id" --child-run-id "$child_run_id" --parent-initial-history "$tmp/parent.initial.json" --child-initial-history "$tmp/child.initial.json" --parent-post-removal-history "$tmp/parent.post.json" --child-post-removal-history "$tmp/child.post.json" >/dev/null 2>&1; then
  echo "failure contract was accepted as a successful child completion" >&2
  exit 1
fi
if sh "$controller_validator" --controller "$tmp/controller.json" --parent-workflow-id "$parent_workflow_id" --parent-run-id "$parent_run_id" --child-workflow-id "$child_workflow_id" --child-run-id "$child_run_id" --initiated-event-id 5 >/dev/null 2>&1; then
  echo "failure controller outcome was accepted as completed" >&2
  exit 1
fi

echo "child-failure-replay contract passed"
