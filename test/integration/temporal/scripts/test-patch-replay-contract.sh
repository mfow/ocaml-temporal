#!/bin/sh
set -eu

# Exercises the complete offline patch-replay acceptance protocol.  This is a
# fast, Docker-free gate for the exact scripts that the live Compose controller
# invokes: all three source transitions, Native_worker diagnostics, strict Core
# marker decoding, controller ordering, and representative tampering attempts.

root=$(CDPATH='' cd -- "$(dirname "$0")/../../../.." && pwd)
fixture="$root/test/integration/temporal/fixtures/patch-replay"
normalizer="$root/test/integration/temporal/scripts/normalize-patch-replay-history.sh"
validator="$root/test/integration/temporal/scripts/validate-patch-replay.sh"
controller_validator="$root/test/integration/temporal/scripts/validate-patch-replay-controller.sh"
identity_validator="$root/test/integration/temporal/scripts/validate-restart-replay-identity.sh"
legacy_workflow_id=two-binary-patch-replay-legacy
legacy_run_id=11111111-1111-4111-8111-111111111111
new_workflow_id=two-binary-patch-replay-new
new_run_id=22222222-2222-4222-8222-222222222222
removal_workflow_id=two-binary-patch-replay-removal
removal_run_id=33333333-3333-4333-8333-333333333333
patch_id=smoke.patch_replay_history.activity.v1
# This is inside the signed-64 range section that was previously easy to omit
# from hand-written decimal regular expressions.  The second value is exactly
# one greater than the largest permitted positive signed-64 integer.
high_valid_signed_64=9223372036849999999
outside_signed_64=9223372036854775808

[ -r "$normalizer" ]
[ -r "$validator" ]
[ -r "$controller_validator" ]
[ -r "$identity_validator" ]
for file in \
  "$fixture/legacy.history.initial.json" \
  "$fixture/legacy.history.terminal.json" \
  "$fixture/legacy.diagnostics.initial.json" \
  "$fixture/legacy.diagnostics.json" \
  "$fixture/new.history.initial.json" \
  "$fixture/new.history.terminal.json" \
  "$fixture/new.diagnostics.initial.json" \
  "$fixture/new.diagnostics.json" \
  "$fixture/removal.history.initial.json" \
  "$fixture/removal.history.terminal.json" \
  "$fixture/removal.diagnostics.initial.json" \
  "$fixture/removal.diagnostics.json" \
  "$fixture/controller.json" \
  "$root/docs/schemas/acceptance/patch-replay-history.schema.json" \
  "$root/docs/schemas/acceptance/patch-replay-diagnostics.schema.json" \
  "$root/docs/schemas/acceptance/patch-replay-controller.schema.json"; do
  [ -r "$file" ]
  jq -e 'type == "object"' "$file" >/dev/null
done

# Asserts that a deliberately malformed input is rejected.  A passing command
# is a test failure because each case below represents evidence the live gate
# must not accept after a controller, CLI, or protocol regression.
expect_failure() {
  if "$@" >/dev/null 2>&1; then
    echo "expected command to fail: $*" >&2
    exit 1
  fi
}

# Applies the complete three-scenario identity contract consistently in every
# positive and tamper case below. Keeping these arguments in one helper avoids
# accidentally testing a reduced controller surface after adding a scenario.
validate_controller() {
  sh "$controller_validator" --controller "$1" \
    --legacy-workflow-id "$legacy_workflow_id" --legacy-run-id "$legacy_run_id" \
    --new-workflow-id "$new_workflow_id" --new-run-id "$new_run_id" \
    --removal-workflow-id "$removal_workflow_id" --removal-run-id "$removal_run_id"
}

# Tests a JSON Schema regex directly as well as exercising the shell
# validators below.  JSON Schema cannot express the lifecycle relationships,
# but its number-boundary rules must stay aligned with the executable checks.
schema_pattern_matches() {
  schema=$1
  definition=$2
  candidate=$3
  jq -e --arg definition "$definition" --arg candidate "$candidate" '
    .["$defs"][$definition].pattern as $pattern
    | ($pattern | type == "string" and length > 0)
      and ($candidate | test($pattern))
  ' "$schema" >/dev/null
}

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

# The history adapter deliberately accepts a caller-supplied run ID when the
# CLI's `workflow show` response omits it. The live runner first binds that
# value to a closed `workflow describe` envelope; keep that proof on this
# Docker-free path so a mismatched response cannot make unrelated history look
# like the driver-accepted execution.
jq -n --arg workflow_id "$legacy_workflow_id" --arg run_id "$legacy_run_id" \
  '{workflowExecutionInfo:{execution:{workflowId:$workflow_id,runId:$run_id}}}' \
  >"$tmp/describe.json"
sh "$identity_validator" --input "$tmp/describe.json" \
  --workflow-id "$legacy_workflow_id" --run-id "$legacy_run_id" >/dev/null
expect_failure sh "$identity_validator" --input "$tmp/describe.json" \
  --workflow-id "$legacy_workflow_id" --run-id "$new_run_id"
expect_failure sh "$identity_validator" --input "$tmp/describe.json" \
  --workflow-id "$new_workflow_id" --run-id "$legacy_run_id"

# Both protobuf-JSON spellings are supported, but a plausible top-level
# execution object is not an accepted describe envelope. This prevents the
# validation boundary from drifting into a permissive JSON field search.
jq '{workflow_execution_info:{workflow_execution:{workflow_id:.workflowExecutionInfo.execution.workflowId,run_id:.workflowExecutionInfo.execution.runId}}}' \
  "$tmp/describe.json" >"$tmp/describe-snake.json"
sh "$identity_validator" --input "$tmp/describe-snake.json" \
  --workflow-id "$legacy_workflow_id" --run-id "$legacy_run_id" >/dev/null
jq '.workflowExecutionInfo.execution' "$tmp/describe.json" \
  >"$tmp/describe-top-level.json"
expect_failure sh "$identity_validator" --input "$tmp/describe-top-level.json" \
  --workflow-id "$legacy_workflow_id" --run-id "$legacy_run_id"

# Initial validation happens before source replacement and therefore consumes
# the one-record snapshot owned by generation one.  Terminal validation uses
# the persisted two-record document and the exact initial-history prefix.
sh "$validator" \
  --mode legacy-initial \
  --history "$fixture/legacy.history.initial.json" \
  --diagnostics "$fixture/legacy.diagnostics.initial.json" \
  --workflow-id "$legacy_workflow_id" \
  --run-id "$legacy_run_id" \
  --patch-id "$patch_id" >/dev/null
sh "$validator" \
  --mode legacy-terminal \
  --history "$fixture/legacy.history.terminal.json" \
  --initial-history "$fixture/legacy.history.initial.json" \
  --diagnostics "$fixture/legacy.diagnostics.json" \
  --workflow-id "$legacy_workflow_id" \
  --run-id "$legacy_run_id" \
  --patch-id "$patch_id" >/dev/null
sh "$validator" \
  --mode new-initial \
  --history "$fixture/new.history.initial.json" \
  --diagnostics "$fixture/new.diagnostics.initial.json" \
  --workflow-id "$new_workflow_id" \
  --run-id "$new_run_id" \
  --patch-id "$patch_id" >/dev/null
sh "$validator" \
  --mode new-terminal \
  --history "$fixture/new.history.terminal.json" \
  --initial-history "$fixture/new.history.initial.json" \
  --diagnostics "$fixture/new.diagnostics.json" \
  --workflow-id "$new_workflow_id" \
  --run-id "$new_run_id" \
  --patch-id "$patch_id" >/dev/null
sh "$validator" \
  --mode removal-initial \
  --history "$fixture/removal.history.initial.json" \
  --diagnostics "$fixture/removal.diagnostics.initial.json" \
  --workflow-id "$removal_workflow_id" \
  --run-id "$removal_run_id" \
  --patch-id "$patch_id" >/dev/null
sh "$validator" \
  --mode removal-terminal \
  --history "$fixture/removal.history.terminal.json" \
  --initial-history "$fixture/removal.history.initial.json" \
  --diagnostics "$fixture/removal.diagnostics.json" \
  --workflow-id "$removal_workflow_id" \
  --run-id "$removal_run_id" \
  --patch-id "$patch_id" >/dev/null

validate_controller "$fixture/controller.json" >/dev/null

# Recreate the protobuf-JSON shape emitted by the Temporal CLI for a fresh
# patched initial history.  The normalizer must recover only the closed marker
# fields, including the activity-independent Core payload contract.
jq -n \
  --arg workflow_id "$new_workflow_id" \
  --arg run_id "$new_run_id" \
  --arg patch_id "$patch_id" \
  '{
    workflowExecution: {workflowId: $workflow_id, runId: $run_id},
    history: {events: [
      {eventId: "1", eventType: "EVENT_TYPE_WORKFLOW_EXECUTION_STARTED",
       workflowExecutionStartedEventAttributes: {workflowId: $workflow_id}},
      {eventId: "2", eventType: "EVENT_TYPE_WORKFLOW_TASK_SCHEDULED"},
      {eventId: "3", eventType: "EVENT_TYPE_WORKFLOW_TASK_STARTED"},
      {eventId: "4", eventType: "EVENT_TYPE_WORKFLOW_TASK_COMPLETED"},
      {eventId: "5", eventType: "EVENT_TYPE_MARKER_RECORDED",
       markerRecordedEventAttributes: {
         markerName: "core_patch",
         details: {
           "patch-data": {
             payloads: [{
               metadata: {encoding: ("json/plain" | @base64)},
               data: ({id: $patch_id, deprecated: false} | tojson | @base64)
             }]
           }
         }
       }},
      {eventId: "6", eventType: "EVENT_TYPE_TIMER_STARTED"}
    ]}
  }' >"$tmp/cli-new-initial.json"

sh "$normalizer" \
  --workflow-id "$new_workflow_id" \
  --run-id "$new_run_id" \
  --output "$tmp/normalized-new-initial.json" \
  <"$tmp/cli-new-initial.json"
jq -e --slurpfile expected "$fixture/new.history.initial.json" \
  '. == $expected[0]' "$tmp/normalized-new-initial.json" >/dev/null

# The same Core payload shape carries the deprecation state used by the
# removal scenario. Prove the normalizer retains true rather than merely
# accepting the hand-written normalized fixture.
jq --arg workflow_id "$removal_workflow_id" --arg run_id "$removal_run_id" '
  .workflowExecution = {workflowId: $workflow_id, runId: $run_id}
  | .history.events[0].workflowExecutionStartedEventAttributes.workflowId = $workflow_id
  | .history.events[4].markerRecordedEventAttributes.details["patch-data"].payloads[0].data =
      ({id: "smoke.patch_replay_history.activity.v1", deprecated: true} | tojson | @base64)
' "$tmp/cli-new-initial.json" >"$tmp/cli-removal-initial.json"
sh "$normalizer" \
  --workflow-id "$removal_workflow_id" \
  --run-id "$removal_run_id" \
  --output "$tmp/normalized-removal-initial.json" \
  <"$tmp/cli-removal-initial.json"
jq -e --slurpfile expected "$fixture/removal.history.initial.json" \
  '. == $expected[0]' "$tmp/normalized-removal-initial.json" >/dev/null

# The marker payload must carry a JSON boolean, not a truthy string. The
# normalizer rejects this at the raw Core-payload boundary before a normalized
# history can misrepresent the marker lifecycle.
jq '.history.events[4].markerRecordedEventAttributes.details["patch-data"].payloads[0].data =
      ({id: "smoke.patch_replay_history.activity.v1", deprecated: "true"} | tojson | @base64)' \
  "$tmp/cli-removal-initial.json" >"$tmp/cli-removal-string-deprecated.json"
expect_failure sh "$normalizer" \
  --workflow-id "$removal_workflow_id" --run-id "$removal_run_id" \
  --output "$tmp/normalized-removal-string-deprecated.json" \
  <"$tmp/cli-removal-string-deprecated.json"

# `workflow show` may return a top-level HistoryEvent response with no
# enclosing run identity.  The normalizer may label that event list with its
# requested run only because the strict `workflow describe` identity proof
# above already bound that command argument to this execution.
jq '{events: .history.events}' "$tmp/cli-new-initial.json" \
  >"$tmp/cli-new-initial-list.json"
sh "$normalizer" \
  --workflow-id "$new_workflow_id" \
  --run-id "$new_run_id" \
  --output "$tmp/normalized-new-initial-list.json" \
  <"$tmp/cli-new-initial-list.json"
jq -e --slurpfile expected "$fixture/new.history.initial.json" \
  '. == $expected[0]' "$tmp/normalized-new-initial-list.json" >/dev/null

# A fallback is never a repair mechanism for an explicit malformed or
# contradictory identity.  These cases used to be hidden by jq `//` and would
# have accepted a response whose envelope could not actually identify the
# execution being replayed.
jq '.workflowExecution.runId = null' "$tmp/cli-new-initial.json" \
  >"$tmp/cli-new-initial-null-run.json"
expect_failure sh "$normalizer" \
  --workflow-id "$new_workflow_id" --run-id "$new_run_id" \
  --output "$tmp/normalized-new-initial-null-run.json" \
  <"$tmp/cli-new-initial-null-run.json"

jq '.history.events[0].workflowExecutionStartedEventAttributes.workflowId = "different-workflow"' \
  "$tmp/cli-new-initial.json" >"$tmp/cli-new-initial-conflicting-workflow.json"
expect_failure sh "$normalizer" \
  --workflow-id "$new_workflow_id" --run-id "$new_run_id" \
  --output "$tmp/normalized-new-initial-conflicting-workflow.json" \
  <"$tmp/cli-new-initial-conflicting-workflow.json"

# The adapter accepts the one alternate protobuf spelling documented in its
# source, but it must never accept both detail keys or an unrecognized envelope.
jq '.history.events[4].markerRecordedEventAttributes.details
      |= {patch_data: .["patch-data"]}' \
  "$tmp/cli-new-initial.json" >"$tmp/cli-new-initial-snake.json"
sh "$normalizer" \
  --workflow-id "$new_workflow_id" \
  --run-id "$new_run_id" \
  --output "$tmp/normalized-new-initial-snake.json" \
  <"$tmp/cli-new-initial-snake.json"
jq -e --slurpfile expected "$fixture/new.history.initial.json" \
  '. == $expected[0]' "$tmp/normalized-new-initial-snake.json" >/dev/null

# Only scheduled activities retain their type in the projection.  A later
# event cannot smuggle an activity type field into the normalized document.
jq -n --arg workflow_id "$legacy_workflow_id" --arg run_id "$legacy_run_id" \
  '{workflowExecution: {workflowId: $workflow_id, runId: $run_id}, history: {events: [
    {eventId: "1", eventType: "EVENT_TYPE_WORKFLOW_EXECUTION_STARTED",
     workflowExecutionStartedEventAttributes: {workflowId: $workflow_id}},
    {eventId: "2", eventType: "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED",
     activityTaskScheduledEventAttributes: {activityType: {name: "smoke.patch_replay_history.legacy_activity"}}},
    {eventId: "3", eventType: "EVENT_TYPE_ACTIVITY_TASK_STARTED"}
  ]}}' >"$tmp/cli-activity.json"
sh "$normalizer" \
  --workflow-id "$legacy_workflow_id" \
  --run-id "$legacy_run_id" \
  --output "$tmp/normalized-activity.json" <"$tmp/cli-activity.json"
jq -e '
  (.events[0] | keys | sort) == ["event_id", "type"]
  and (.events[1] | keys | sort) == ["activity_type", "event_id", "type"]
  and (.events[1].activity_type == "smoke.patch_replay_history.legacy_activity")
  and (.events[2] | keys | sort) == ["event_id", "type"]
' "$tmp/normalized-activity.json" >/dev/null

# Core's patch marker is a single JSON payload, not an arbitrary marker bag.
# Every mutation below retains plausible JSON and must still fail closed.
jq '.history.events[4].markerRecordedEventAttributes.details.unexpected = {}' \
  "$tmp/cli-new-initial.json" >"$tmp/cli-marker-extra-detail.json"
expect_failure sh "$normalizer" \
  --workflow-id "$new_workflow_id" --run-id "$new_run_id" \
  --output "$tmp/normalized-marker-extra-detail.json" \
  <"$tmp/cli-marker-extra-detail.json"

jq '.history.events[4].markerRecordedEventAttributes.details["patch-data"].payloads[0].metadata.encoding = ("text/plain" | @base64)' \
  "$tmp/cli-new-initial.json" >"$tmp/cli-marker-wrong-encoding.json"
expect_failure sh "$normalizer" \
  --workflow-id "$new_workflow_id" --run-id "$new_run_id" \
  --output "$tmp/normalized-marker-wrong-encoding.json" \
  <"$tmp/cli-marker-wrong-encoding.json"

jq '.history.events[4].markerRecordedEventAttributes.details["patch-data"].payloads[0].metadata.extra = "x"' \
  "$tmp/cli-new-initial.json" >"$tmp/cli-marker-extra-metadata.json"
expect_failure sh "$normalizer" \
  --workflow-id "$new_workflow_id" --run-id "$new_run_id" \
  --output "$tmp/normalized-marker-extra-metadata.json" \
  <"$tmp/cli-marker-extra-metadata.json"

jq '.history.events[4].markerRecordedEventAttributes.markerName = "not-core"' \
  "$tmp/cli-new-initial.json" >"$tmp/cli-marker-not-core.json"
expect_failure sh "$normalizer" \
  --workflow-id "$new_workflow_id" --run-id "$new_run_id" \
  --output "$tmp/normalized-marker-not-core.json" \
  <"$tmp/cli-marker-not-core.json"

# Temporal event IDs and observer history lengths are decimal strings so they
# retain every signed-64 value exactly.  Check the formerly missed 19-digit
# interval, the hard upper bound, and all three schema definitions alongside
# the independent executable validators.
for schema_definition in \
  "$root/docs/schemas/acceptance/patch-replay-history.schema.json:event_id" \
  "$root/docs/schemas/acceptance/patch-replay-diagnostics.schema.json:history_length" \
  "$root/docs/schemas/acceptance/patch-replay-controller.schema.json:positive_signed_64"; do
  schema=${schema_definition%:*}
  definition=${schema_definition##*:}
  if ! schema_pattern_matches "$schema" "$definition" "$high_valid_signed_64"; then
    echo "schema unexpectedly rejected valid signed-64 value: $schema_definition" >&2
    exit 1
  fi
  if schema_pattern_matches "$schema" "$definition" "$outside_signed_64"; then
    echo "schema accepted out-of-range signed-64 value: $schema_definition" >&2
    exit 1
  fi
done

jq --arg event_id "$high_valid_signed_64" \
  '.history.events[5].eventId = $event_id' "$tmp/cli-new-initial.json" \
  >"$tmp/cli-new-initial-large-event-id.json"
sh "$normalizer" \
  --workflow-id "$new_workflow_id" --run-id "$new_run_id" \
  --output "$tmp/normalized-new-initial-large-event-id.json" \
  <"$tmp/cli-new-initial-large-event-id.json"
jq -e --arg event_id "$high_valid_signed_64" \
  '.events[5].event_id == $event_id' \
  "$tmp/normalized-new-initial-large-event-id.json" >/dev/null

jq --arg event_id "$outside_signed_64" \
  '.history.events[5].eventId = $event_id' "$tmp/cli-new-initial.json" \
  >"$tmp/cli-new-initial-outside-event-id.json"
expect_failure sh "$normalizer" \
  --workflow-id "$new_workflow_id" --run-id "$new_run_id" \
  --output "$tmp/normalized-new-initial-outside-event-id.json" \
  <"$tmp/cli-new-initial-outside-event-id.json"

jq --arg event_id "$high_valid_signed_64" \
  '.events[5].event_id = $event_id' "$fixture/new.history.initial.json" \
  >"$tmp/new-initial-large-event-id.json"
sh "$validator" \
  --mode new-initial --history "$tmp/new-initial-large-event-id.json" \
  --diagnostics "$fixture/new.diagnostics.initial.json" \
  --workflow-id "$new_workflow_id" --run-id "$new_run_id" --patch-id "$patch_id" >/dev/null

jq --arg event_id "$outside_signed_64" \
  '.events[5].event_id = $event_id' "$fixture/new.history.initial.json" \
  >"$tmp/new-initial-outside-event-id.json"
expect_failure sh "$validator" \
  --mode new-initial --history "$tmp/new-initial-outside-event-id.json" \
  --diagnostics "$fixture/new.diagnostics.initial.json" \
  --workflow-id "$new_workflow_id" --run-id "$new_run_id" --patch-id "$patch_id"

jq --arg history_length "$high_valid_signed_64" \
  '.records[1].history_length = $history_length' "$fixture/new.diagnostics.json" \
  >"$tmp/new-diagnostics-large-history-length.json"
sh "$validator" \
  --mode new-terminal --history "$fixture/new.history.terminal.json" \
  --initial-history "$fixture/new.history.initial.json" \
  --diagnostics "$tmp/new-diagnostics-large-history-length.json" \
  --workflow-id "$new_workflow_id" --run-id "$new_run_id" --patch-id "$patch_id" >/dev/null

jq --arg history_length "$outside_signed_64" \
  '.records[1].history_length = $history_length' "$fixture/new.diagnostics.json" \
  >"$tmp/new-diagnostics-outside-history-length.json"
expect_failure sh "$validator" \
  --mode new-terminal --history "$fixture/new.history.terminal.json" \
  --initial-history "$fixture/new.history.initial.json" \
  --diagnostics "$tmp/new-diagnostics-outside-history-length.json" \
  --workflow-id "$new_workflow_id" --run-id "$new_run_id" --patch-id "$patch_id"

jq --arg history_length "$high_valid_signed_64" \
  '.events[6].history_length = $history_length' "$fixture/controller.json" \
  >"$tmp/controller-large-history-length.json"
validate_controller "$tmp/controller-large-history-length.json" >/dev/null

jq --arg history_length "$outside_signed_64" \
  '.events[6].history_length = $history_length' "$fixture/controller.json" \
  >"$tmp/controller-outside-history-length.json"
expect_failure validate_controller "$tmp/controller-outside-history-length.json"

# A legacy history must stay marker-free, including its terminal snapshot.
jq '.events[4:4] += [{
  event_id: "5", type: "MarkerRecorded", marker_name: "core_patch",
  patch_id: "smoke.patch_replay_history.activity.v1", deprecated: false
}] | .events[5:] |= map(.event_id = ((.event_id | tonumber) + 1 | tostring))' \
  "$fixture/legacy.history.initial.json" >"$tmp/legacy-initial-marker.json"
expect_failure sh "$validator" \
  --mode legacy-initial --history "$tmp/legacy-initial-marker.json" \
  --diagnostics "$fixture/legacy.diagnostics.initial.json" \
  --workflow-id "$legacy_workflow_id" --run-id "$legacy_run_id" --patch-id "$patch_id"

# A new history needs exactly one marker before its timer; an omitted marker,
# a duplicate marker, a wrong durable patch ID, or a reordered timer is not a
# compatible source deployment.
jq 'del(.events[4])' \
  "$fixture/new.history.initial.json" >"$tmp/new-initial-no-marker.json"
expect_failure sh "$validator" \
  --mode new-initial --history "$tmp/new-initial-no-marker.json" \
  --diagnostics "$fixture/new.diagnostics.initial.json" \
  --workflow-id "$new_workflow_id" --run-id "$new_run_id" --patch-id "$patch_id"

# The removal generation is safe only after a true deprecated marker. A
# structurally valid active marker must not be accepted as removal evidence.
jq '.events[4].deprecated = false' \
  "$fixture/removal.history.initial.json" >"$tmp/removal-initial-active-marker.json"
expect_failure sh "$validator" \
  --mode removal-initial --history "$tmp/removal-initial-active-marker.json" \
  --diagnostics "$fixture/removal.diagnostics.initial.json" \
  --workflow-id "$removal_workflow_id" --run-id "$removal_run_id" --patch-id "$patch_id"

jq '.events[5:5] += [{
  event_id: "6", type: "MarkerRecorded", marker_name: "core_patch",
  patch_id: "smoke.patch_replay_history.activity.v1", deprecated: false
}] | .events[6:] |= map(.event_id = ((.event_id | tonumber) + 1 | tostring))' \
  "$fixture/new.history.initial.json" >"$tmp/new-initial-duplicate-marker.json"
expect_failure sh "$validator" \
  --mode new-initial --history "$tmp/new-initial-duplicate-marker.json" \
  --diagnostics "$fixture/new.diagnostics.initial.json" \
  --workflow-id "$new_workflow_id" --run-id "$new_run_id" --patch-id "$patch_id"

jq '.events[4].patch_id = "different.patch"' \
  "$fixture/new.history.initial.json" >"$tmp/new-initial-wrong-patch.json"
expect_failure sh "$validator" \
  --mode new-initial --history "$tmp/new-initial-wrong-patch.json" \
  --diagnostics "$fixture/new.diagnostics.initial.json" \
  --workflow-id "$new_workflow_id" --run-id "$new_run_id" --patch-id "$patch_id"

# Active-to-deprecated replay must retain the original false marker; a true
# marker here would describe a different lifecycle history.
jq '.events[4].deprecated = true' \
  "$fixture/new.history.initial.json" >"$tmp/new-initial-deprecated-marker.json"
expect_failure sh "$validator" \
  --mode new-initial --history "$tmp/new-initial-deprecated-marker.json" \
  --diagnostics "$fixture/new.diagnostics.initial.json" \
  --workflow-id "$new_workflow_id" --run-id "$new_run_id" --patch-id "$patch_id"

# The durable activity type is the branch oracle.  A terminal result that
# looks successful but schedules the opposite activity must not pass.
jq '.events[9].activity_type = "smoke.patch_replay_history.patched_activity"' \
  "$fixture/legacy.history.terminal.json" >"$tmp/legacy-terminal-new-activity.json"
expect_failure sh "$validator" \
  --mode legacy-terminal --history "$tmp/legacy-terminal-new-activity.json" \
  --initial-history "$fixture/legacy.history.initial.json" \
  --diagnostics "$fixture/legacy.diagnostics.json" \
  --workflow-id "$legacy_workflow_id" --run-id "$legacy_run_id" --patch-id "$patch_id"

# A terminal snapshot must be an exact extension of its own initial snapshot,
# rather than a separately valid history for the same claimed identity.
jq '.events[2].type = "WorkflowTaskScheduled"' \
  "$fixture/new.history.initial.json" >"$tmp/new-initial-prefix-mismatch.json"
expect_failure sh "$validator" \
  --mode new-terminal --history "$fixture/new.history.terminal.json" \
  --initial-history "$tmp/new-initial-prefix-mismatch.json" \
  --diagnostics "$fixture/new.diagnostics.json" \
  --workflow-id "$new_workflow_id" --run-id "$new_run_id" --patch-id "$patch_id"

# The Native_worker observer must retain the generation-two replay fact once
# the terminal history is checked.  It is not enough to retain a non-replay
# activation from the stopped source process.
jq '.records[1].is_replaying = false' "$fixture/new.diagnostics.json" \
  >"$tmp/new-diagnostics-not-replaying.json"
expect_failure sh "$validator" \
  --mode new-terminal --history "$fixture/new.history.terminal.json" \
  --initial-history "$fixture/new.history.initial.json" \
  --diagnostics "$tmp/new-diagnostics-not-replaying.json" \
  --workflow-id "$new_workflow_id" --run-id "$new_run_id" --patch-id "$patch_id"

# A pre-replacement controller record cannot use a two-record diagnostic file:
# this catches a runner that reads only after the new worker has already made
# progress and thereby loses the initial stop boundary.
expect_failure sh "$validator" \
  --mode legacy-initial --history "$fixture/legacy.history.initial.json" \
  --diagnostics "$fixture/legacy.diagnostics.json" \
  --workflow-id "$legacy_workflow_id" --run-id "$legacy_run_id" --patch-id "$patch_id"

# The controller must preserve every observable branch, marker count and
# deprecation state, source topology, and final PostgreSQL cleanup in order.
jq '.events[6].marker_count = 1' "$fixture/controller.json" \
  >"$tmp/controller-legacy-marker.json"
expect_failure validate_controller "$tmp/controller-legacy-marker.json"

jq '.events[3].worker_version = "patched"' "$fixture/controller.json" \
  >"$tmp/controller-missing-legacy-source.json"
expect_failure validate_controller "$tmp/controller-missing-legacy-source.json"

jq '.events[5].container_id = .events[3].container_id' "$fixture/controller.json" \
  >"$tmp/controller-reused-container.json"
expect_failure validate_controller "$tmp/controller-reused-container.json"

# The terminal history is captured only after the driver has been joined.  A
# controller record must not claim that post-completion history before the
# completion it depends on, for any source-replacement scenario.
jq '.events[7:9] |= [.[1], .[0]]' "$fixture/controller.json" \
  >"$tmp/controller-legacy-terminal-before-driver.json"
expect_failure validate_controller "$tmp/controller-legacy-terminal-before-driver.json"

jq '.events[17:19] |= [.[1], .[0]]' "$fixture/controller.json" \
  >"$tmp/controller-new-terminal-before-driver.json"
expect_failure validate_controller "$tmp/controller-new-terminal-before-driver.json"

jq '.events[27:29] |= [.[1], .[0]]' "$fixture/controller.json" \
  >"$tmp/controller-removal-terminal-before-driver.json"
expect_failure validate_controller "$tmp/controller-removal-terminal-before-driver.json"

jq '.events[26].marker_deprecated = false' "$fixture/controller.json" \
  >"$tmp/controller-removal-active-marker.json"
expect_failure validate_controller "$tmp/controller-removal-active-marker.json"

jq '.events[31].remaining_project_volumes = 1' "$fixture/controller.json" \
  >"$tmp/controller-retained-volume.json"
expect_failure validate_controller "$tmp/controller-retained-volume.json"

echo 'patch-replay contract: ok'
