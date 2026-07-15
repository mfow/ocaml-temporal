#!/bin/sh
set -eu

# Validates one normalized history snapshot from the old/new workflow-patch
# acceptance.  The contract proves an actual Temporal history transition, not
# a worker log: a legacy execution has no marker and schedules the legacy
# activity after replay, while a new execution keeps one Core marker and
# schedules the patched activity.

# Prints the command-line contract and exits with the conventional usage code.
usage() {
  cat >&2 <<'EOF'
usage: validate-patch-replay.sh \
       --mode legacy-initial|legacy-terminal|new-initial|new-terminal \
       --history FILE --workflow-id ID --run-id ID --patch-id ID \
       --diagnostics FILE [--initial-history FILE]
EOF
  exit 2
}

# Reports one invariant failure without exposing workflow inputs, activity
# payloads, or raw Temporal failure attributes in the acceptance output.
fail() {
  echo "patch-replay validation failed: $*" >&2
  exit 1
}

mode=''
history=''
initial_history=''
diagnostics=''
workflow_id=''
run_id=''
patch_id=''
while [ "$#" -gt 0 ]; do
  case "$1" in
    --mode)
      [ "$#" -ge 2 ] || usage
      mode=$2
      shift 2
      ;;
    --history)
      [ "$#" -ge 2 ] || usage
      history=$2
      shift 2
      ;;
    --initial-history)
      [ "$#" -ge 2 ] || usage
      initial_history=$2
      shift 2
      ;;
    --diagnostics)
      [ "$#" -ge 2 ] || usage
      diagnostics=$2
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
    --patch-id)
      [ "$#" -ge 2 ] || usage
      patch_id=$2
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

[ -n "$mode" ] || usage
[ -n "$history" ] || usage
[ -n "$diagnostics" ] || usage
[ -n "$workflow_id" ] || usage
[ -n "$run_id" ] || usage
[ -n "$patch_id" ] || usage
[ -r "$history" ] || fail "history file is not readable: $history"
[ -r "$diagnostics" ] || fail "diagnostics file is not readable: $diagnostics"

case "$mode" in
  legacy-initial)
    scenario=legacy
    stage=initial
    expected_marker_count=0
    expected_activity=smoke.patch_replay_history.legacy_activity
    ;;
  legacy-terminal)
    scenario=legacy
    stage=terminal
    expected_marker_count=0
    expected_activity=smoke.patch_replay_history.legacy_activity
    ;;
  new-initial)
    scenario=new
    stage=initial
    expected_marker_count=1
    expected_activity=smoke.patch_replay_history.patched_activity
    ;;
  new-terminal)
    scenario=new
    stage=terminal
    expected_marker_count=1
    expected_activity=smoke.patch_replay_history.patched_activity
    ;;
  *)
    usage
    ;;
esac

if [ "$stage" = terminal ]; then
  [ -n "$initial_history" ] || fail "terminal modes require --initial-history"
  [ -r "$initial_history" ] || fail "initial history file is not readable: $initial_history"
elif [ -n "$initial_history" ]; then
  fail "initial modes must not receive --initial-history"
fi

jq_bin=${JQ_BIN:-jq}
command -v "$jq_bin" >/dev/null 2>&1 || fail "jq is required (set JQ_BIN to its path)"

# These are the only semantic event types the normalized adapter may emit for
# this narrow scenario.  Keeping failure and cancellation types in the set
# lets the structural check describe them precisely, then stage validation
# fails them explicitly rather than treating a new server enum as success.
event_types='[
  "WorkflowExecutionStarted",
  "WorkflowTaskScheduled",
  "WorkflowTaskStarted",
  "WorkflowTaskCompleted",
  "WorkflowTaskFailed",
  "WorkflowTaskTimedOut",
  "TimerStarted",
  "TimerFired",
  "TimerCanceled",
  "ActivityTaskScheduled",
  "ActivityTaskStarted",
  "ActivityTaskCompleted",
  "ActivityTaskFailed",
  "ActivityTaskCanceled",
  "MarkerRecorded",
  "UpsertWorkflowSearchAttributes",
  "WorkflowExecutionCompleted",
  "WorkflowExecutionFailed",
  "WorkflowExecutionCanceled",
  "WorkflowExecutionTerminated",
  "WorkflowExecutionTimedOut",
  "WorkflowExecutionContinuedAsNew"
]'

# Validates the closed wire document before comparing history snapshots.  In
# particular, `activity_type` may appear only on a scheduled activity and the
# Core marker fields may appear only on MarkerRecorded.  That makes accidental
# copying of raw attributes or payload data a protocol failure, not a harmless
# extra field.
validate_history_shape() {
  history_path=$1
  label=$2
  if ! "$jq_bin" -e \
    --arg expected_workflow "$workflow_id" \
    --arg expected_run "$run_id" \
    --arg expected_patch "$patch_id" \
    --argjson event_types "$event_types" \
    '
      def identifier:
        type == "string" and length > 0 and length <= 65536
        and test("^[^[:cntrl:]]*$");
      def positive_signed_64:
        type == "string"
        and test("^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[01][0-9]{17}|92[01][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[01][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6]|9223372036854775807)$");
      def event_shape:
        type == "object"
        and (.event_id | positive_signed_64)
        and (.type | type == "string" and ($event_types | index(.)) != null)
        and (if .type == "ActivityTaskScheduled" then
               (keys | sort) == ["activity_type", "event_id", "type"]
               and (.activity_type | identifier)
             elif .type == "MarkerRecorded" then
               (keys | sort) == ["deprecated", "event_id", "marker_name", "patch_id", "type"]
               and .marker_name == "core_patch"
               and .patch_id == $expected_patch
               and (.patch_id | identifier)
               and .deprecated == false
             else
               (keys | sort) == ["event_id", "type"]
             end);
      type == "object"
      and (keys | sort) == ["events", "run_id", "workflow_id"]
      and .workflow_id == $expected_workflow
      and .run_id == $expected_run
      and (.workflow_id | identifier)
      and (.run_id | identifier)
      and (.events | type == "array" and length >= 1 and length <= 1000000)
      and all(.events[]; event_shape)
      and ([.events[].event_id] as $ids
        | [range(1; ($ids | length)) as $index
          | (($ids[$index] | length) > ($ids[$index - 1] | length))
            or ((($ids[$index] | length) == ($ids[$index - 1] | length))
                and $ids[$index] > $ids[$index - 1])
        ] | all)
    ' "$history_path" >/dev/null; then
    fail "$label does not match the closed normalized history contract"
  fi
}

# The initial snapshot deliberately has a tiny exact event sequence.  A
# patched execution may produce the optional Core search-attribute event, but
# it can only appear after the marker and before the timer.  No broad
# subsequence matcher is used here: an extra task, activity, or terminal event
# would invalidate the stop-before-timer boundary.
validate_initial_stage() {
  history_path=$1
  if ! "$jq_bin" -e \
    --arg scenario "$scenario" \
    --argjson expected_marker_count "$expected_marker_count" \
    '
      [.events[].type] as $types
      | ([.events[] | select(.type == "MarkerRecorded")] | length)
          == $expected_marker_count
      and (if $scenario == "legacy" then
             $types == [
               "WorkflowExecutionStarted",
               "WorkflowTaskScheduled",
               "WorkflowTaskStarted",
               "WorkflowTaskCompleted",
               "TimerStarted"
             ]
           else
             $types == [
               "WorkflowExecutionStarted",
               "WorkflowTaskScheduled",
               "WorkflowTaskStarted",
               "WorkflowTaskCompleted",
               "MarkerRecorded",
               "TimerStarted"
             ]
             or $types == [
               "WorkflowExecutionStarted",
               "WorkflowTaskScheduled",
               "WorkflowTaskStarted",
               "WorkflowTaskCompleted",
               "MarkerRecorded",
               "UpsertWorkflowSearchAttributes",
               "TimerStarted"
             ]
           end)
    ' "$history_path" >/dev/null; then
    fail "$mode history does not prove the exact pending-timer boundary"
  fi
}

validate_history_shape "$history" "$mode history"

if [ "$stage" = initial ]; then
  validate_initial_stage "$history"
else
  validate_history_shape "$initial_history" "$mode initial history"
  validate_initial_stage "$initial_history"

  # The terminal history must retain every byte of the normalized initial
  # prefix.  Identity alone is not sufficient: a malformed adapter could
  # substitute a different run's history while preserving IDs in its wrapper.
  if ! "$jq_bin" -e --slurpfile initial "$initial_history" '
    . as $terminal
    | $initial[0] as $baseline
    | ($baseline.events | length) <= ($terminal.events | length)
      and all(range(0; ($baseline.events | length));
        $terminal.events[.] == $baseline.events[.])
  ' "$history" >/dev/null; then
    fail "$mode terminal history does not extend its exact initial prefix"
  fi

  # After the captured timer boundary this fixture has one exact completion
  # path.  An activity result may not be inferred from a driver string: the
  # activity's durable type name itself proves which source branch scheduled
  # it.  The event list also forbids late marker creation on an old history.
  if ! "$jq_bin" -e \
    --slurpfile initial "$initial_history" \
    --arg expected_activity "$expected_activity" \
    --argjson expected_marker_count "$expected_marker_count" \
    '
      . as $terminal
      | ($initial[0].events | length) as $prefix_length
      | ($terminal.events[$prefix_length:] | map(.type)) as $tail
      | ($tail == [
          "TimerFired",
          "WorkflowTaskScheduled",
          "WorkflowTaskStarted",
          "WorkflowTaskCompleted",
          "ActivityTaskScheduled",
          "ActivityTaskStarted",
          "ActivityTaskCompleted",
          "WorkflowTaskScheduled",
          "WorkflowTaskStarted",
          "WorkflowTaskCompleted",
          "WorkflowExecutionCompleted"
        ])
      and ([.events[] | select(.type == "MarkerRecorded")] | length)
          == $expected_marker_count
      and ([.events[] | select(.type == "ActivityTaskScheduled")]) as $activities
      | ($activities | length == 1)
      and $activities[0].activity_type == $expected_activity
      and .events[-1].type == "WorkflowExecutionCompleted"
    ' "$history" >/dev/null; then
    fail "$mode terminal history does not prove the expected branch and completion order"
  fi
fi

# Native_worker owns this deliberately compact diagnostic format.  Initial
# validation happens before replacement and therefore requires exactly its one
# observed generation-one activation.  Terminal validation additionally
# requires the generation-two replay record; no patch decision is invented
# here because the durable marker and activity type are the observable proof.
if ! "$jq_bin" -e \
  --arg expected_workflow "$workflow_id" \
  --arg expected_run "$run_id" \
  --arg stage "$stage" \
  '
    def identifier:
      type == "string" and length > 0 and length <= 65536
      and test("^[^[:cntrl:]]*$");
    def nonnegative_signed_64:
      type == "string"
      and test("^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[01][0-9]{17}|92[01][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[01][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6]|9223372036854775807)$");
    def record_shape:
      type == "object"
      and (keys | sort) == ["generation", "history_length", "is_replaying", "phase"]
      and (.phase | . == "initial" or . == "replay")
      and (.generation | type == "number" and . == floor and . >= 1 and . <= 2147483647)
      and (.is_replaying | type == "boolean")
      and (.history_length | nonnegative_signed_64);
    type == "object"
    and (keys | sort) == ["records", "run_id", "workflow_id"]
    and .workflow_id == $expected_workflow and (.workflow_id | identifier)
    and .run_id == $expected_run and (.run_id | identifier)
    and (.records | type == "array")
    and all(.records[]; record_shape)
    and (if $stage == "initial" then
           .records == [
             {
               phase: "initial",
               generation: 1,
               is_replaying: false,
               history_length: .records[0].history_length
             }
           ]
           and (.records[0].history_length != "0")
         else
           ([.records[].phase] == ["initial", "replay"])
           and .records[0].generation == 1
           and .records[0].is_replaying == false
           and .records[0].history_length != "0"
           and .records[1].generation == 2
           and .records[1].is_replaying == true
           and .records[1].history_length != "0"
         end)
  ' "$diagnostics" >/dev/null; then
  fail "$mode diagnostics do not prove the required generation boundary"
fi

event_count=$("$jq_bin" -r '.events | length' "$history")
diagnostic_count=$("$jq_bin" -r '.records | length' "$diagnostics")
printf 'patch_replay_history mode=%s workflow_id=%s run_id=%s event_count=%s diagnostic_count=%s\n' \
  "$mode" "$workflow_id" "$run_id" "$event_count" "$diagnostic_count"
