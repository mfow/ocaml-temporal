#!/bin/sh
set -eu

# Validates the small, payload-free documents exchanged by the live
# restart/replay controller. The Temporal CLI does not provide the normalized
# history shape consumed here, so this script accepts the JSON projection
# written by [normalize-history.sh] rather than scraping human-formatted CLI
# output. Terminal validation also requires the initial snapshot: checking the
# two documents independently would allow an adapter bug to replace the
# workflow's history between the restart boundary and completion.

# Prints the command-line contract and exits with the conventional usage code.
usage() {
  cat >&2 <<'EOF'
usage: validate-restart-replay.sh --history FILE --workflow-id ID --run-id ID
       [--stage initial|terminal] [--initial-history FILE]
       [--diagnostics FILE] [--require-replay]
EOF
  exit 2
}

# Reports one invariant failure without exposing workflow payloads or history
# attributes that are intentionally outside this diagnostic contract.
fail() {
  echo "restart/replay validation failed: $*" >&2
  exit 1
}

history=''
initial_history=''
diagnostics=''
workflow_id=''
run_id=''
stage=terminal
require_replay=0

while [ "$#" -gt 0 ]; do
  case "$1" in
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
    --stage)
      [ "$#" -ge 2 ] || usage
      stage=$2
      shift 2
      ;;
    --require-replay)
      require_replay=1
      shift
      ;;
    --help|-h)
      usage
      ;;
    *)
      usage
      ;;
  esac
done

[ -n "$history" ] || usage
[ -n "$workflow_id" ] || usage
[ -n "$run_id" ] || usage
[ "$stage" = initial ] || [ "$stage" = terminal ] || usage
[ -r "$history" ] || fail "history file is not readable: $history"
if [ "$stage" = terminal ] && [ -z "$initial_history" ]; then
  fail "terminal stage requires --initial-history"
fi
if [ -n "$initial_history" ] && [ ! -r "$initial_history" ]; then
  fail "initial history file is not readable: $initial_history"
fi
if [ -n "$diagnostics" ] && [ ! -r "$diagnostics" ]; then
  fail "diagnostics file is not readable: $diagnostics"
fi
if [ "$require_replay" -eq 1 ] && [ -z "$diagnostics" ]; then
  fail "--require-replay requires --diagnostics"
fi

jq_bin=${JQ_BIN:-jq}
command -v "$jq_bin" >/dev/null 2>&1 || fail "jq is required (set JQ_BIN to its path)"

# The schema files document this contract for external tooling. These checks
# repeat the safety-critical invariants here so the Make target remains a
# useful standalone gate even when no JSON-schema CLI is installed.
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
  "WorkflowExecutionCompleted",
  "WorkflowExecutionFailed",
  "WorkflowExecutionCanceled",
  "WorkflowExecutionTerminated",
  "WorkflowExecutionTimedOut",
  "WorkflowExecutionContinuedAsNew"
]'
terminal_types='[
  "WorkflowTaskFailed",
  "TimerCanceled",
  "ActivityTaskFailed",
  "ActivityTaskCanceled",
  "WorkflowExecutionFailed",
  "WorkflowExecutionCanceled",
  "WorkflowExecutionTerminated",
  "WorkflowExecutionTimedOut",
  "WorkflowExecutionContinuedAsNew",
  "WorkflowExecutionCompleted"
]'

# Validates the top-level object, exact object keys, bounded identifiers,
# event shape, and strictly increasing server event IDs. Event IDs are decimal
# strings rather than JSON numbers: Temporal uses signed 64-bit IDs, and
# converting them to a double would silently lose ordering precision above
# 2^53. No payload or arbitrary event attributes are accepted here. The
# function is used for both snapshots so the prefix comparison below never
# compares an unchecked baseline with a checked terminal history.
validate_history_shape() {
  history_path=$1
  error_message=$2
  if ! "$jq_bin" -e \
    --arg expected_workflow "$workflow_id" \
    --arg expected_run "$run_id" \
    --argjson event_types "$event_types" \
    '
      type == "object"
      and (keys | sort) == ["events", "run_id", "workflow_id"]
      and (.workflow_id == $expected_workflow)
      and (.run_id == $expected_run)
      and (.workflow_id | type == "string" and length > 0 and length <= 65536)
      and (.run_id | type == "string" and length > 0 and length <= 65536)
      and (.events | type == "array" and length >= 1 and length <= 1000000)
      and (all(.events[];
        (type == "object")
        and ((keys | sort) == ["event_id", "type"])
        and (.event_id | type == "string" and test("^[1-9][0-9]{0,18}$"))
        and (.event_id |
          length < 19 or (length == 19 and . <= "9223372036854775807"))
        and (.type | type == "string" and ($event_types | index(.)) != null)
      ))
      and (([.events[].event_id] as $ids |
        [range(1; ($ids | length)) as $i |
          (($ids[$i] | length) > ($ids[$i - 1] | length))
          or ((($ids[$i] | length) == ($ids[$i - 1] | length))
              and $ids[$i] > $ids[$i - 1])
        ] | all))
    ' "$history_path" >/dev/null; then
    fail "$error_message"
  fi
}

validate_history_shape "$history" \
  "history document does not match the strict normalized contract"

# This helper treats the required event names as an ordered subsequence. It
# permits unrelated scheduling/started events between the boundaries while
# still rejecting a history that reaches the terminal result out of order.
has_order='def has_order($required):
  reduce .events[].type as $actual
    ({index: 0};
      if .index < ($required | length) and $actual == $required[.index]
      then .index += 1
      else .
      end)
  | .index == ($required | length);'

# Checks that one history ends at the observed pending-timer boundary without
# a timer firing, activity, or terminal event that would make worker shutdown
# unsafe.
validate_initial_stage() {
  history_path=$1
  error_message=$2
  if ! "$jq_bin" -e \
    --argjson required '["WorkflowExecutionStarted", "WorkflowTaskCompleted", "TimerStarted"]' \
    --argjson terminal_types "$terminal_types" \
     "$has_order
     has_order(\$required)
     and (all(.events[];
       . as \$event
       | all(\$terminal_types[]; . != \$event.type)
         and \$event.type != \"TimerFired\"
         and \$event.type != \"ActivityTaskScheduled\"
         and \$event.type != \"ActivityTaskStarted\"
         and \$event.type != \"ActivityTaskCompleted\"
     ))" "$history_path" >/dev/null; then
    fail "$error_message"
  fi
}

# Checks that one history contains the complete timer/activity path and ends
# with a successful workflow completion rather than a failure or cancellation.
validate_terminal_stage() {
  history_path=$1
  if ! "$jq_bin" -e \
    --argjson required '["WorkflowExecutionStarted", "WorkflowTaskCompleted", "TimerStarted", "TimerFired", "ActivityTaskScheduled", "ActivityTaskCompleted", "WorkflowExecutionCompleted"]' \
    --argjson terminal_types "$terminal_types" \
    "$has_order
     has_order(\$required)
     and .events[-1].type == \"WorkflowExecutionCompleted\"
     and (all(.events[0:-1][];
       . as \$event | all(\$terminal_types[]; . != \$event.type)
     ))" "$history_path" >/dev/null; then
    fail "terminal history does not prove timer, activity, and completion order"
  fi
}

if [ "$stage" = initial ]; then
  validate_initial_stage "$history" \
    "initial history does not prove a pending timer before worker stop"
else
  validate_history_shape "$initial_history" \
    "initial history does not match the strict normalized contract"
  validate_initial_stage "$initial_history" \
    "initial history does not prove a pending timer before worker stop"

  # The terminal document must extend the exact initial event prefix. Matching
  # only workflow/run IDs is insufficient: a malformed adapter could return a
  # different history for the same IDs and falsely turn an unrelated success
  # into restart/replay evidence.
  if ! "$jq_bin" -e \
    --slurpfile initial "$initial_history" \
    '
      . as $terminal
      | $initial[0] as $baseline
      | ($baseline.events | length) <= ($terminal.events | length)
      and all(range(0; ($baseline.events | length));
        $terminal.events[.] == $baseline.events[.])
    ' "$history" >/dev/null; then
    fail "terminal history does not extend the validated initial history"
  fi

  validate_terminal_stage "$history"
fi

event_count=$("$jq_bin" -r '.events | length' "$history")
printf 'restart_replay_history stage=%s workflow_id=%s run_id=%s event_count=%s\n' \
  "$stage" "$workflow_id" "$run_id" "$event_count"

if [ -n "$diagnostics" ]; then
  # Diagnostic records deliberately carry no workflow input, payload bytes,
  # activity output, timestamps, or process identifiers. The same run identity
  # is repeated at the document root and checked against the history above.
  if ! "$jq_bin" -e \
    --arg expected_workflow "$workflow_id" \
    --arg expected_run "$run_id" \
    '
      type == "object"
      and (keys | sort) == ["records", "run_id", "workflow_id"]
      and .workflow_id == $expected_workflow
      and .run_id == $expected_run
      and (.records | type == "array" and length >= 1 and length <= 16)
      and (all(.records[];
        (type == "object")
        and ((keys | sort) == ["generation", "history_length", "is_replaying", "phase"])
        and (.phase | . == "initial" or . == "replay")
        and (.generation | type == "number" and . == floor and . >= 1 and . <= 2147483647)
        and (.is_replaying | type == "boolean")
        and (.history_length | type == "string" and test("^(?:0|[1-9][0-9]{0,18})$"))
        and (.history_length |
          length < 19 or (length == 19 and . <= "9223372036854775807"))
      ))
      and ([.records[] | .phase] | index("initial") == 0)
      and (all(.records[];
        if .phase == "initial"
        then .generation == 1 and .is_replaying == false
        else .generation >= 2 and .is_replaying == true and .history_length != "0"
        end
      ))
    ' "$diagnostics" >/dev/null; then
    fail "diagnostics document does not match the strict replay marker contract"
  fi

  if [ "$require_replay" -eq 1 ]; then
    if ! "$jq_bin" -e \
      '([.records[] | .phase] == ["initial", "replay"])
       and .records[1].generation == 2
       and .records[1].is_replaying == true
       and .records[1].history_length != "0"' "$diagnostics" >/dev/null; then
      fail "diagnostics do not contain the required generation-2 replay marker"
    fi
  fi

  diagnostic_count=$("$jq_bin" -r '.records | length' "$diagnostics")
  if [ "$require_replay" -eq 1 ]; then
    replay_status=true
  else
    replay_status=false
  fi
  printf 'restart_replay_diagnostics workflow_id=%s run_id=%s record_count=%s replay_required=%s\n' \
    "$workflow_id" "$run_id" "$diagnostic_count" "$replay_status"
fi
