#!/bin/sh
set -eu

# Validates the payload-free evidence for the live one-entry sticky-cache
# scenario. JSON Schema documents the individual diagnostic record, while this
# script checks the cross-document identity, ordering, and history-prefix
# invariants that standard JSON Schema cannot express.

usage() {
  cat >&2 <<'EOF'
usage: validate-cache-eviction.sh --stage initial --initial-history FILE
       --workflow-id ID --run-id ID

       validate-cache-eviction.sh --diagnostics FILE --initial-history FILE
       --terminal-history FILE --workflow-id ID --run-id ID
EOF
  exit 2
}

# Reports one bounded validation failure. The input files are deliberately
# normalized projections, so no workflow payload or Temporal failure detail is
# included in an error message.
fail() {
  echo "cache-eviction validation failed: $*" >&2
  exit 1
}

diagnostics=''
initial_history=''
terminal_history=''
workflow_id=''
run_id=''
stage=terminal
while [ "$#" -gt 0 ]; do
  case "$1" in
    --diagnostics)
      [ "$#" -ge 2 ] || usage
      diagnostics=$2
      shift 2
      ;;
    --initial-history)
      [ "$#" -ge 2 ] || usage
      initial_history=$2
      shift 2
      ;;
    --terminal-history)
      [ "$#" -ge 2 ] || usage
      terminal_history=$2
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
    --help|-h)
      usage
      ;;
    *)
      usage
      ;;
  esac
done

[ -n "$initial_history" ] || usage
[ -n "$workflow_id" ] || usage
[ -n "$run_id" ] || usage
[ "$stage" = initial ] || [ "$stage" = terminal ] || usage
[ -r "$initial_history" ] || fail "initial history is not readable"
if [ "$stage" = terminal ]; then
  [ -n "$diagnostics" ] || usage
  [ -n "$terminal_history" ] || usage
  [ -r "$diagnostics" ] || fail "diagnostics file is not readable"
  [ -r "$terminal_history" ] || fail "terminal history is not readable"
fi

jq_bin=${JQ_BIN:-jq}
command -v "$jq_bin" >/dev/null 2>&1 || fail "jq is required (set JQ_BIN to its path)"

# The normalizer already rejects unknown Temporal event variants. Repeat the
# closed projection shape here so the cache evidence is safe even when this
# validator is used without the normalizer's own prior invocation.
validate_history_shape() {
  history_path=$1
  if ! "$jq_bin" -e --arg workflow_id "$workflow_id" --arg run_id "$run_id" '
    type == "object"
    and (keys | sort) == ["events", "run_id", "workflow_id"]
    and .workflow_id == $workflow_id
    and .run_id == $run_id
    and (.events | type == "array" and length >= 1 and length <= 1000000)
    and (all(.events[];
      type == "object"
      and (keys | sort) == ["event_id", "type"]
      and (.event_id | type == "string" and test("^[1-9][0-9]{0,18}$"))
      and (.event_id | length < 19 or (length == 19 and . <= "9223372036854775807"))
      and (.type | type == "string" and length > 0 and length <= 128)
    ))
    and (([.events[].event_id] as $ids |
      [range(1; ($ids | length)) as $i |
        (($ids[$i] | length) > ($ids[$i - 1] | length))
        or ((($ids[$i] | length) == ($ids[$i - 1] | length))
            and $ids[$i] > $ids[$i - 1])
      ] | all))
  ' "$history_path" >/dev/null; then
    fail "history document does not match the normalized closed shape"
  fi
}

validate_history_shape "$initial_history"

# Checks a required event-type subsequence without caring about polling events
# inserted by Temporal Server between the durable workflow boundaries.
has_order='def has_order($required):
  reduce .events[].type as $actual
    ({index: 0};
      if .index < ($required | length) and $actual == $required[.index]
      then .index += 1
      else .
      end)
  | .index == ($required | length);'

# Before the pressure workflow starts, the target must have scheduled a timer
# but not fired it or reached any terminal state. This means the one-entry
# cache has an idle sticky execution to evict rather than a finished run.
validate_initial_stage() {
  history_path=$1
  if ! "$jq_bin" -e --argjson terminal_types '[
    "WorkflowExecutionCompleted", "WorkflowExecutionFailed",
    "WorkflowExecutionCanceled", "WorkflowExecutionTerminated",
    "WorkflowExecutionTimedOut", "WorkflowExecutionContinuedAsNew"
  ]' "$has_order"'
    has_order(["WorkflowExecutionStarted", "WorkflowTaskCompleted", "TimerStarted"])
    and (all(.events[];
      . as $event
      | $event.type != "TimerFired"
        and all($terminal_types[]; . != $event.type)
    ))
  ' "$history_path" >/dev/null; then
    fail "initial history does not prove an outstanding durable timer"
  fi
}

validate_initial_stage "$initial_history"

# The controller polls this stage before it creates the pressure workflow. A
# standalone success proves only the safe pre-pressure boundary; terminal and
# diagnostic evidence remains mandatory for the default full validation.
if [ "$stage" = initial ]; then
  event_count=$("$jq_bin" -r '.events | length' "$initial_history")
  printf 'cache_eviction_history stage=initial workflow_id=%s run_id=%s event_count=%s\n' \
    "$workflow_id" "$run_id" "$event_count"
  exit 0
fi

validate_history_shape "$terminal_history"

# The completed target history must preserve the exact initial event prefix,
# then show its timer firing and a successful terminal completion. A rewritten
# or truncated history would otherwise let an unrelated result satisfy the
# CacheFull diagnostic for the original run.
if ! "$jq_bin" -e --slurpfile initial "$initial_history" "$has_order"'
  ($initial[0]) as $initial
  | (.events[0:($initial.events | length)] == $initial.events)
    and has_order([
      "WorkflowExecutionStarted", "WorkflowTaskCompleted", "TimerStarted",
      "TimerFired", "WorkflowExecutionCompleted"
    ])
    and .events[-1].type == "WorkflowExecutionCompleted"
' "$terminal_history" >/dev/null; then
  fail "terminal history does not preserve the target timer-to-completion path"
fi

# The three diagnostic records are an exact protocol: initial activation,
# post-acknowledgement CacheFull evidence, then a replay activation. The long
# regex keeps decimal text inside signed 64-bit range without converting it to
# jq's floating-point number representation.
if ! "$jq_bin" -e --arg workflow_id "$workflow_id" --arg run_id "$run_id" '
  def non_negative_i64:
    test("^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[01][0-9]{17}|92[01][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[01][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6]|9223372036854775807)$");
  type == "object"
  and (keys | sort) == ["records", "run_id", "workflow_id"]
  and .workflow_id == $workflow_id
  and .run_id == $run_id
  and (.workflow_id | type == "string" and length > 0 and length <= 65536)
  and (.run_id | type == "string" and length > 0 and length <= 65536)
  and (.records | type == "array" and length == 3)
  and (.records[0] | type == "object"
       and (keys | sort) == ["history_length", "is_replaying", "phase"]
       and .phase == "initial" and .is_replaying == false
       and (.history_length | type == "string" and non_negative_i64))
  and (.records[1] | type == "object"
       and (keys | sort) == ["empty_completion", "phase"]
       and .phase == "cache_full_acknowledged" and .empty_completion == true)
  and (.records[2] | type == "object"
       and (keys | sort) == ["history_length", "is_replaying", "phase"]
       and .phase == "replay" and .is_replaying == true
       and (.history_length | type == "string" and non_negative_i64 and . != "0"))
' "$diagnostics" >/dev/null; then
  fail "diagnostics do not prove accepted CacheFull eviction followed by replay"
fi
