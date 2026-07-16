#!/bin/sh
set -eu

# Validates the durable evidence for a parent workflow awaiting one timer-based
# child across worker replacement. The validator deliberately consumes only
# normalized, payload-free JSON so Temporal CLI response changes cannot widen
# the acceptance boundary through an unreviewed attribute or payload field.

# Prints the command-line contract and exits with the conventional usage code.
usage() {
  cat >&2 <<'EOF'
usage: validate-parent-child-restart-replay.sh \
       --stage initial|post-removal|terminal \
       --parent-history FILE --child-history FILE --diagnostics FILE \
       --parent-workflow-id ID --parent-run-id ID \
       --child-workflow-id ID --child-run-id ID \
       [--outcome success|failure] [--child-workflow-type TYPE] \
       [--parent-initial-history FILE --child-initial-history FILE] \
       [--parent-post-removal-history FILE --child-post-removal-history FILE]

initial:      accepts exactly the generation-one initial snapshots and two records.
post-removal: also requires the initial snapshots; verifies nonterminal prefixes
              after generation-one removal and before generation-two readiness.
terminal:     also requires both initial and post-removal snapshots; verifies
              replay records, exact prefixes, and the selected terminal order.
EOF
  exit 2
}

# Reports a protocol failure without echoing untrusted workflow payloads or
# failure details from raw Temporal history.
fail() {
  echo "parent-child restart-replay validation failed: $*" >&2
  exit 1
}

stage=''
parent_history=''
child_history=''
diagnostics=''
parent_initial_history=''
child_initial_history=''
parent_post_removal_history=''
child_post_removal_history=''
parent_workflow_id=''
parent_run_id=''
child_workflow_id=''
child_run_id=''
# This acceptance protocol is intentionally specific to the live smoke child,
# so a matching identifier alone cannot make a different workflow definition
# look like the expected durable parent-child relationship.
expected_child_workflow_type='smoke.parent_child_restart_child'
outcome='success'
while [ "$#" -gt 0 ]; do
  case "$1" in
    --stage)
      [ "$#" -ge 2 ] || usage
      stage=$2
      shift 2
      ;;
    --parent-history)
      [ "$#" -ge 2 ] || usage
      parent_history=$2
      shift 2
      ;;
    --child-history)
      [ "$#" -ge 2 ] || usage
      child_history=$2
      shift 2
      ;;
    --diagnostics)
      [ "$#" -ge 2 ] || usage
      diagnostics=$2
      shift 2
      ;;
    --parent-initial-history)
      [ "$#" -ge 2 ] || usage
      parent_initial_history=$2
      shift 2
      ;;
    --child-initial-history)
      [ "$#" -ge 2 ] || usage
      child_initial_history=$2
      shift 2
      ;;
    --parent-post-removal-history)
      [ "$#" -ge 2 ] || usage
      parent_post_removal_history=$2
      shift 2
      ;;
    --child-post-removal-history)
      [ "$#" -ge 2 ] || usage
      child_post_removal_history=$2
      shift 2
      ;;
    --parent-workflow-id)
      [ "$#" -ge 2 ] || usage
      parent_workflow_id=$2
      shift 2
      ;;
    --parent-run-id)
      [ "$#" -ge 2 ] || usage
      parent_run_id=$2
      shift 2
      ;;
    --child-workflow-id)
      [ "$#" -ge 2 ] || usage
      child_workflow_id=$2
      shift 2
      ;;
    --child-run-id)
      [ "$#" -ge 2 ] || usage
      child_run_id=$2
      shift 2
      ;;
    --child-workflow-type)
      [ "$#" -ge 2 ] || usage
      expected_child_workflow_type=$2
      shift 2
      ;;
    --outcome)
      [ "$#" -ge 2 ] || usage
      outcome=$2
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

case "$stage" in
  initial|post-removal|terminal) ;;
  *) usage ;;
esac
case "$outcome" in
  success|failure) ;;
  *) usage ;;
esac
for value in \
  "$parent_history" "$child_history" "$diagnostics" \
  "$parent_workflow_id" "$parent_run_id" "$child_workflow_id" "$child_run_id"; do
  [ -n "$value" ] || usage
done
for file in "$parent_history" "$child_history" "$diagnostics"; do
  [ -r "$file" ] || fail "required file is not readable: $file"
done

case "$stage" in
  initial)
    [ -z "$parent_initial_history" ] || fail "initial stage must not receive initial-history arguments"
    [ -z "$child_initial_history" ] || fail "initial stage must not receive initial-history arguments"
    [ -z "$parent_post_removal_history" ] || fail "initial stage must not receive post-removal-history arguments"
    [ -z "$child_post_removal_history" ] || fail "initial stage must not receive post-removal-history arguments"
    ;;
  post-removal)
    [ -n "$parent_initial_history" ] || fail "post-removal stage requires --parent-initial-history"
    [ -n "$child_initial_history" ] || fail "post-removal stage requires --child-initial-history"
    [ -z "$parent_post_removal_history" ] || fail "post-removal stage must not receive --parent-post-removal-history"
    [ -z "$child_post_removal_history" ] || fail "post-removal stage must not receive --child-post-removal-history"
    for file in "$parent_initial_history" "$child_initial_history"; do
      [ -r "$file" ] || fail "initial history file is not readable: $file"
    done
    ;;
  terminal)
    for file in \
      "$parent_initial_history" "$child_initial_history" \
      "$parent_post_removal_history" "$child_post_removal_history"; do
      [ -n "$file" ] || fail "terminal stage requires initial and post-removal histories"
      [ -r "$file" ] || fail "history file is not readable: $file"
    done
    ;;
esac

jq_bin=${JQ_BIN:-jq}
command -v "$jq_bin" >/dev/null 2>&1 || fail "jq is required (set JQ_BIN to its path)"

# Validates the closed normalized-history shape for one role. This duplicates
# the schema's essential checks so the Makefile-only contract does not depend
# on a host JSON Schema executable, and it makes cross-file assertions below
# safe to write without defensive defaults.
validate_history_shape() {
  history_path=$1
  expected_role=$2
  expected_workflow=$3
  expected_run=$4
  if ! "$jq_bin" -e \
    --arg expected_role "$expected_role" \
    --arg expected_workflow "$expected_workflow" \
    --arg expected_run "$expected_run" \
    '
      def identifier:
        type == "string" and length > 0 and utf8bytelength <= 4096
        and test("^[^[:cntrl:]]*$");
      def positive_signed_64:
        type == "string"
        and test("^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[01][0-9]{17}|92[01][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[01][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6]|9223372036854775807)$");
      def plain_type:
        . == "WorkflowTaskScheduled"
        or . == "WorkflowTaskStarted"
        or . == "WorkflowTaskCompleted"
        or . == "WorkflowTaskFailed"
        or . == "WorkflowTaskTimedOut"
        or . == "TimerStarted"
        or . == "TimerFired"
        or . == "TimerCanceled"
        or . == "WorkflowExecutionCompleted"
        or . == "WorkflowExecutionFailed"
        or . == "WorkflowExecutionCanceled"
        or . == "WorkflowExecutionTerminated"
        or . == "WorkflowExecutionTimedOut";
      def child_terminal_type:
        . == "ChildWorkflowExecutionCompleted"
        or . == "ChildWorkflowExecutionFailed"
        or . == "ChildWorkflowExecutionCanceled"
        or . == "ChildWorkflowExecutionTimedOut"
        or . == "ChildWorkflowExecutionTerminated";
      def event_shape($role):
        type == "object"
        and (.event_id | positive_signed_64)
        and (.type | type == "string")
        and (if .type == "WorkflowExecutionStarted" then
               if $role == "parent" then
                 (keys | sort) == ["event_id", "type"]
               else
                 (keys | sort) == ["event_id", "parent_initiated_event_id", "parent_run_id", "parent_workflow_id", "type", "workflow_type"]
                 and (.workflow_type | identifier)
                 and (.parent_workflow_id | identifier)
                 and (.parent_run_id | identifier)
                 and (.parent_initiated_event_id | positive_signed_64)
               end
             elif .type == "StartChildWorkflowExecutionInitiated" then
               (keys | sort) == ["child_workflow_id", "child_workflow_type", "event_id", "type"]
               and (.child_workflow_id | identifier)
               and (.child_workflow_type | identifier)
             elif .type == "StartChildWorkflowExecutionFailed" then
               (keys | sort) == ["child_workflow_id", "child_workflow_type", "event_id", "initiated_event_id", "type"]
               and (.child_workflow_id | identifier)
               and (.child_workflow_type | identifier)
               and (.initiated_event_id | positive_signed_64)
             elif .type == "ChildWorkflowExecutionStarted" then
               (keys | sort) == ["child_run_id", "child_workflow_id", "child_workflow_type", "event_id", "initiated_event_id", "type"]
               and (.child_workflow_id | identifier)
               and (.child_run_id | identifier)
               and (.child_workflow_type | identifier)
               and (.initiated_event_id | positive_signed_64)
             elif (.type | child_terminal_type) then
               (keys | sort) == ["child_run_id", "child_workflow_id", "child_workflow_type", "event_id", "initiated_event_id", "started_event_id", "type"]
               and (.child_workflow_id | identifier)
               and (.child_run_id | identifier)
               and (.child_workflow_type | identifier)
               and (.initiated_event_id | positive_signed_64)
               and (.started_event_id | positive_signed_64)
             elif (.type | plain_type) then
               (keys | sort) == ["event_id", "type"]
             else false
             end);
      def strictly_increasing($ids):
        [range(1; ($ids | length)) as $index
         | (($ids[$index] | length) > ($ids[$index - 1] | length))
           or ((($ids[$index] | length) == ($ids[$index - 1] | length))
               and $ids[$index] > $ids[$index - 1])]
        | all;
      type == "object"
      and (keys | sort) == ["events", "role", "run_id", "workflow_id"]
      and .role == $expected_role
      and .workflow_id == $expected_workflow
      and .run_id == $expected_run
      and (.workflow_id | identifier)
      and (.run_id | identifier)
      and (.events | type == "array" and length >= 1 and length <= 1000000)
      and all(.events[]; event_shape($expected_role))
      and ([.events[].event_id] | strictly_increasing(.))
    ' "$history_path" >/dev/null; then
    fail "$expected_role history does not match the closed normalized-history shape"
  fi
}

# Validates the common parent-child relationship that must already be durable
# in the initial snapshot: a parent initiation, its started event, and the
# child workflow's reciprocal parent linkage all identify the same executions.
validate_initial_pair() {
  initial_parent=$1
  initial_child=$2
  if ! "$jq_bin" -n -e \
    --slurpfile parent "$initial_parent" \
    --slurpfile child "$initial_child" \
    --arg parent_workflow "$parent_workflow_id" \
    --arg parent_run "$parent_run_id" \
    --arg child_workflow "$child_workflow_id" \
    --arg child_run "$child_run_id" \
    --arg child_workflow_type "$expected_child_workflow_type" \
    '
      $parent[0] as $parent_history
      | $child[0] as $child_history
      | (["WorkflowExecutionStarted", "WorkflowTaskScheduled", "WorkflowTaskStarted", "WorkflowTaskCompleted", "StartChildWorkflowExecutionInitiated", "ChildWorkflowExecutionStarted", "WorkflowTaskScheduled", "WorkflowTaskStarted", "WorkflowTaskCompleted"] == [$parent_history.events[].type])
      | select(.)
      | (["WorkflowExecutionStarted", "WorkflowTaskScheduled", "WorkflowTaskStarted", "WorkflowTaskCompleted", "TimerStarted"] == [$child_history.events[].type])
      | select(.)
      | $parent_history.events[4] as $initiated
      | $parent_history.events[5] as $started
      | $child_history.events[0] as $child_started
      | $initiated.child_workflow_id == $child_workflow
      and $initiated.child_workflow_type == $child_workflow_type
      and $started.child_workflow_id == $child_workflow
      and $started.child_run_id == $child_run
      and $started.initiated_event_id == $initiated.event_id
      and $started.child_workflow_type == $child_workflow_type
      and $child_started.parent_workflow_id == $parent_workflow
      and $child_started.parent_run_id == $parent_run
      and $child_started.parent_initiated_event_id == $initiated.event_id
      and $child_started.workflow_type == $child_workflow_type
    ' >/dev/null; then
    fail "initial histories do not prove one cross-linked nonterminal child execution"
  fi
}

# Validates the snapshots made after the generation-one container is removed.
# The child timer may fire independently while no worker exists, but neither
# workflow may become terminal and the child may not gain a worker-driven task
# completion before the fresh generation is ready.
validate_post_removal_pair() {
  initial_parent=$1
  initial_child=$2
  post_parent=$3
  post_child=$4
  if ! "$jq_bin" -n -e \
    --slurpfile initial_parent "$initial_parent" \
    --slurpfile initial_child "$initial_child" \
    --slurpfile post_parent "$post_parent" \
    --slurpfile post_child "$post_child" \
    '
      def prefix($prefix; $whole):
        $whole.events[0:($prefix.events | length)] == $prefix.events;
      def terminal_type:
        . == "WorkflowExecutionCompleted"
        or . == "WorkflowExecutionFailed"
        or . == "WorkflowExecutionCanceled"
        or . == "WorkflowExecutionTerminated"
        or . == "WorkflowExecutionTimedOut"
        or . == "ChildWorkflowExecutionCompleted"
        or . == "ChildWorkflowExecutionFailed"
        or . == "ChildWorkflowExecutionCanceled"
        or . == "ChildWorkflowExecutionTimedOut"
        or . == "ChildWorkflowExecutionTerminated";
      $initial_parent[0] as $initial_parent_history
      | $initial_child[0] as $initial_child_history
      | $post_parent[0] as $post_parent_history
      | $post_child[0] as $post_child_history
      | prefix($initial_parent_history; $post_parent_history)
      and prefix($initial_child_history; $post_child_history)
      and $post_parent_history.events == $initial_parent_history.events
      and (($post_parent_history.events | map(.type) | any(terminal_type)) | not)
      and (($post_child_history.events | map(.type) | any(terminal_type)) | not)
      and ([ $post_child_history.events[($initial_child_history.events | length):][].type ] == []
           or [ $post_child_history.events[($initial_child_history.events | length):][].type ] == ["TimerFired", "WorkflowTaskScheduled"])
      and (($post_child_history.events | map(.type) | map(select(. == "TimerStarted")) | length) == 1)
      and (($post_child_history.events | map(.type) | map(select(. == "TimerFired")) | length) <= 1)
    ' >/dev/null; then
    fail "post-removal histories do not preserve the nonterminal durable prefix"
  fi
}

# Validates the fixed role-keyed diagnostics document. Record lengths are
# activation observations, not normalized snapshot counts: they must be
# positive, bounded by the related snapshot, and monotonic across generations.
validate_diagnostics() {
  expected_record_count=$1
  initial_parent=$2
  initial_child=$3
  terminal_parent=$4
  terminal_child=$5
  if ! "$jq_bin" -e \
    --slurpfile initial_parent "$initial_parent" \
    --slurpfile initial_child "$initial_child" \
    --slurpfile terminal_parent "$terminal_parent" \
    --slurpfile terminal_child "$terminal_child" \
    --arg parent_workflow "$parent_workflow_id" \
    --arg parent_run "$parent_run_id" \
    --arg child_workflow "$child_workflow_id" \
    --arg child_run "$child_run_id" \
    --argjson expected_record_count "$expected_record_count" \
    '
      def identifier:
        type == "string" and length > 0 and utf8bytelength <= 4096
        and test("^[^[:cntrl:]]*$");
      def positive_signed_64:
        type == "string"
        and test("^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[01][0-9]{17}|92[01][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[01][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6]|9223372036854775807)$");
      def execution($workflow; $run):
        type == "object"
        and (keys | sort) == ["run_id", "workflow_id"]
        and .workflow_id == $workflow and .run_id == $run
        and (.workflow_id | identifier) and (.run_id | identifier);
      def decimal_lte($left; $right):
        (($left | length) < ($right | length))
        or ((($left | length) == ($right | length)) and $left <= $right);
      def record($role; $phase; $generation; $replaying):
        type == "object"
        and (keys | sort) == ["generation", "history_length", "is_replaying", "phase", "role"]
        and .role == $role and .phase == $phase and .generation == $generation
        and .is_replaying == $replaying and (.history_length | positive_signed_64);
      $initial_parent[0] as $initial_parent_history
      | $initial_child[0] as $initial_child_history
      | $terminal_parent[0] as $terminal_parent_history
      | $terminal_child[0] as $terminal_child_history
      | ($initial_parent_history.events | length | tostring) as $initial_parent_length
      | ($initial_child_history.events | length | tostring) as $initial_child_length
      | ($terminal_parent_history.events | length | tostring) as $terminal_parent_length
      | ($terminal_child_history.events | length | tostring) as $terminal_child_length
      | type == "object"
      and (keys | sort) == ["child", "parent", "records"]
      and (.parent | execution($parent_workflow; $parent_run))
      and (.child | execution($child_workflow; $child_run))
      and (.records | type == "array" and length == $expected_record_count)
      and (.records[0] | record("parent"; "initial"; 1; false))
      and (.records[1] | record("child"; "initial"; 1; false))
      and decimal_lte(.records[0].history_length; $initial_parent_length)
      and decimal_lte(.records[1].history_length; $initial_child_length)
      and (if $expected_record_count == 2 then true
           else (.records[2] | record("parent"; "replay"; 2; true))
             and (.records[3] | record("child"; "replay"; 2; true))
             and decimal_lte(.records[0].history_length; .records[2].history_length)
             and decimal_lte(.records[1].history_length; .records[3].history_length)
             and decimal_lte(.records[2].history_length; $terminal_parent_length)
             and decimal_lte(.records[3].history_length; $terminal_child_length)
           end)
    ' "$diagnostics" >/dev/null; then
    fail "diagnostics do not match the fixed role-keyed replay record contract"
  fi
}

validate_history_shape "$parent_history" parent "$parent_workflow_id" "$parent_run_id"
validate_history_shape "$child_history" child "$child_workflow_id" "$child_run_id"

case "$stage" in
  initial)
    validate_initial_pair "$parent_history" "$child_history"
    validate_diagnostics 2 "$parent_history" "$child_history" "$parent_history" "$child_history"
    ;;
  post-removal)
    validate_history_shape "$parent_initial_history" parent "$parent_workflow_id" "$parent_run_id"
    validate_history_shape "$child_initial_history" child "$child_workflow_id" "$child_run_id"
    validate_initial_pair "$parent_initial_history" "$child_initial_history"
    validate_post_removal_pair \
      "$parent_initial_history" "$child_initial_history" \
      "$parent_history" "$child_history"
    validate_diagnostics 2 \
      "$parent_initial_history" "$child_initial_history" \
      "$parent_history" "$child_history"
    ;;
  terminal)
    validate_history_shape "$parent_initial_history" parent "$parent_workflow_id" "$parent_run_id"
    validate_history_shape "$child_initial_history" child "$child_workflow_id" "$child_run_id"
    validate_history_shape "$parent_post_removal_history" parent "$parent_workflow_id" "$parent_run_id"
    validate_history_shape "$child_post_removal_history" child "$child_workflow_id" "$child_run_id"
    validate_initial_pair "$parent_initial_history" "$child_initial_history"
    validate_post_removal_pair \
      "$parent_initial_history" "$child_initial_history" \
      "$parent_post_removal_history" "$child_post_removal_history"
    if ! "$jq_bin" -n -e \
      --slurpfile initial_parent "$parent_initial_history" \
      --slurpfile initial_child "$child_initial_history" \
      --slurpfile post_parent "$parent_post_removal_history" \
      --slurpfile post_child "$child_post_removal_history" \
      --slurpfile terminal_parent "$parent_history" \
      --slurpfile terminal_child "$child_history" \
      --arg child_workflow "$child_workflow_id" \
      --arg child_run "$child_run_id" \
      --arg expected_child_workflow_type "$expected_child_workflow_type" \
      --arg outcome "$outcome" \
      '
        def prefix($prefix; $whole):
          $whole.events[0:($prefix.events | length)] == $prefix.events;
        $initial_parent[0] as $initial_parent_history
        | $initial_child[0] as $initial_child_history
        | $post_parent[0] as $post_parent_history
        | $post_child[0] as $post_child_history
        | $terminal_parent[0] as $terminal_parent_history
        | $terminal_child[0] as $terminal_child_history
        | $initial_parent_history.events[4] as $initiated
        | $initial_parent_history.events[5] as $started
        | $terminal_parent_history.events[($post_parent_history.events | length)] as $child_terminal
        | prefix($initial_parent_history; $terminal_parent_history)
        and prefix($initial_child_history; $terminal_child_history)
        and prefix($post_parent_history; $terminal_parent_history)
        and prefix($post_child_history; $terminal_child_history)
        and (if $outcome == "success" then
               ["ChildWorkflowExecutionCompleted", "WorkflowTaskScheduled", "WorkflowTaskStarted", "WorkflowTaskCompleted", "WorkflowExecutionCompleted"]
               == [$terminal_parent_history.events[($post_parent_history.events | length):][].type]
             else
               ["ChildWorkflowExecutionFailed", "WorkflowTaskScheduled", "WorkflowTaskStarted", "WorkflowTaskCompleted", "WorkflowExecutionCompleted"]
               == [$terminal_parent_history.events[($post_parent_history.events | length):][].type]
             end)
        and (if $outcome == "success" then
               (([$post_child_history.events[($initial_child_history.events | length):][].type] == []
                 and ["TimerFired", "WorkflowTaskScheduled", "WorkflowTaskStarted", "WorkflowTaskCompleted", "WorkflowExecutionCompleted"]
                     == [$terminal_child_history.events[($post_child_history.events | length):][].type])
                or ([$post_child_history.events[($initial_child_history.events | length):][].type] == ["TimerFired", "WorkflowTaskScheduled"]
                    and ["WorkflowTaskStarted", "WorkflowTaskCompleted", "WorkflowExecutionCompleted"]
                        == [$terminal_child_history.events[($post_child_history.events | length):][].type]))
             else
               (([$post_child_history.events[($initial_child_history.events | length):][].type] == []
                 and ["TimerFired", "WorkflowTaskScheduled", "WorkflowTaskStarted", "WorkflowTaskCompleted", "WorkflowExecutionFailed"]
                     == [$terminal_child_history.events[($post_child_history.events | length):][].type])
                or ([$post_child_history.events[($initial_child_history.events | length):][].type] == ["TimerFired", "WorkflowTaskScheduled"]
                    and ["WorkflowTaskStarted", "WorkflowTaskCompleted", "WorkflowExecutionFailed"]
                        == [$terminal_child_history.events[($post_child_history.events | length):][].type]))
             end)
        and $child_terminal.child_workflow_id == $child_workflow
        and $child_terminal.child_run_id == $child_run
        and $child_terminal.child_workflow_type == $expected_child_workflow_type
        and $child_terminal.initiated_event_id == $initiated.event_id
        and $child_terminal.started_event_id == $started.event_id
      ' >/dev/null; then
      fail "terminal histories do not preserve the post-removal prefix and selected outcome order"
    fi
    validate_diagnostics 4 \
      "$parent_initial_history" "$child_initial_history" \
      "$parent_history" "$child_history"
    ;;
esac

parent_event_count=$($jq_bin '.events | length' "$parent_history")
child_event_count=$($jq_bin '.events | length' "$child_history")
printf 'parent_child_restart_replay stage=%s parent_events=%s child_events=%s\n' \
  "$stage" "$parent_event_count" "$child_event_count"
