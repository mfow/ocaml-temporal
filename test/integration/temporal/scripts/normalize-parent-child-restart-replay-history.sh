#!/bin/sh
set -eu

# Projects a single Temporal CLI history into the narrow, payload-free
# parent-child restart-replay protocol. The caller supplies both exact run
# identities so the projection can reject a child lifecycle event belonging to
# a different execution before any cross-history comparison takes place.

# Prints the command-line contract and exits with the conventional usage code.
usage() {
  cat >&2 <<'EOF'
usage: normalize-parent-child-restart-replay-history.sh \
       --role parent|child \
       --workflow-id ID --run-id ID \
       --counterpart-workflow-id ID --counterpart-run-id ID \
       --output FILE < temporal-cli-history.json
EOF
  exit 2
}

# Reports a malformed or unsupported CLI response without leaving a partial
# projection at the requested output path.
fail() {
  echo "parent-child restart-replay history normalization failed: $*" >&2
  exit 1
}

role=''
workflow_id=''
run_id=''
counterpart_workflow_id=''
counterpart_run_id=''
output=''
while [ "$#" -gt 0 ]; do
  case "$1" in
    --role)
      [ "$#" -ge 2 ] || usage
      role=$2
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
    --counterpart-workflow-id)
      [ "$#" -ge 2 ] || usage
      counterpart_workflow_id=$2
      shift 2
      ;;
    --counterpart-run-id)
      [ "$#" -ge 2 ] || usage
      counterpart_run_id=$2
      shift 2
      ;;
    --output)
      [ "$#" -ge 2 ] || usage
      output=$2
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

case "$role" in
  parent|child) ;;
  *) usage ;;
esac
[ -n "$workflow_id" ] || usage
[ -n "$run_id" ] || usage
[ -n "$counterpart_workflow_id" ] || usage
[ -n "$counterpart_run_id" ] || usage
[ -n "$output" ] || usage

jq_bin=${JQ_BIN:-jq}
command -v "$jq_bin" >/dev/null 2>&1 || fail "jq is required (set JQ_BIN to its path)"

# Creating the temporary file beside the destination keeps `mv` on one file
# system. A failed parse therefore cannot expose a partial JSON document to a
# controller that is polling the output directory.
tmp=$(mktemp "${output}.tmp.XXXXXX") || fail "could not create an output temporary file"
trap 'rm -f "$tmp"' EXIT HUP INT TERM

# The Temporal CLI serializes protobuf field names in camelCase. Some older
# versions used snake_case, so the adapter accepts exactly those two spellings.
# Every helper rejects a duplicate spelling even when values happen to agree:
# duplicated representation is an unsupported response shape, not evidence
# from which to guess a safer value.
if ! "$jq_bin" -e \
  --arg role "$role" \
  --arg expected_workflow "$workflow_id" \
  --arg expected_run "$run_id" \
  --arg expected_counterpart_workflow "$counterpart_workflow_id" \
  --arg expected_counterpart_run "$counterpart_run_id" \
  '
    def identifier:
      type == "string" and length > 0 and utf8bytelength <= 4096
      and test("^[^[:cntrl:]]*$");
    def positive_signed_64:
      type == "string"
      and test("^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[01][0-9]{17}|92[01][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[01][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6]|9223372036854775807)$");
    def compact:
      ascii_downcase | gsub("[^a-z0-9]"; "");
    def key_candidate($object; $key; $label):
      if ($object | type) != "object" then
        error($label + " parent is not an object")
      elif ($object | has($key)) then
        {present: true, value: $object[$key]}
      else
        {present: false, value: null}
      end;
    def field_candidate($object; $camel; $snake; $label):
      if ($object | type) != "object" then
        error($label + " parent is not an object")
      elif (($object | has($camel)) and ($object | has($snake))) then
        error($label + " uses both supported spellings")
      elif ($object | has($camel)) then
        {present: true, value: $object[$camel]}
      elif ($object | has($snake)) then
        {present: true, value: $object[$snake]}
      else
        {present: false, value: null}
      end;
    def required_field($object; $camel; $snake; $label):
      field_candidate($object; $camel; $snake; $label) as $candidate
      | if $candidate.present then $candidate.value
        else error($label + " is missing")
        end;
    def one_candidate($candidates; $label):
      [$candidates[] | select(.present) | .value] as $values
      | if ($values | length) == 1 then $values[0]
        elif ($values | length) == 0 then error($label + " is missing")
        else error($label + " is ambiguous across supported response fields")
        end;
    def identity_value($candidates; $fallback; $label):
      [$candidates[] | select(.present) | .value] as $values
      | if ($values | length) == 0 then $fallback
        elif (($values | all(.[]; identifier)) | not) then
          error($label + " is not a valid identifier")
        elif (($values | unique | length) != 1) then
          error($label + " disagrees across supported response fields")
        else $values[0]
        end;
    def nested_events_candidate($object; $container; $label):
      key_candidate($object; $container; $label) as $outer
      | if ($outer.present | not) then $outer
        elif ($outer.value | type) != "object" then
          error($label + " is not an object")
        else key_candidate($outer.value; "events"; $label + ".events")
        end;
    def event_list:
      one_candidate([
        nested_events_candidate(.; "history"; "history"),
        key_candidate(.; "history_events"; "history_events"),
        key_candidate(.; "events"; "events"),
        nested_events_candidate(.; "workflowExecutionHistory"; "workflowExecutionHistory"),
        nested_events_candidate(.; "workflow_execution_history"; "workflow_execution_history")
      ]; "history event list")
      | if type == "array" and length >= 1 and length <= 1000000 then .
        else error("history event list is not a bounded non-empty array")
        end;
    def execution_envelope:
      [
        key_candidate(.; "workflowExecution"; "workflowExecution"),
        key_candidate(.; "workflow_execution"; "workflow_execution"),
        key_candidate(.; "execution"; "execution")
      ] as $candidates
      | [$candidates[] | select(.present) | .value] as $values
      | if ($values | length) == 0 then {}
        elif ($values | length) == 1 and ($values[0] | type) == "object" then $values[0]
        elif ($values | length) == 1 then error("execution envelope is not an object")
        else error("execution envelope is ambiguous across supported response fields")
        end;
    def event_type($event):
      required_field($event; "eventType"; "event_type"; "event type") as $raw
      | if ($raw | type) != "string" then error("event type is not a string") else $raw end
      | compact
      | sub("^eventtype"; "")
      | sub("^historyeventtype"; "")
      | if . == "workflowexecutionstarted" then "WorkflowExecutionStarted"
        elif . == "workflowtaskscheduled" then "WorkflowTaskScheduled"
        elif . == "workflowtaskstarted" then "WorkflowTaskStarted"
        elif . == "workflowtaskcompleted" then "WorkflowTaskCompleted"
        elif . == "workflowtaskfailed" then "WorkflowTaskFailed"
        elif . == "workflowtasktimedout" then "WorkflowTaskTimedOut"
        elif . == "timerstarted" then "TimerStarted"
        elif . == "timerfired" then "TimerFired"
        elif . == "timercanceled" then "TimerCanceled"
        elif . == "startchildworkflowexecutioninitiated" then "StartChildWorkflowExecutionInitiated"
        elif . == "startchildworkflowexecutionfailed" then "StartChildWorkflowExecutionFailed"
        elif . == "childworkflowexecutionstarted" then "ChildWorkflowExecutionStarted"
        elif . == "childworkflowexecutioncompleted" then "ChildWorkflowExecutionCompleted"
        elif . == "childworkflowexecutionfailed" then "ChildWorkflowExecutionFailed"
        elif . == "childworkflowexecutioncanceled" then "ChildWorkflowExecutionCanceled"
        elif . == "childworkflowexecutiontimedout" then "ChildWorkflowExecutionTimedOut"
        elif . == "childworkflowexecutionterminated" then "ChildWorkflowExecutionTerminated"
        elif . == "workflowexecutioncompleted" then "WorkflowExecutionCompleted"
        elif . == "workflowexecutionfailed" then "WorkflowExecutionFailed"
        elif . == "workflowexecutioncanceled" then "WorkflowExecutionCanceled"
        elif . == "workflowexecutionterminated" then "WorkflowExecutionTerminated"
        elif . == "workflowexecutiontimedout" then "WorkflowExecutionTimedOut"
        else error("unknown Temporal event type")
        end;
    def event_id($event):
      required_field($event; "eventId"; "event_id"; "event ID") as $value
      | if ($value | positive_signed_64) then $value
        else error("event ID is outside the positive signed-64 range")
        end;
    def attribute_object($event; $camel; $snake; $label):
      required_field($event; $camel; $snake; $label) as $attributes
      | if ($attributes | type) == "object" then $attributes
        else error($label + " is not an object")
        end;
    def workflow_type($attributes; $label):
      required_field($attributes; "workflowType"; "workflow_type"; $label + " workflow type") as $type
      | if ($type | type) != "object" then
          error($label + " workflow type is not an object")
        elif (($type | keys | sort) != ["name"]) then
          error($label + " workflow type has an unsupported shape")
        elif ($type.name | identifier) then $type.name
        else error($label + " workflow type name is not a valid identifier")
        end;
    def execution_identity($execution; $label):
      if ($execution | type) != "object" then
        error($label + " is not an object")
      elif (($execution | keys | sort) == ["runId", "workflowId"]) then
        {workflow_id: $execution.workflowId, run_id: $execution.runId}
      elif (($execution | keys | sort) == ["run_id", "workflow_id"]) then
        {workflow_id: $execution.workflow_id, run_id: $execution.run_id}
      else
        error($label + " has an unsupported identity shape")
      end
      | if (.workflow_id | identifier) and (.run_id | identifier) then .
        else error($label + " contains an invalid identity")
        end;
    def child_execution($attributes; $label):
      required_field($attributes; "workflowExecution"; "workflow_execution"; $label + " workflow execution")
      | execution_identity(.; $label + " workflow execution");
    def positive_attribute_id($attributes; $camel; $snake; $label):
      required_field($attributes; $camel; $snake; $label) as $value
      | if ($value | positive_signed_64) then $value
        else error($label + " is outside the positive signed-64 range")
        end;
    def checked_counterpart($execution; $label):
      if $execution.workflow_id == $expected_counterpart_workflow
         and $execution.run_id == $expected_counterpart_run then $execution
      else error($label + " disagrees with the expected counterpart execution")
      end;
    def child_started_event($base; $attributes):
      child_execution($attributes; "child started")
      | checked_counterpart(.; "child started") as $execution
      | workflow_type($attributes; "child started") as $type
      | positive_attribute_id($attributes; "initiatedEventId"; "initiated_event_id"; "child started initiated event ID") as $initiated
      | $base + {
          child_workflow_id: $execution.workflow_id,
          child_run_id: $execution.run_id,
          child_workflow_type: $type,
          initiated_event_id: $initiated
        };
    def child_terminal_event($base; $attributes):
      child_execution($attributes; "child terminal")
      | checked_counterpart(.; "child terminal") as $execution
      | workflow_type($attributes; "child terminal") as $type
      | positive_attribute_id($attributes; "initiatedEventId"; "initiated_event_id"; "child terminal initiated event ID") as $initiated
      | positive_attribute_id($attributes; "startedEventId"; "started_event_id"; "child terminal started event ID") as $started
      | $base + {
          child_workflow_id: $execution.workflow_id,
          child_run_id: $execution.run_id,
          child_workflow_type: $type,
          initiated_event_id: $initiated,
          started_event_id: $started
        };
    def normalized_event($role):
      {event_id: event_id(.), type: event_type(.)} as $base
      | if $base.type == "WorkflowExecutionStarted" and $role == "child" then
          attribute_object(.; "workflowExecutionStartedEventAttributes"; "workflow_execution_started_event_attributes"; "child workflow started attributes") as $attributes
          | (required_field($attributes; "parentWorkflowExecution"; "parent_workflow_execution"; "child workflow parent execution")
             | execution_identity(.; "child workflow parent execution")
             | checked_counterpart(.; "child workflow parent execution")) as $parent
          | workflow_type($attributes; "child workflow started") as $type
          | positive_attribute_id($attributes; "parentInitiatedEventId"; "parent_initiated_event_id"; "child workflow parent initiated event ID") as $initiated
          | $base + {
              workflow_type: $type,
              parent_workflow_id: $parent.workflow_id,
              parent_run_id: $parent.run_id,
              parent_initiated_event_id: $initiated
            }
        elif $base.type == "StartChildWorkflowExecutionInitiated" then
          attribute_object(.; "startChildWorkflowExecutionInitiatedEventAttributes"; "start_child_workflow_execution_initiated_event_attributes"; "child start initiated attributes") as $attributes
          | required_field($attributes; "workflowId"; "workflow_id"; "child start initiated workflow ID") as $child_workflow
          | (if ($child_workflow | identifier) and $child_workflow == $expected_counterpart_workflow then $child_workflow
             else error("child start initiated workflow ID disagrees with the expected counterpart workflow")
             end) as $child_workflow
          | workflow_type($attributes; "child start initiated") as $type
          | $base + {child_workflow_id: $child_workflow, child_workflow_type: $type}
        elif $base.type == "StartChildWorkflowExecutionFailed" then
          attribute_object(.; "startChildWorkflowExecutionFailedEventAttributes"; "start_child_workflow_execution_failed_event_attributes"; "child start failed attributes") as $attributes
          | required_field($attributes; "workflowId"; "workflow_id"; "child start failed workflow ID") as $child_workflow
          | (if ($child_workflow | identifier) and $child_workflow == $expected_counterpart_workflow then $child_workflow
             else error("child start failed workflow ID disagrees with the expected counterpart workflow")
             end) as $child_workflow
          | workflow_type($attributes; "child start failed") as $type
          | positive_attribute_id($attributes; "initiatedEventId"; "initiated_event_id"; "child start failed initiated event ID") as $initiated
          | $base + {
              child_workflow_id: $child_workflow,
              child_workflow_type: $type,
              initiated_event_id: $initiated
            }
        elif $base.type == "ChildWorkflowExecutionStarted" then
          attribute_object(.; "childWorkflowExecutionStartedEventAttributes"; "child_workflow_execution_started_event_attributes"; "child started attributes")
          | child_started_event($base; .)
        elif ($base.type == "ChildWorkflowExecutionCompleted"
              or $base.type == "ChildWorkflowExecutionFailed"
              or $base.type == "ChildWorkflowExecutionCanceled"
              or $base.type == "ChildWorkflowExecutionTimedOut"
              or $base.type == "ChildWorkflowExecutionTerminated") then
          if $base.type == "ChildWorkflowExecutionCompleted" then
            attribute_object(.; "childWorkflowExecutionCompletedEventAttributes"; "child_workflow_execution_completed_event_attributes"; "child completed attributes")
          elif $base.type == "ChildWorkflowExecutionFailed" then
            attribute_object(.; "childWorkflowExecutionFailedEventAttributes"; "child_workflow_execution_failed_event_attributes"; "child failed attributes")
          elif $base.type == "ChildWorkflowExecutionCanceled" then
            attribute_object(.; "childWorkflowExecutionCanceledEventAttributes"; "child_workflow_execution_canceled_event_attributes"; "child canceled attributes")
          elif $base.type == "ChildWorkflowExecutionTimedOut" then
            attribute_object(.; "childWorkflowExecutionTimedOutEventAttributes"; "child_workflow_execution_timed_out_event_attributes"; "child timed out attributes")
          else
            attribute_object(.; "childWorkflowExecutionTerminatedEventAttributes"; "child_workflow_execution_terminated_event_attributes"; "child terminated attributes")
          end
          | child_terminal_event($base; .)
        else $base
        end;
    def strictly_increasing($ids):
      [range(1; ($ids | length)) as $index
       | (($ids[$index] | length) > ($ids[$index - 1] | length))
         or ((($ids[$index] | length) == ($ids[$index - 1] | length))
             and $ids[$index] > $ids[$index - 1])]
      | all;
    . as $document
    | if ($role == "parent" or $role == "child")
         and ($expected_workflow | identifier)
         and ($expected_run | identifier)
         and ($expected_counterpart_workflow | identifier)
         and ($expected_counterpart_run | identifier) then .
      else error("command-line identity is invalid")
      end
    | (event_list) as $raw
    | (execution_envelope) as $execution
    | ($raw[0] | event_type(.)) as $first_type
    | if $first_type == "WorkflowExecutionStarted" then .
      else error("history does not begin with WorkflowExecutionStarted")
      end
    | ($raw[0]
       | attribute_object(.; "workflowExecutionStartedEventAttributes"; "workflow_execution_started_event_attributes"; "workflow started attributes")) as $started
    | (identity_value([
         field_candidate($execution; "workflowId"; "workflow_id"; "execution workflow ID"),
         field_candidate($started; "workflowId"; "workflow_id"; "start workflow ID"),
         field_candidate($document; "workflowId"; "workflow_id"; "top-level workflow ID")
       ]; null; "workflow ID")) as $actual_workflow
    | if ($actual_workflow | identifier) and $actual_workflow == $expected_workflow then .
      else error("history workflow ID disagrees with the requested execution")
      end
    | (identity_value([
         field_candidate($execution; "runId"; "run_id"; "execution run ID"),
         field_candidate($document; "runId"; "run_id"; "top-level run ID")
       ]; $expected_run; "run ID")) as $actual_run
    | if ($actual_run | identifier) and $actual_run == $expected_run then .
      else error("history run ID disagrees with the requested execution")
      end
    | {
        role: $role,
        workflow_id: $actual_workflow,
        run_id: $actual_run,
        events: [$raw[] | normalized_event($role)]
      }
    | if ([.events[].event_id] | strictly_increasing(.)) then .
      else error("history event IDs are not strictly increasing")
      end
  ' >"$tmp"; then
  fail "CLI response was not a supported parent-child history document"
fi

mv "$tmp" "$output"
trap - EXIT HUP INT TERM
