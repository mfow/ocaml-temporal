#!/bin/sh
set -eu

# Projects one Temporal CLI history response into the deliberately small,
# payload-free patch-replay contract.  The live controller only needs durable
# event order, exact execution identity, the scheduled activity type, and the
# one Core patch marker.  Rejecting every other marker shape prevents a CLI or
# protocol change from silently weakening the acceptance evidence.

# Prints the command-line contract and exits with the conventional usage code.
usage() {
  echo "usage: normalize-patch-replay-history.sh --workflow-id ID --run-id ID --output FILE" >&2
  exit 2
}

# Reports a malformed Temporal CLI response without writing a partial output
# document that a later validation command could accidentally consume.
fail() {
  echo "patch-replay history normalization failed: $*" >&2
  exit 1
}

workflow_id=''
run_id=''
output=''
while [ "$#" -gt 0 ]; do
  case "$1" in
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

[ -n "$workflow_id" ] || usage
[ -n "$run_id" ] || usage
[ -n "$output" ] || usage

jq_bin=${JQ_BIN:-jq}
command -v "$jq_bin" >/dev/null 2>&1 || fail "jq is required (set JQ_BIN to its path)"

tmp="${output}.tmp.$$"
trap 'rm -f "$tmp"' EXIT HUP INT TERM

# Temporal's protobuf JSON uses camelCase today, while some CLI releases used
# snake_case.  The adapter accepts those two stable spellings only.  It never
# copies arbitrary protobuf attributes or user payloads into the contract.
if ! "$jq_bin" -e \
  --arg expected_workflow "$workflow_id" \
  --arg expected_run "$run_id" \
  '
    def compact:
      ascii_downcase | gsub("[^a-z0-9]"; "");
    def field($object; $camel; $snake):
      ($object[$camel] // $object[$snake]);
    def identifier:
      type == "string" and length > 0 and length <= 65536
      and test("^[^[:cntrl:]]*$");
    # Reads one identity field without letting an explicitly malformed value
    # disappear through jq null-coalescing.  Temporal CLI output can repeat a
    # matching workflow ID in the execution envelope and start event, but a
    # present null, both supported spellings, or a disagreement is evidence of
    # an unsupported response rather than permission to use a fallback.
    def identity_field($object; $camel; $snake; $label):
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
    # Resolves redundant identity fields only when every explicitly supplied
    # value is the same valid identifier.  `fallback` is used solely when no
    # representation supplies the field at all; it is what keeps top-level
    # CLI HistoryEvent lists compatible after the controller has independently
    # proved their requested workflow/run pair with `workflow describe`.
    def identity_value($candidates; $fallback; $label):
      [$candidates[] | select(.present) | .value] as $values
      | if ($values | length) == 0 then $fallback
        elif (($values | all(.[]; identifier)) | not) then
          error($label + " is not a valid identifier")
        elif (($values | unique | length) != 1) then
          error($label + " disagrees across supported response fields")
        else $values[0]
        end;
    def event_type:
      (field(.; "eventType"; "event_type") // .type // "")
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
        elif . == "activitytaskscheduled" then "ActivityTaskScheduled"
        elif . == "activitytaskstarted" then "ActivityTaskStarted"
        elif . == "activitytaskcompleted" then "ActivityTaskCompleted"
        elif . == "activitytaskfailed" then "ActivityTaskFailed"
        elif . == "activitytaskcanceled" then "ActivityTaskCanceled"
        elif . == "markerrecorded" then "MarkerRecorded"
        elif . == "upsertworkflowsearchattributes" then "UpsertWorkflowSearchAttributes"
        elif . == "workflowexecutioncompleted" then "WorkflowExecutionCompleted"
        elif . == "workflowexecutionfailed" then "WorkflowExecutionFailed"
        elif . == "workflowexecutioncanceled" then "WorkflowExecutionCanceled"
        elif . == "workflowexecutionterminated" then "WorkflowExecutionTerminated"
        elif . == "workflowexecutiontimedout" then "WorkflowExecutionTimedOut"
        elif . == "workflowexecutioncontinuedasnew" then "WorkflowExecutionContinuedAsNew"
        else error("unknown Temporal event type")
        end;
    def event_id:
      (field(.; "eventId"; "event_id") // .id)
      | if type == "string" then .
        else error("Temporal event ID must be a decimal string")
        end;
    def activity_type:
      (.activityTaskScheduledEventAttributes
       // .activity_task_scheduled_event_attributes // {}) as $attributes
      | (field($attributes; "activityType"; "activity_type") // {}) as $activity
      | (field($activity; "name"; "name")) as $name
      | if ($name | identifier) then $name
        else error("activity schedule has no supported activity type name")
        end;
    def patch_marker:
      (.markerRecordedEventAttributes
       // .marker_recorded_event_attributes // {}) as $attributes
      | (field($attributes; "markerName"; "marker_name")) as $marker_name
      | if $marker_name != "core_patch" then
          error("patch-replay history contains a non-Core marker")
        else
          (field($attributes; "details"; "details") // {}) as $details
          | if ($details | type) != "object"
               or (((($details | keys | sort) == ["patch-data"])
                    or (($details | keys | sort) == ["patch_data"])) | not)
            then error("Core patch marker details do not contain exactly patch-data")
            else
              (field($details; "patch-data"; "patch_data")) as $patch_data
              | if ($patch_data | type) != "object"
                   or (($patch_data | keys | sort) != ["payloads"])
                   or ($patch_data.payloads | type) != "array"
                   or ($patch_data.payloads | length) != 1
                   or (($patch_data.payloads[0] | type) != "object")
                   or (($patch_data.payloads[0] | keys | sort) != ["data", "metadata"])
                   or (($patch_data.payloads[0].metadata | type) != "object")
                   or (($patch_data.payloads[0].metadata | keys | sort) != ["encoding"])
                   or (($patch_data.payloads[0].metadata.encoding | type) != "string")
                   or (($patch_data.payloads[0].data | type) != "string")
                then error("Core patch marker payload envelope has an unsupported shape")
                else {
                  encoding: ($patch_data.payloads[0].metadata.encoding | @base64d),
                  data: $patch_data.payloads[0].data
                } as $payload
                | if $payload.encoding != "json/plain" then
                    error("Core patch marker payload encoding is not json/plain")
                  else
                    ($payload.data | @base64d | fromjson) as $decoded
                    | if ($decoded | type) != "object"
                         or (($decoded | keys | sort) != ["deprecated", "id"])
                         or (($decoded.id | identifier) | not)
                         or (($decoded.deprecated | type) != "boolean")
                      then error("Core patch marker JSON has an unsupported shape")
                      else {
                        marker_name: "core_patch",
                        patch_id: $decoded.id,
                        deprecated: $decoded.deprecated
                      }
                      end
                  end
                end
            end
        end;
    def normalized_event:
      {event_id: event_id, type: event_type} as $base
      | if $base.type == "ActivityTaskScheduled" then
          $base + {activity_type: activity_type}
        elif $base.type == "MarkerRecorded" then
          $base + patch_marker
        else $base
        end;
    def event_list:
      (.history.events // .history_events // .events
       // .workflowExecutionHistory.events // .workflow_execution_history.events);
    def execution:
      (.workflowExecution // .workflow_execution // .execution // {});
    (event_list) as $events
    | execution as $execution
    | ($events | if type == "array" and length >= 1
                  then . else error("history has no events") end) as $raw
    | ($raw[0]
       | (.workflowExecutionStartedEventAttributes
          // .workflow_execution_started_event_attributes // {})) as $started
    | (identity_value([
         identity_field($execution; "workflowId"; "workflow_id";
                        "execution workflow ID"),
         identity_field($started; "workflowId"; "workflow_id";
                        "start-event workflow ID"),
         identity_field(.; "workflowId"; "workflow_id";
                        "top-level workflow ID")
       ]; null; "workflow ID")) as $raw_workflow
    | (if ($raw_workflow | identifier) then $raw_workflow
       else error("history has no workflow ID")
       end) as $workflow
    # A HistoryEvent list does not guarantee an enclosing run ID in every
    # supported Temporal CLI shape.  The live controller must therefore prove
    # the requested workflow/run pair first with `workflow describe` and its
    # strict identity validator.  Only after that independent proof may this
    # payload-free projection label an otherwise unlabelled event list with
    # the requested run ID.  If the CLI does include an ID, a disagreement is
    # rejected below rather than being overwritten by the command-line value.
    | (identity_value([
         identity_field($execution; "runId"; "run_id";
                        "execution run ID"),
         identity_field(.; "runId"; "run_id"; "top-level run ID")
       ]; $expected_run; "run ID")) as $raw_run
    | (if ($raw_run | identifier) then $raw_run
       else error("history has no run ID")
       end) as $run
    | {
        workflow_id: $workflow,
        run_id: $run,
        events: [$raw[] | normalized_event]
      }
    | select(.workflow_id == $expected_workflow and .run_id == $expected_run)
    | if all(.events[]; (.event_id | test("^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[01][0-9]{17}|92[01][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[01][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6]|9223372036854775807)$")))
      then . else error("event ID is outside the positive signed-64 range") end
  ' >"$tmp"; then
  fail "CLI response was not a supported patch-replay history document"
fi

mv "$tmp" "$output"
trap - EXIT HUP INT TERM
