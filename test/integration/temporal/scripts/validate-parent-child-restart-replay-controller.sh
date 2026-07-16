#!/bin/sh
set -eu

# Validates the controller's payload-free lifecycle account. This document is
# intentionally separate from history validation: it proves that snapshots and
# replay observations were made on opposite sides of a complete worker
# replacement rather than merely appearing in an unordered test log.

# Prints the command-line contract and exits with the conventional usage code.
usage() {
  cat >&2 <<'EOF'
usage: validate-parent-child-restart-replay-controller.sh \
       --controller FILE \
       --parent-workflow-id ID --parent-run-id ID \
       --child-workflow-id ID --child-run-id ID \
       --initiated-event-id EVENT_ID \
       [--expected-outcome completed|child_failure_recovered]
EOF
  exit 2
}

# Reports one closed-controller invariant failure without printing untrusted
# document content that could contain a caller's workflow input or result.
fail() {
  echo "parent-child restart-replay controller validation failed: $*" >&2
  exit 1
}

controller=''
parent_workflow_id=''
parent_run_id=''
child_workflow_id=''
child_run_id=''
initiated_event_id=''
expected_outcome='completed'
while [ "$#" -gt 0 ]; do
  case "$1" in
    --controller)
      [ "$#" -ge 2 ] || usage
      controller=$2
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
    --initiated-event-id)
      [ "$#" -ge 2 ] || usage
      initiated_event_id=$2
      shift 2
      ;;
    --expected-outcome)
      [ "$#" -ge 2 ] || usage
      expected_outcome=$2
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

case "$expected_outcome" in
  completed|child_failure_recovered) ;;
  *) usage ;;
esac

for value in \
  "$controller" "$parent_workflow_id" "$parent_run_id" \
  "$child_workflow_id" "$child_run_id" "$initiated_event_id"; do
  [ -n "$value" ] || usage
done
[ -r "$controller" ] || fail "controller file is not readable: $controller"

jq_bin=${JQ_BIN:-jq}
command -v "$jq_bin" >/dev/null 2>&1 || fail "jq is required (set JQ_BIN to its path)"

if ! "$jq_bin" -e \
  --arg parent_workflow "$parent_workflow_id" \
  --arg parent_run "$parent_run_id" \
  --arg child_workflow "$child_workflow_id" \
  --arg child_run "$child_run_id" \
  --arg initiated_event "$initiated_event_id" \
  --arg expected_outcome "$expected_outcome" \
  '
    def identifier:
      type == "string" and length > 0 and utf8bytelength <= 4096
      and test("^[^[:cntrl:]]*$");
    def positive_signed_64:
      type == "string"
      and test("^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[01][0-9]{17}|92[01][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[01][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6]|9223372036854775807)$");
    def container_id:
      type == "string" and test("^[a-f0-9]{12,64}$");
    def positive_count:
      type == "number" and floor == . and . >= 1 and . <= 1000000;
    def stack_ready:
      type == "object"
      and (keys | sort) == ["remaining_project_volumes_before_start", "status", "step", "temporal_healthy"]
      and .step == "stack_ready" and .status == "ok"
      and .temporal_healthy == true and .remaining_project_volumes_before_start == 0;
    def parent_driver_accepted:
      type == "object"
      and (keys | sort) == ["run_id", "status", "step", "workflow_id"]
      and .step == "parent_driver_accepted" and .status == "ok"
      and .workflow_id == $parent_workflow and .run_id == $parent_run;
    def child_execution_observed:
      type == "object"
      and (keys | sort) == ["initiated_event_id", "parent_run_id", "parent_workflow_id", "run_id", "status", "step", "workflow_id"]
      and .step == "child_execution_observed" and .status == "ok"
      and .workflow_id == $child_workflow and .run_id == $child_run
      and .parent_workflow_id == $parent_workflow and .parent_run_id == $parent_run
      and .initiated_event_id == $initiated_event and (.initiated_event_id | positive_signed_64);
    def history_checked($role; $stage):
      type == "object"
      and (keys | sort) == ["event_count", "role", "stage", "status", "step"]
      and .step == "history_checked" and .status == "ok"
      and .role == $role and .stage == $stage and (.event_count | positive_count);
    def worker_stopped($generation):
      type == "object"
      and (keys | sort) == ["container_id", "exit_code", "generation", "shutdown_marker", "status", "step"]
      and .step == "worker_generation_stopped" and .status == "ok"
      and .generation == $generation and (.container_id | container_id)
      and .exit_code == 0 and .shutdown_marker == true;
    def worker_removed($generation):
      type == "object"
      and (keys | sort) == ["container_id", "generation", "remaining_worker_containers", "status", "step"]
      and .step == "worker_generation_removed" and .status == "ok"
      and .generation == $generation and (.container_id | container_id)
      and .remaining_worker_containers == 0;
    def worker_ready:
      type == "object"
      and (keys | sort) == ["container_id", "fresh_container", "generation", "readiness_generation", "status", "step"]
      and .step == "worker_generation_ready" and .status == "ok"
      and .generation == 2 and (.container_id | container_id)
      and .readiness_generation == 2 and .fresh_container == true;
    def replay_observed($role):
      type == "object"
      and (keys | sort) == ["generation", "history_length", "is_replaying", "role", "status", "step"]
      and .step == "replay_observed" and .status == "ok"
      and .role == $role and .generation == 2 and .is_replaying == true
      and (.history_length | positive_signed_64);
    def parent_driver_completed:
      type == "object"
      and (keys | sort) == ["outcome", "run_id", "status", "step", "workflow_id"]
      and .step == "parent_driver_completed" and .status == "ok"
      and .workflow_id == $parent_workflow and .run_id == $parent_run
      and .outcome == $expected_outcome;
    def volume_removed:
      type == "object"
      and (keys | sort) == ["remaining_project_volumes", "status", "step"]
      and .step == "postgres_volume_removed" and .status == "ok"
      and .remaining_project_volumes == 0;
    type == "object"
    and (keys | sort) == ["child_run_id", "child_workflow_id", "events", "initiated_event_id", "parent_run_id", "parent_workflow_id"]
    and .parent_workflow_id == $parent_workflow and (.parent_workflow_id | identifier)
    and .parent_run_id == $parent_run and (.parent_run_id | identifier)
    and .child_workflow_id == $child_workflow and (.child_workflow_id | identifier)
    and .child_run_id == $child_run and (.child_run_id | identifier)
    and .initiated_event_id == $initiated_event and (.initiated_event_id | positive_signed_64)
    and (.events | type == "array" and length == 18)
    and (.events[0] | stack_ready)
    and (.events[1] | parent_driver_accepted)
    and (.events[2] | child_execution_observed)
    and (.events[3] | history_checked("parent"; "initial"))
    and (.events[4] | history_checked("child"; "initial"))
    and (.events[5] | worker_stopped(1))
    and (.events[6] | worker_removed(1))
    and (.events[7] | history_checked("parent"; "post_removal"))
    and (.events[8] | history_checked("child"; "post_removal"))
    and (.events[9] | worker_ready)
    and (.events[10] | replay_observed("parent"))
    and (.events[11] | replay_observed("child"))
    and (.events[12] | parent_driver_completed)
    and (.events[13] | history_checked("parent"; "terminal"))
    and (.events[14] | history_checked("child"; "terminal"))
    and (.events[15] | worker_stopped(2))
    and (.events[16] | worker_removed(2))
    and (.events[17] | volume_removed)
    and (.events[5].container_id == .events[6].container_id)
    and (.events[5].container_id != .events[9].container_id)
    and (.events[9].container_id == .events[15].container_id)
    and (.events[15].container_id == .events[16].container_id)
  ' "$controller" >/dev/null; then
  fail "controller document does not match the closed 18-step lifecycle"
fi

printf 'parent_child_restart_replay_controller parent_workflow_id=%s parent_run_id=%s child_workflow_id=%s child_run_id=%s steps=18\n' \
  "$parent_workflow_id" "$parent_run_id" "$child_workflow_id" "$child_run_id"
