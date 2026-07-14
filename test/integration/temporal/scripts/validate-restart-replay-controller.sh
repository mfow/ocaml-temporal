#!/bin/sh
set -eu

# Validates the payload-free lifecycle evidence emitted by the Docker Compose
# restart/replay controller. The controller is deliberately modeled
# as an ordered document rather than inferred from interleaved container logs:
# a history check, worker replacement, replay marker, terminal result, and
# volume cleanup must be observed in that order before an acceptance run can
# be called successful. Replacement mode distinguishes a graceful stop from a
# forced process crash, so a crash run cannot accidentally pass on a stale
# shutdown marker.

# Prints the command-line contract and exits with the conventional usage code.
usage() {
  cat >&2 <<'EOF'
usage: validate-restart-replay-controller.sh \
       --controller FILE --workflow-id ID --run-id ID \
       [--replacement-mode graceful|crash]
EOF
  exit 2
}

# Reports one invariant failure without copying payloads, process output, or
# remote error text into the diagnostic stream.
fail() {
  echo "restart/replay controller validation failed: $*" >&2
  exit 1
}

controller=''
workflow_id=''
run_id=''
replacement_mode=graceful

while [ "$#" -gt 0 ]; do
  case "$1" in
    --controller)
      [ "$#" -ge 2 ] || usage
      controller=$2
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
    --replacement-mode)
      [ "$#" -ge 2 ] || usage
      replacement_mode=$2
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

[ -n "$controller" ] || usage
[ -n "$workflow_id" ] || usage
[ -n "$run_id" ] || usage
[ -r "$controller" ] || fail "controller file is not readable: $controller"
case "$replacement_mode" in
  graceful|crash) ;;
  *) fail "unsupported replacement mode: $replacement_mode" ;;
esac

jq_bin=${JQ_BIN:-jq}
command -v "$jq_bin" >/dev/null 2>&1 || fail "jq is required (set JQ_BIN to its path)"

# The schema documents the same closed record for external tooling. These
# checks intentionally remain local so the Makefile gate does not depend on a
# JSON-Schema executable being installed on a contributor's host.
if ! "$jq_bin" -e \
  --arg expected_workflow "$workflow_id" \
  --arg expected_run "$run_id" \
  --arg expected_replacement_mode "$replacement_mode" \
  '
    def identifier:
      type == "string"
      and length >= 1 and length <= 65536
      # The jq regular-expression parser handles POSIX control classes more
      # predictably than JSON-style unicode escapes.  The schema remains the
      # normative representation; this keeps the shell-side check equivalent.
      and test("^[^[:cntrl:]]*$");
    def container_id:
      type == "string" and test("^[a-f0-9]{12,64}$");
    def positive_count:
      type == "number" and . == floor and . >= 1 and . <= 1000000;
    def nonnegative_count:
      type == "number" and . == floor and . >= 0 and . <= 1000000;
    def positive_signed_64:
      type == "string"
      and test("^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[01][0-9]{17}|92[01][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[01][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6]|9223372036854775807)$");
    type == "object"
    and (keys | sort) == ["events", "run_id", "workflow_id"]
    and (.workflow_id == $expected_workflow and (.workflow_id | identifier))
    and (.run_id == $expected_run and (.run_id | identifier))
    and (.events | type == "array" and length == 13)
    and (.events[0] | type == "object"
      and (keys | sort) == ["remaining_project_volumes_before_start", "stale_project_volumes_before_cleanup", "status", "step", "temporal_healthy"]
      and .step == "stack_ready"
      and .status == "ok"
      and (.stale_project_volumes_before_cleanup | nonnegative_count)
      and .remaining_project_volumes_before_start == 0
      and .temporal_healthy == true)
    and (.events[1] | type == "object"
      and (keys | sort) == ["run_id", "status", "step", "workflow_id"]
      and .step == "driver_accepted"
      and .status == "ok"
      and .workflow_id == $expected_workflow
      and .run_id == $expected_run
      and (.workflow_id | identifier)
      and (.run_id | identifier))
    and (.events[2] | type == "object"
      and (keys | sort) == ["event_count", "stage", "status", "step"]
      and .step == "history_checked"
      and .status == "ok"
      and .stage == "initial"
      and (.event_count | positive_count))
    and (.events[3] == {"step": "driver_waiting", "status": "ok"})
    and (.events[4] | type == "object"
      and (keys | sort) == ["container_id", "exit_code", "generation", "replacement_mode", "shutdown_marker", "status", "step"]
      and .step == "generation_one_replaced"
      and .status == "ok"
      and .generation == 1
      and (.container_id | container_id)
      and .replacement_mode == $expected_replacement_mode
      and (.exit_code | type == "number" and . == floor and . >= 0 and . <= 255)
      and (.shutdown_marker | type == "boolean")
      and (if $expected_replacement_mode == "graceful"
           then .exit_code == 0 and .shutdown_marker == true
           else .exit_code == 137 and .shutdown_marker == false
           end))
    and (.events[5] | type == "object"
      and (keys | sort) == ["container_id", "generation", "remaining_worker_containers", "status", "step"]
      and .step == "generation_one_removed"
      and .status == "ok"
      and .generation == 1
      and (.container_id | container_id)
      and .remaining_worker_containers == 0)
    and (.events[6] | type == "object"
      and (keys | sort) == ["container_id", "fresh_container", "generation", "readiness_generation", "status", "step"]
      and .step == "generation_two_ready"
      and .status == "ok"
      and .generation == 2
      and (.container_id | container_id)
      and .readiness_generation == 2
      and .fresh_container == true)
    and (.events[7] | type == "object"
      and (keys | sort) == ["generation", "history_length", "is_replaying", "status", "step"]
      and .step == "replay_observed"
      and .status == "ok"
      and .generation == 2
      and .is_replaying == true
      and (.history_length | positive_signed_64))
    and (.events[8] | type == "object"
      and (keys | sort) == ["event_count", "stage", "status", "step"]
      and .step == "history_checked"
      and .status == "ok"
      and .stage == "terminal"
      and (.event_count | positive_count))
    and (.events[9] == {"step": "driver_completed", "status": "ok", "outcome": "completed"})
    and (.events[10] | type == "object"
      and (keys | sort) == ["container_id", "exit_code", "generation", "shutdown_marker", "status", "step"]
      and .step == "generation_two_stopped"
      and .status == "ok"
      and .generation == 2
      and (.container_id | container_id)
      and .exit_code == 0
      and .shutdown_marker == true)
    and (.events[11] | type == "object"
      and (keys | sort) == ["container_id", "generation", "remaining_worker_containers", "status", "step"]
      and .step == "generation_two_removed"
      and .status == "ok"
      and .generation == 2
      and (.container_id | container_id)
      and .remaining_worker_containers == 0)
    and (.events[12] == {
      "step": "postgres_volume_removed",
      "status": "ok",
      "remaining_project_volumes": 0
    })
    # The replaced worker must be removed before the replacement is accepted.
    and (.events[4].container_id == .events[5].container_id)
    and (.events[4].container_id != .events[6].container_id)
    and (.events[5].remaining_worker_containers == 0)
    and (.events[6].fresh_container == true)
    and (.events[10].container_id == .events[11].container_id)
    and (.events[10].container_id == .events[6].container_id)
    and (.events[11].remaining_worker_containers == 0)
  ' "$controller" >/dev/null; then
  fail "controller document does not match the strict ordered lifecycle contract"
fi

printf 'restart_replay_controller workflow_id=%s run_id=%s steps=13 replacement_mode=%s container_replaced=true volume_removed=true\n' \
  "$workflow_id" "$run_id" "$replacement_mode"
