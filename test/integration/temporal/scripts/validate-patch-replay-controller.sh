#!/bin/sh
set -eu

# Validates the ordered, payload-free lifecycle record produced by the live
# old/new patch replay controller.  It intentionally records only observable
# facts: distinct containers, exact run identities, normalized-history marker
# counts, and the branch selected by the durable activity type.  It does not
# claim internal SDK decisions or commands that the controller cannot observe.

# Prints the command-line contract and exits with the conventional usage code.
usage() {
  cat >&2 <<'EOF'
usage: validate-patch-replay-controller.sh \
       --controller FILE \
       --legacy-workflow-id ID --legacy-run-id ID \
       --new-workflow-id ID --new-run-id ID
EOF
  exit 2
}

# Reports one lifecycle-contract failure without printing container logs,
# workflow payloads, or server error text that could contain application data.
fail() {
  echo "patch-replay controller validation failed: $*" >&2
  exit 1
}

controller=''
legacy_workflow_id=''
legacy_run_id=''
new_workflow_id=''
new_run_id=''
while [ "$#" -gt 0 ]; do
  case "$1" in
    --controller)
      [ "$#" -ge 2 ] || usage
      controller=$2
      shift 2
      ;;
    --legacy-workflow-id)
      [ "$#" -ge 2 ] || usage
      legacy_workflow_id=$2
      shift 2
      ;;
    --legacy-run-id)
      [ "$#" -ge 2 ] || usage
      legacy_run_id=$2
      shift 2
      ;;
    --new-workflow-id)
      [ "$#" -ge 2 ] || usage
      new_workflow_id=$2
      shift 2
      ;;
    --new-run-id)
      [ "$#" -ge 2 ] || usage
      new_run_id=$2
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
[ -n "$legacy_workflow_id" ] || usage
[ -n "$legacy_run_id" ] || usage
[ -n "$new_workflow_id" ] || usage
[ -n "$new_run_id" ] || usage
[ -r "$controller" ] || fail "controller file is not readable: $controller"

jq_bin=${JQ_BIN:-jq}
command -v "$jq_bin" >/dev/null 2>&1 || fail "jq is required (set JQ_BIN to its path)"

# The document has a fixed cardinality because the controller executes two
# complete source-replacement scenarios in one Compose stack.  Position-based
# checking makes temporal order explicit: a terminal history or cleanup record
# cannot be accepted before the corresponding generation has been replaced.
if ! "$jq_bin" -e \
  --arg legacy_workflow "$legacy_workflow_id" \
  --arg legacy_run "$legacy_run_id" \
  --arg new_workflow "$new_workflow_id" \
  --arg new_run "$new_run_id" \
  '
    def identifier:
      type == "string" and length > 0 and length <= 65536
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
    def stack_ready:
      type == "object"
      and (keys | sort) == ["remaining_project_volumes_before_start", "stale_project_volumes_before_cleanup", "status", "step", "temporal_healthy"]
      and .step == "stack_ready" and .status == "ok"
      and (.stale_project_volumes_before_cleanup | nonnegative_count)
      and .remaining_project_volumes_before_start == 0
      and .temporal_healthy == true;
    def driver_accepted($scenario; $workflow; $run):
      type == "object"
      and (keys | sort) == ["run_id", "scenario", "status", "step", "workflow_id"]
      and .step == "driver_accepted" and .status == "ok"
      and .scenario == $scenario
      and .workflow_id == $workflow and (.workflow_id | identifier)
      and .run_id == $run and (.run_id | identifier);
    def history_checked($scenario; $stage):
      type == "object"
      and (keys | sort) == ["event_count", "scenario", "stage", "status", "step"]
      and .step == "history_checked" and .status == "ok"
      and .scenario == $scenario and .stage == $stage
      and (.event_count | positive_count);
    def worker_stopped($scenario; $generation; $worker_version):
      type == "object"
      and (keys | sort) == ["container_id", "exit_code", "generation", "scenario", "shutdown_marker", "status", "step", "worker_version"]
      and .step == "worker_generation_stopped" and .status == "ok"
      and .scenario == $scenario and .generation == $generation
      and (.container_id | container_id)
      and .worker_version == $worker_version
      and .exit_code == 0 and .shutdown_marker == true;
    def worker_removed($scenario; $generation):
      type == "object"
      and (keys | sort) == ["container_id", "generation", "remaining_worker_containers", "scenario", "status", "step"]
      and .step == "worker_generation_removed" and .status == "ok"
      and .scenario == $scenario and .generation == $generation
      and (.container_id | container_id)
      and .remaining_worker_containers == 0;
    def worker_ready($scenario; $worker_version):
      type == "object"
      and (keys | sort) == ["container_id", "fresh_container", "generation", "readiness_generation", "scenario", "status", "step", "worker_version"]
      and .step == "worker_generation_ready" and .status == "ok"
      and .scenario == $scenario and .generation == 2
      and (.container_id | container_id)
      and .worker_version == $worker_version
      and .readiness_generation == 2 and .fresh_container == true;
    def replay_observed($scenario; $branch; $marker_count):
      type == "object"
      and (keys | sort) == ["branch", "generation", "history_length", "is_replaying", "marker_count", "scenario", "status", "step"]
      and .step == "replay_observed" and .status == "ok"
      and .scenario == $scenario and .generation == 2
      and .is_replaying == true and (.history_length | positive_signed_64)
      and .branch == $branch and .marker_count == $marker_count;
    def driver_completed($scenario):
      type == "object"
      and (keys | sort) == ["outcome", "scenario", "status", "step"]
      and .step == "driver_completed" and .status == "ok"
      and .scenario == $scenario and .outcome == "completed";
    def volume_removed:
      type == "object"
      and (keys | sort) == ["remaining_project_volumes", "status", "step"]
      and .step == "postgres_volume_removed" and .status == "ok"
      and .remaining_project_volumes == 0;
    type == "object"
    and (keys | sort) == ["events", "legacy_run_id", "legacy_workflow_id", "new_run_id", "new_workflow_id"]
    and .legacy_workflow_id == $legacy_workflow and (.legacy_workflow_id | identifier)
    and .legacy_run_id == $legacy_run and (.legacy_run_id | identifier)
    and .new_workflow_id == $new_workflow and (.new_workflow_id | identifier)
    and .new_run_id == $new_run and (.new_run_id | identifier)
    and (.events | type == "array" and length == 22)
    and (.events[0] | stack_ready)
    and (.events[1] | driver_accepted("legacy"; $legacy_workflow; $legacy_run))
    and (.events[2] | history_checked("legacy"; "initial"))
    and (.events[3] | worker_stopped("legacy"; 1; "legacy"))
    and (.events[4] | worker_removed("legacy"; 1))
    and (.events[5] | worker_ready("legacy"; "patched"))
    and (.events[6] | replay_observed("legacy"; "old"; 0))
    # The runner joins the client driver before it asks Temporal for the
    # terminal snapshot.  The evidence order must mirror that real operation:
    # a terminal history check is not claimed before driver completion.
    and (.events[7] | driver_completed("legacy"))
    and (.events[8] | history_checked("legacy"; "terminal"))
    and (.events[9] | worker_stopped("legacy"; 2; "patched"))
    and (.events[10] | worker_removed("legacy"; 2))
    and (.events[11] | driver_accepted("new"; $new_workflow; $new_run))
    and (.events[12] | history_checked("new"; "initial"))
    and (.events[13] | worker_stopped("new"; 1; "patched"))
    and (.events[14] | worker_removed("new"; 1))
    and (.events[15] | worker_ready("new"; "patched"))
    and (.events[16] | replay_observed("new"; "new"; 1))
    and (.events[17] | driver_completed("new"))
    and (.events[18] | history_checked("new"; "terminal"))
    and (.events[19] | worker_stopped("new"; 2; "patched"))
    and (.events[20] | worker_removed("new"; 2))
    and (.events[21] | volume_removed)
    # Each replaced worker must be gone before a fresh generation-two
    # container is trusted, and generation two must be stopped and removed
    # after the exact terminal-history check.
    and (.events[3].container_id == .events[4].container_id)
    and (.events[3].container_id != .events[5].container_id)
    and (.events[5].container_id == .events[9].container_id)
    and (.events[9].container_id == .events[10].container_id)
    and (.events[13].container_id == .events[14].container_id)
    and (.events[13].container_id != .events[15].container_id)
    and (.events[15].container_id == .events[19].container_id)
    and (.events[19].container_id == .events[20].container_id)
    and ([.events[3].container_id, .events[5].container_id,
          .events[13].container_id, .events[15].container_id]
         | unique | length == 4)
  ' "$controller" >/dev/null; then
  fail "controller document does not match the strict two-scenario lifecycle contract"
fi

printf 'patch_replay_controller legacy_workflow_id=%s legacy_run_id=%s new_workflow_id=%s new_run_id=%s steps=22 volume_removed=true\n' \
  "$legacy_workflow_id" "$legacy_run_id" "$new_workflow_id" "$new_run_id"
