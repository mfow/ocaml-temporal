(** The genuinely pre-patch workflow definition for the first acceptance
    worker binary.

    Keeping this module out of [temporal_patch_replay_support] means the legacy
    executable does not link the compilation unit that defines the replacement
    patch gate. That binary separation is part of the acceptance evidence, not
    merely an organisational preference. *)

(** Local aliases make the public-SDK-only boundary explicit without exposing
    any implementation or native worker details in this fixture module. *)
module Support = Patch_replay_support
module Worker = Temporal.Worker
module Workflow = Temporal.Workflow

(** Defines the source version that existed before the behavior change. The
    durable timer is deliberately the first command, so the controller can
    stop this worker after [TimerStarted] and leave a marker-free history for
    the replacement binary to replay. *)
let legacy_workflow =
  Workflow.define ~name:Support.workflow_type ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.string (fun () ->
      let open Temporal.Result_syntax in
      let* () = Workflow.sleep Support.replacement_timer in
      Support.run_legacy_activity ())

(** Registers only the legacy workflow and activity with the shared worker
    lifecycle. The absence of the patched activity and patch-gate definition
    makes it impossible for generation one to produce new-branch history. *)
let run () =
  Support.run_worker ~workflows:[ Worker.workflow legacy_workflow ]
    ~activities:[ Worker.activity Support.legacy_activity ]
