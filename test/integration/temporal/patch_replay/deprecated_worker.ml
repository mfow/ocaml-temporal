(** Replacement worker used after an active patch has been deployed safely.

    This executable calls [Temporal.Workflow.deprecate_patch] instead of
    [Temporal.Workflow.patched]. It therefore records the lifecycle transition
    in new histories while making the patched behavior unconditional in source.
    Keeping this definition in its own executable proves that generation two
    no longer contains the original branch decision. *)

(** Short aliases keep the fixture focused on the public SDK boundary. *)
module Support = Patch_replay_support
module Worker = Temporal.Worker
module Workflow = Temporal.Workflow

(** Defines the deprecated-patch source generation. During replay, Core
    consumes the existing active marker without appending or changing history;
    for a new execution it records a marker whose [deprecated] flag is true. *)
let deprecated_workflow =
  Workflow.define ~name:Support.workflow_type ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.string (fun () ->
      Workflow.deprecate_patch ~id:Support.patch_id;
      let open Temporal.Result_syntax in
      let* () = Workflow.sleep Support.replacement_timer in
      Support.run_patched_activity ())

(** Registers only the unconditional patched behavior required by this source
    generation. The historical activity is no longer reachable. *)
let run () =
  Support.run_worker ~workflows:[ Worker.workflow deprecated_workflow ]
    ~activities:[ Worker.activity Support.patched_activity ]

(** Converts the typed worker result into the fixture process status. *)
let () =
  match run () with
  | Ok () -> Printf.printf "deprecated patch worker stopped cleanly\n%!"
  | Error error ->
      Printf.eprintf "deprecated patch worker failed (%s): %s\n%!"
        (Temporal.Error.kind error) (Temporal.Error.message error);
      exit 1
