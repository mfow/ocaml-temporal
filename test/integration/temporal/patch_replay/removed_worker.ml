(** Final worker generation after a patch marker has been deprecated.

    This executable intentionally contains neither [Temporal.Workflow.patched]
    nor [Temporal.Workflow.deprecate_patch]. Its workflow runs the migrated
    behavior unconditionally, proving that a deprecated marker can be replayed
    after the patch API call is removed from source. *)

(** Short aliases keep the fixture focused on the public SDK boundary. *)
module Support = Patch_replay_support
module Worker = Temporal.Worker
module Workflow = Temporal.Workflow

(** Defines the post-removal workflow. The timer and activity command order
    stay identical to the deprecated generation, but no patch command is
    emitted while replaying its retained history marker. *)
let removed_workflow =
  Workflow.define ~name:Support.workflow_type ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.string (fun () ->
      let open Temporal.Result_syntax in
      let* () = Workflow.sleep Support.replacement_timer in
      Support.run_patched_activity ())

(** Registers the final workflow and its sole reachable activity. *)
let run () =
  Support.run_worker ~workflows:[ Worker.workflow removed_workflow ]
    ~activities:[ Worker.activity Support.patched_activity ]

(** Converts the typed worker result into the fixture process status. *)
let () =
  match run () with
  | Ok () -> Printf.printf "removed patch worker stopped cleanly\n%!"
  | Error error ->
      Printf.eprintf "removed patch worker failed (%s): %s\n%!"
        (Temporal.Error.kind error) (Temporal.Error.message error);
      exit 1
