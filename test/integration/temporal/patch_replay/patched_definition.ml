(** The replacement workflow definition for the second acceptance worker
    binary.

    This is the sole fixture compilation unit that invokes
    [Temporal.Workflow.patched]. The separate module lets Dune link it only
    into [patched_worker.exe], preserving a meaningful pre-patch legacy binary
    for the old-history scenario. *)

(** Local aliases document that this source version uses only the public SDK
    and the shared fixture helpers; it does not reach into runtime internals. *)
module Support = Patch_replay_support
module Worker = Temporal.Worker
module Workflow = Temporal.Workflow

(** Defines the changed source version. The patch decision precedes the timer:
    Core returns [false] when replaying a marker-free legacy history and
    [true] for a new execution or an existing marker-bearing history. Keeping
    the timer in the same position preserves the old command prefix. *)
let patched_workflow =
  Workflow.define ~name:Support.workflow_type ~input:Temporal.Codec.unit
    ~output:Temporal.Codec.string (fun () ->
      let use_patched_branch = Workflow.patched ~id:Support.patch_id in
      let open Temporal.Result_syntax in
      let* () = Workflow.sleep Support.replacement_timer in
      if use_patched_branch then Support.run_patched_activity ()
      else Support.run_legacy_activity ())

(** Registers the replacement workflow plus both activities. A replay of an
    older marker-free history still schedules the legacy activity, whereas a
    new or marker-present replay schedules the patched activity. *)
let run () =
  Support.run_worker ~workflows:[ Worker.workflow patched_workflow ]
    ~activities:
      [ Worker.activity Support.legacy_activity; Worker.activity Support.patched_activity ]
