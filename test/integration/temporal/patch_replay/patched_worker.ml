(** Active-patch executable for the live workflow-patch lifecycle acceptance.

    This binary registers the same Temporal workflow type as [legacy_worker],
    but obtains its implementation from [Patched_definition], the separate
    compilation unit that calls [Temporal.Workflow.patched]. It preserves the
    legacy branch for marker-free histories while adding the new durable branch
    for patch-bearing executions. It runs generation two of the legacy scenario
    and generation one of the active-to-deprecated scenario. *)

(** Converts the typed replacement-worker result into a conventional process
    status without leaking workflow or activity payloads into the log. *)
let () =
  match Patched_definition.run () with
  | Ok () -> Printf.printf "patched replay worker stopped cleanly\n%!"
  | Error error ->
      Printf.eprintf "patched replay worker failed (%s): %s\n%!"
        (Temporal.Error.kind error) (Temporal.Error.message error);
      exit 1
