(** Pre-patch executable for the live workflow-patch lifecycle acceptance.

    Its only workflow implementation comes from [Legacy_definition], a module
    compiled and linked separately from every patch-aware definition. A later
    controller replacement therefore tests a genuine history created before
    the patch gate rather than a runtime flag hidden in one binary. *)

(** Converts the typed worker result into a conventional process status while
    preserving the SDK error category and avoiding payload logging. *)
let () =
  match Legacy_definition.run () with
  | Ok () -> Printf.printf "legacy patch replay worker stopped cleanly\n%!"
  | Error error ->
      Printf.eprintf "legacy patch replay worker failed (%s): %s\n%!"
        (Temporal.Error.kind error) (Temporal.Error.message error);
      exit 1
