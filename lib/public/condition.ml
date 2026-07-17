(** Provides the public, direct-style condition wait over the package-private
    scheduler condition store.  The store owns every continuation and is
    rechecked by the activation loop; this module exposes only typed results
    and ordinary OCaml predicates. *)

(** A result-aware condition predicate used by the public convenience API. *)
type predicate = unit -> (bool, Error.t) result

(** Converts an application predicate exception into a structured defect rather
    than allowing it to escape through the scheduler's activation boundary. *)
let protect_predicate predicate () =
  try predicate () with
  | exn ->
      Error
        (Error.defect
           ~message:(
             "Temporal.Condition predicate raised: " ^ Printexc.to_string exn))

(** Evaluates and waits through the current workflow context.  A call outside
    workflow execution returns a typed defect and retains no callback. *)
let wait_until_result predicate =
  match Temporal_sdk_kernel.Workflow_context_store.current () with
  | None ->
      Error
        (Error.defect
           ~message:
             "Temporal.Condition.wait_until used outside a workflow execution")
  | Some context ->
      let predicate () =
        match protect_predicate predicate () with
        | Ok value -> Ok value
        | Error error -> Error (Error_private.to_base error)
      in
      Temporal_sdk_kernel.Workflow_context_store.wait_until context ~predicate
      |> Result.map_error Error_private.of_base

(** Waits for a boolean predicate by adapting it to the typed predicate form. *)
let wait_until predicate = wait_until_result (fun () -> Ok (predicate ()))
