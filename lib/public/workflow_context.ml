(** Reports whether this OCaml Domain is currently running workflow code. This
    is a diagnostic API. If workflow logic branches on the result, replay must
    take the same branch. *)
let is_active () =
  Option.is_some (Temporal_runtime.Workflow_context_store.current ())
