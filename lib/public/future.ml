(** Re-exports the workflow runtime's future operations. All scheduling remains
    inside the workflow execution that created the future. *)
type ('value, 'error) t = ('value, 'error) Temporal_runtime.Future_store.t

type ('left, 'right) race =
  ('left, 'right) Temporal_runtime.Future_store.race =
  | Left of 'left
  | Right of 'right

(** Builds the structured API defect shared by every aggregate ownership
    check. The message is stable enough for diagnostics without exposing
    scheduler identities. *)
let ownership_error () =
  Error.defect
    ~message:
      "Temporal future combinator received futures from different workflow \
       executions"

let await = Temporal_runtime.Future_store.await
let map = Temporal_runtime.Future_store.map
let map_error = Temporal_runtime.Future_store.map_error
let both left right =
  Temporal_runtime.Future_store.both ~ownership_error left right

(** Preserves the active workflow's ownership for the empty identity aggregate.
    Without an input from which to infer an owner, the low-level store uses an
    inert owner; selecting the current context here lets [all []] compose with
    ordinary workflow futures while remaining ready and command-free. *)
let all futures =
  match (futures, Temporal_runtime.Workflow_context_store.current ()) with
  | [], Some context ->
      Temporal_runtime.Workflow_context_store.resolved context (Ok [])
  | _ -> Temporal_runtime.Future_store.all ~ownership_error futures

let race left right =
  Temporal_runtime.Future_store.race ~ownership_error left right

let first leading rest =
  Temporal_runtime.Future_store.first ~ownership_error leading rest
let is_ready = Temporal_runtime.Future_store.is_ready
let peek = Temporal_runtime.Future_store.peek
