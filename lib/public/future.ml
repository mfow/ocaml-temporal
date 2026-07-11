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

let all futures = Temporal_runtime.Future_store.all ~ownership_error futures

let race left right =
  Temporal_runtime.Future_store.race ~ownership_error left right

let first leading rest =
  Temporal_runtime.Future_store.first ~ownership_error leading rest
let is_ready = Temporal_runtime.Future_store.is_ready
let peek = Temporal_runtime.Future_store.peek
