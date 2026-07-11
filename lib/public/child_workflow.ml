(** Produces the defect used when a child workflow operation is attempted
    outside deterministic workflow execution. *)
let outside_error () =
  Error.defect ~message:"child workflow operation used outside a workflow"

(** Creates an already-completed future without emitting a command. Inside a
    workflow the current scheduler owns it; outside a workflow an inert store
    retains the diagnostic for [peek] and [await]. *)
let resolved result =
  match Temporal_runtime.Workflow_context_store.current () with
  | Some context -> Temporal_runtime.Workflow_context_store.resolved context result
  | None -> Temporal_runtime.Future_store.resolved ~outside_error result

(** Validates durable identity and encodes input before allocating a private
    sequence number. Consequently invalid requests cannot change command order
    or appear in replay history. *)
let start ~id definition input =
  if String.equal id "" then
    resolved
      (Error (Error.defect ~message:"child workflow id must not be empty"))
  else
    match Codec.encode (Temporal_base.Definition.input definition) input with
    | Error error -> resolved (Error error)
    | Ok input -> (
        match Temporal_runtime.Workflow_context_store.current () with
        | None ->
            Temporal_runtime.Workflow_context_store.detached_error
              ~message:"child workflow operation used outside a workflow"
        | Some context ->
            Temporal_runtime.Workflow_context_store.start_child_workflow context
              ~id ~name:(Workflow.name definition) ~input
              ~decode:(Codec.decode (Temporal_base.Definition.output definition)))

(** Implements the direct-style child call as start followed by an effect-backed
    wait. Expected child and codec failures remain explicit [result] values. *)
let execute ~id definition input = Future.await (start ~id definition input)
