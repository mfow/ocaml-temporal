(** Produces the defect used when a child workflow operation is attempted
    outside deterministic workflow execution. *)
let outside_error () =
  Error.defect ~message:"child workflow operation used outside a workflow"

(** Creates an already-completed future without emitting a command. Inside a
    workflow the current scheduler owns it; outside a workflow an inert store
    retains the diagnostic for [peek] and [await]. *)
let resolved result =
  match Temporal_runtime.Workflow_context_store.current () with
  | Some context ->
      Future_private.of_internal
        (Temporal_runtime.Workflow_context_store.resolved context
           (Result.map_error Error_private.to_base result))
  | None -> Future_private.resolved ~outside_error result

(** Maximum UTF-8 byte length accepted by the strict bridge JSON protocol. The
    server's configurable identifier policy can be narrower, but no command may
    exceed this compiled cross-language safety boundary. *)
let max_id_utf_8_bytes = 65_536

(** Validates every property required before an ID can enter command history.
    OCaml strings are byte sequences, so both an explicit UTF-8 check and a byte
    limit are necessary before JSON encoding. *)
let validate_id id =
  if String.equal id "" then
    Error (Error.defect ~message:"child workflow id must not be empty")
  else if String.contains id '\000' then
    Error (Error.defect ~message:"child workflow id must not contain NUL")
  else if String.length id > max_id_utf_8_bytes then
    Error
      (Error.defect
         ~message:"child workflow id exceeds 65536 UTF-8 bytes")
  else if not (Temporal_base.Codec.valid_utf_8 id) then
    Error (Error.defect ~message:"child workflow id must be valid UTF-8")
  else Ok ()

(** Validates durable identity and encodes input before allocating a private
    sequence number. Consequently invalid requests cannot change command order
    or appear in replay history. *)
let start ~id definition input =
  match validate_id id with
  | Error error -> resolved (Error error)
  | Ok () -> (
      match Codec_private.encode_base (Workflow.input definition) input with
      | Error error -> resolved (Error (Error_private.of_base error))
      | Ok input -> (
          match Temporal_runtime.Workflow_context_store.current () with
          | None -> resolved (Error (outside_error ()))
          | Some context ->
              Temporal_runtime.Workflow_context_store.start_child_workflow context
                ~id ~name:(Workflow.name definition) ~input
                ~decode:(Codec_private.decode_base (Workflow.output definition))
              |> Future_private.of_internal))

(** Implements the direct-style child call as start followed by an effect-backed
    wait. Expected child and codec failures remain explicit [result] values. *)
let execute ~id definition input = Future.await (start ~id definition input)
