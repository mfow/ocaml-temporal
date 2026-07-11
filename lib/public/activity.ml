(** Public activity definitions reuse the private representation so workflow
    and worker code cannot disagree about names or codecs. *)
type ('input, 'output) implementation =
  'input -> ('output, Error.t) result

type ('input, 'output) t =
  ( 'input,
    'output,
    ('input, 'output) implementation )
  Temporal_base.Definition.t

(** Constructs a worker-owned activity with executable implementation code. *)
let define ~name ~input ~output implementation =
  Temporal_base.Definition.make ~name ~input ~output
    ~implementation:(Some implementation)

(** Constructs a command-only activity target implemented by another worker. *)
let remote ~name ~input ~output =
  Temporal_base.Definition.make ~name ~input ~output ~implementation:None

let name = Temporal_base.Definition.name

(** The cancellation policy attached to a scheduled activity. The constructors
    are part of the public OCaml API rather than exposing protocol JSON. *)
type cancellation_type =
  | Try_cancel
  | Wait_cancellation_completed
  | Abandon

(** Converts the public cancellation policy at the one boundary where a
    workflow command is created. *)
let runtime_cancellation_type = function
  | Try_cancel -> Temporal_runtime.Activation.Try_cancel
  | Wait_cancellation_completed ->
      Temporal_runtime.Activation.Wait_cancellation_completed
  | Abandon -> Temporal_runtime.Activation.Abandon

(** Validates optional activity identities before allocating a workflow
    sequence. Invalid options therefore produce a typed future error and cannot
    alter deterministic history. *)
let validate_optional_identifier field = function
  | None -> Ok ()
  | Some value when String.equal value "" ->
      Error (Error.defect ~message:(field ^ " must not be empty"))
  | Some value when String.contains value '\000' ->
      Error (Error.defect ~message:(field ^ " must not contain NUL"))
  | Some value when String.length value > 65_536 ->
      Error (Error.defect ~message:(field ^ " exceeds 65536 bytes"))
  | Some value when not (Temporal_base.Codec.valid_utf_8 value) ->
      Error (Error.defect ~message:(field ^ " must be valid UTF-8"))
  | Some _ -> Ok ()

(** Produces the defect used when an activity operation escapes the deterministic
    workflow runtime. *)
let outside_error () =
  Error.defect ~message:"activity operation used outside a workflow"

(** Creates an already-resolved future in either the active execution or a
    detached store. Detached futures preserve the error for diagnostics but
    cannot be awaited as valid workflow work. *)
let resolved result =
  match Temporal_runtime.Workflow_context_store.current () with
  | Some context -> Temporal_runtime.Workflow_context_store.resolved context result
  | None -> Temporal_runtime.Future_store.resolved ~outside_error result

(** Encodes before allocating a command ID. This ordering guarantees malformed
    input never appears in workflow history and still returns through a future. *)
let start ?activity_id ?task_queue ?schedule_to_close_timeout
    ?schedule_to_start_timeout ?start_to_close_timeout ?heartbeat_timeout
    ?(cancellation_type = Try_cancel) ?(do_not_eagerly_execute = false)
    definition input =
  match Codec.encode (Temporal_base.Definition.input definition) input with
  | Error error -> resolved (Error error)
  | Ok input ->
      (match validate_optional_identifier "activity id" activity_id with
      | Error error -> resolved (Error error)
      | Ok () ->
          (match validate_optional_identifier "task queue" task_queue with
          | Error error -> resolved (Error error)
          | Ok () ->
              match Temporal_runtime.Workflow_context_store.current () with
              | None ->
                  Temporal_runtime.Workflow_context_store.detached_error
                    ~message:"activity operation used outside a workflow"
              | Some context ->
                  let schedule_to_close_timeout =
                    Option.map Duration.to_ms schedule_to_close_timeout
                  in
                  let schedule_to_start_timeout =
                    Option.map Duration.to_ms schedule_to_start_timeout
                  in
                  let start_to_close_timeout =
                    Option.map Duration.to_ms start_to_close_timeout
                  in
                  let heartbeat_timeout =
                    Option.map Duration.to_ms heartbeat_timeout
                  in
                  Temporal_runtime.Workflow_context_store.schedule_activity
                    context ~name:(name definition) ~input ?activity_id
                    ?task_queue ?schedule_to_close_timeout
                    ?schedule_to_start_timeout ?start_to_close_timeout
                    ?heartbeat_timeout
                    ~cancellation_type:(runtime_cancellation_type cancellation_type)
                    ~do_not_eagerly_execute
                    ~decode:(Codec.decode (Temporal_base.Definition.output definition))
                    ()))

(** Direct-style convenience for the common schedule-then-await case. *)
let execute ?activity_id ?task_queue ?schedule_to_close_timeout
    ?schedule_to_start_timeout ?start_to_close_timeout ?heartbeat_timeout
    ?cancellation_type ?do_not_eagerly_execute definition input =
  Future.await
    (start ?activity_id ?task_queue ?schedule_to_close_timeout
       ?schedule_to_start_timeout ?start_to_close_timeout ?heartbeat_timeout
       ?cancellation_type ?do_not_eagerly_execute definition input)
