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
let start definition input =
  match Codec.encode (Temporal_base.Definition.input definition) input with
  | Error error -> resolved (Error error)
  | Ok input -> (
      match Temporal_runtime.Workflow_context_store.current () with
      | None ->
          Temporal_runtime.Workflow_context_store.detached_error
            ~message:"activity operation used outside a workflow"
      | Some context ->
          Temporal_runtime.Workflow_context_store.schedule_activity context
            ~name:(name definition) ~input
            ~decode:(Codec.decode (Temporal_base.Definition.output definition)))

(** Direct-style convenience for the common schedule-then-await case. *)
let execute definition input = Future.await (start definition input)
