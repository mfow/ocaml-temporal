(** Implements the public worker over the private semantic backend. *)

(** Heterogeneous workflow registration package. *)
type registered_workflow =
  | Workflow : ('input, 'output) Workflow.t -> registered_workflow

(** Heterogeneous activity registration package. *)
type registered_activity =
  | Activity : ('input, 'output) Activity.t -> registered_activity

(** Packs a workflow definition for the heterogeneous registration list. *)
let workflow definition = Workflow definition

(** Packs an activity definition for the heterogeneous registration list. *)
let activity definition = Activity definition

(** A local workflow entry keeps its definition and implementation together so
    decoding and execution cannot accidentally use different codecs. *)
type workflow_entry =
  | Workflow_entry : {
      definition : ('input, 'output) Workflow.t;
      implementation : ('input, 'output) Workflow.implementation;
    }
      -> workflow_entry

(** A local activity entry has the same invariant as a workflow entry. *)
type activity_entry =
  | Activity_entry : {
      definition : ('input, 'output) Activity.t;
      implementation : ('input, 'output) Activity.implementation;
    }
      -> activity_entry

(** String keys give stable registration and lookup order without relying on
    hash-table iteration, which matters when the backend config is serialized. *)
module Name_map = Map.Make (String)

(** A worker owns one backend and immutable registries after construction. *)
type t = {
  backend : Backend.worker;
  workflows : workflow_entry Name_map.t;
  activities : activity_entry Name_map.t;
  mutable closed : bool;
}

(** Stable default identity for worker diagnostics. *)
let default_identity = "ocaml-temporal-worker"

(** Rejects empty or NUL-containing worker settings before backend allocation. *)
let validate_name field value =
  if String.equal value "" then
    Error (Error.defect ~message:(field ^ " must not be empty"))
  else if String.contains value '\000' then
    Error (Error.defect ~message:(field ^ " must not contain NUL"))
  else Ok ()

(** Adds a workflow to the registry while rejecting duplicate names and remote
    references that do not contain executable OCaml code. *)
let add_workflow registry (Workflow definition) =
  let name = Workflow.name definition in
  match Temporal_base.Definition.implementation definition with
  | None ->
      Error
        (Error.defect
           ~message:("workflow " ^ name ^ " has no local implementation"))
  | Some implementation ->
      if Name_map.mem name registry then
        Error
          (Error.defect ~message:("duplicate workflow registration: " ^ name))
      else
        Ok
          (Name_map.add name
             (Workflow_entry { definition; implementation })
             registry)

(** Adds an activity to the registry with the same duplicate and implementation
    checks used for workflows. *)
let add_activity registry (Activity definition) =
  let name = Activity.name definition in
  match Temporal_base.Definition.implementation definition with
  | None ->
      Error
        (Error.defect
           ~message:("activity " ^ name ^ " has no local implementation"))
  | Some implementation ->
      if Name_map.mem name registry then
        Error
          (Error.defect ~message:("duplicate activity registration: " ^ name))
      else
        Ok
          (Name_map.add name
             (Activity_entry { definition; implementation })
             registry)

(** Builds a workflow registry before opening any backend resource. *)
let collect_workflows definitions =
  List.fold_left
    (fun result definition ->
      Result.bind result (fun registry -> add_workflow registry definition))
    (Ok Name_map.empty) definitions

(** Builds an activity registry before opening any backend resource. *)
let collect_activities definitions =
  List.fold_left
    (fun result definition ->
      Result.bind result (fun registry -> add_activity registry definition))
    (Ok Name_map.empty) definitions

(** Creates the private backend only after all local registration invariants are
    proven. This ordering prevents leaked graphs on invalid definitions. *)
let create ?(identity = default_identity) ~target_url ~namespace ~task_queue
    ~workflows ~activities () =
  match validate_name "namespace" namespace with
  | Error error -> Error error
  | Ok () -> (
      match validate_name "task queue" task_queue with
      | Error error -> Error error
      | Ok () -> (
          match validate_name "identity" identity with
          | Error error -> Error error
          | Ok () -> (
              match collect_workflows workflows with
              | Error error -> Error error
              | Ok workflows -> (
                  match collect_activities activities with
                  | Error error -> Error error
                  | Ok activities ->
                      let config : Backend.config =
                        { target_url; namespace; identity; task_queue = Some task_queue }
                      in
                      let workflow_names =
                        Name_map.bindings workflows |> List.map fst
                      in
                      let activity_names =
                        Name_map.bindings activities |> List.map fst
                      in
                      Result.map
                        (fun backend ->
                          { backend; workflows; activities; closed = false })
                        (Backend.worker_create config ~workflow_names
                           ~activity_names)))))

(** Converts an implementation exception into a structured defect rather than
    letting a user callback tear down the worker poll loop. *)
let protect_implementation operation implementation input =
  match implementation input with
  | result -> result
  | exception exn ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "%s implementation raised: %s" operation
                (Printexc.to_string exn)))

(** Dispatches one workflow task through its typed codec and implementation. *)
let dispatch_workflow worker task =
  match Name_map.find_opt task.Backend.workflow_name worker.workflows with
  | None ->
      Error
        (Temporal_base.Error.make ~category:`Workflow
           ~message:("unregistered workflow task: " ^ task.workflow_name) ())
  | Some (Workflow_entry { definition; implementation }) -> (
      match
        Codec.decode (Temporal_base.Definition.input definition) task.input
      with
      | Error error -> Error error
      | Ok input -> (
          match protect_implementation "workflow" implementation input with
          | Error error -> Error error
          | Ok output -> Codec.encode (Temporal_base.Definition.output definition) output))

(** Dispatches one activity task through its typed codec and implementation. *)
let dispatch_activity worker task =
  match Name_map.find_opt task.Backend.activity_name worker.activities with
  | None ->
      Error
        (Temporal_base.Error.make ~category:`Activity
           ~message:("unregistered activity task: " ^ task.activity_name) ())
  | Some (Activity_entry { definition; implementation }) -> (
      match
        Codec.decode (Temporal_base.Definition.input definition) task.input
      with
      | Error error -> Error error
      | Ok input -> (
          match protect_implementation "activity" implementation input with
          | Error error -> Error error
          | Ok output -> Codec.encode (Temporal_base.Definition.output definition) output))

(** Completes a workflow task even when dispatch produced a typed failure. The
    backend receives the failure so Core cannot be left waiting for a response. *)
let complete_workflow worker task =
  match dispatch_workflow worker task with
  | Ok output ->
      Backend.worker_complete_workflow worker.backend
        (Backend.Workflow_completed { task_token = task.task_token; output })
  | Error error ->
      Result.bind
        (Backend.worker_complete_workflow worker.backend
           (Backend.Workflow_failed
              { task_token = task.task_token; error }))
        (fun () -> Error error)

(** Completes an activity task even when local decoding or execution failed. *)
let complete_activity worker task =
  match dispatch_activity worker task with
  | Ok output ->
      Backend.worker_complete_activity worker.backend
        (Backend.Activity_completed { task_token = task.task_token; output })
  | Error error ->
      Result.bind
        (Backend.worker_complete_activity worker.backend
           (Backend.Activity_failed
              { task_token = task.task_token; error }))
        (fun () -> Error error)

(** Polls both streams until each reports shutdown. Core-backed adapters may
    block inside their poll calls; this loop never waits on an OCaml lock. *)
let run worker =
  if worker.closed then
    Error
      (Temporal_base.Error.make ~category:`Bridge ~message:"worker is shut down" ())
  else
    let rec loop workflow_shutdown activity_shutdown =
      if workflow_shutdown && activity_shutdown then Ok ()
      else
        let workflow_result =
          if workflow_shutdown then Ok Backend.Shutdown
          else Backend.worker_poll_workflow worker.backend
        in
        Result.bind workflow_result (function
          | Backend.Task task ->
              Result.bind (complete_workflow worker task) (fun () ->
                  loop false activity_shutdown)
          | Backend.Idle ->
              let activity_result =
                if activity_shutdown then Ok Backend.Shutdown
                else Backend.worker_poll_activity worker.backend
              in
              Result.bind activity_result (function
                | Backend.Task task ->
                    Result.bind (complete_activity worker task) (fun () ->
                        loop workflow_shutdown false)
                | Backend.Idle -> loop workflow_shutdown activity_shutdown
                | Backend.Shutdown -> loop workflow_shutdown true)
          | Backend.Shutdown ->
              let activity_result =
                if activity_shutdown then Ok Backend.Shutdown
                else Backend.worker_poll_activity worker.backend
              in
              Result.bind activity_result (function
                | Backend.Task task ->
                    Result.bind (complete_activity worker task) (fun () ->
                        loop true false)
                | Backend.Idle -> loop true activity_shutdown
                | Backend.Shutdown -> loop true true))
    in
    loop false false

(** Shuts down the backend once and remembers that no new poll may be admitted. *)
let shutdown worker =
  if worker.closed then Ok ()
  else
    match Backend.worker_shutdown worker.backend with
    | Ok () as result ->
        worker.closed <- true;
        result
    | Error _ as error -> error
