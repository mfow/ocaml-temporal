(** Private worker-loop state for the first live OCaml workflow execution
    boundary.

    Rust/Core ownership and semantic JSON validation live below this module in
    the supervisor. This module therefore receives typed activations, resolves
    each run ID to an existentially typed [Execution.t], and sends typed
    completions back through the same supervisor. The registry is mutable only
    behind one mutex; it is never shared with workflow fibers or native code. *)

module Protocol = Temporal_protocol.Workflow_protocol
module Definition = Temporal_base.Definition
module Codec = Temporal_base.Codec
module Base_error = Temporal_base.Error
module Observability = Temporal_base.Observability

(** Result-bind notation keeps all expected boundary failures on typed paths. *)
let ( let* ) = Result.bind

(** The source-side operations that this adapter needs. The signature is kept
    independent of [Sdk_supervisor.Native] so semantic execution can be tested
    with a deterministic queue and can be wired to any future readiness API. *)
module type SUPERVISOR = sig
  type t
  type error

  val try_poll_workflow :
    t -> (Protocol.activation option, error) result

  val complete_workflow :
    t -> Protocol.completion -> (unit, error) result

  val error_code : error -> string
  val error_message : error -> string
end

(** Stable diagnostics deliberately contain no payload bytes or native values. *)
type error_view = { code : string; path : string; message : string }

(** The public-facing existential registration. Its constructor remains
    private so callers can only produce values through [register]. *)
type registered_workflow =
  | Workflow :
      ('input, 'output,
       'input -> ('output, Base_error.t) result)
      Definition.t ->
      registered_workflow

(** One typed execution hidden behind the run-ID map. Both the definition and
    execution share the same input/output type parameters, which prevents a
    completion from being encoded with the wrong codec. *)
type run =
  | Run :
      {
        definition :
          ('input, 'output,
           'input -> ('output, Base_error.t) result)
          Definition.t;
        execution : ('input, 'output) Execution.t;
      }
      -> run

(** At most one run can be leased for each Temporal run ID. *)
module Run_map = Map.Make (String)

(** One worker-loop result. *)
type outcome =
  | Not_ready
  | Completed of {
      run_id : string;
      command_count : int;
      terminal : bool;
    }
  | Rejected of {
      run_id : string option;
      error : error_view;
      lease_retired : bool;
    }

(** Existentially typed definition map values. *)
type registered_definition =
  | Registered_definition :
      ('input, 'output,
       'input -> ('output, Base_error.t) result)
      Definition.t ->
      registered_definition

(** Bounds diagnostics that may contain application codec messages before they
    enter Logs or a Temporal failure. The suffix makes truncation explicit. *)
let bounded_message value =
  let maximum = 1_024 in
  if String.length value <= maximum then value
  else String.sub value 0 (maximum - 3) ^ "..."

(** Bounds arbitrary source classifications before they reach the stable
    adapter error view. *)
let bounded_code value =
  let maximum = 128 in
  if String.length value <= maximum then value
  else String.sub value 0 (maximum - 3) ^ "..."

(** Creates an immutable diagnostic in one place so all branches preserve the
    same privacy and size rules. *)
let make_error ?(path = "$") code message : error_view =
  { code = bounded_code code; path; message = bounded_message message }

(** Converts an unexpected OCaml exception into a bounded diagnostic. The
    adapter catches such exceptions at the lease boundary so a defect in a
    codec or scheduler cannot unwind past [poll] while leaving a native lease
    silently unacknowledged. Exceptions still indicate defects, not ordinary
    workflow failures; this conversion is a last-resort cleanup guard. *)
let exception_error ?(path = "$") exception_ =
  let message =
    try Printexc.to_string exception_ with _ -> "unprintable OCaml exception"
  in
  make_error ~path "ocaml_exception" message

(** Converts a supervisor error without trusting its accessor functions to be
    exception-free. A broken diagnostic accessor must not make lease handling
    itself escape the typed error boundary. *)
let supervisor_error ?(path = "$") ~error_code ~error_message source_error =
  try
    make_error ~path
      (error_code source_error)
      (error_message source_error)
  with exception_ -> exception_error ~path exception_

(** Converts a native execution diagnostic without exposing its representation. *)
let native_error error =
  let view = Native_execution.error_view error in
  make_error ~path:view.path view.code view.message

(** Converts an application error from a workflow codec or implementation into
    a bridge failure description. Details remain in the typed error only until
    this point and are deliberately not copied into the message. *)
let application_error ?(path = "$") error =
  let view = Base_error.view error in
  make_error ~path (Base_error.kind error) view.message

(** Builds a non-retryable Temporal application failure for an adapter-level
    rejection. The failure is submitted through the ordinary completion path,
    which lets Core retire the exact lease instead of abandoning it. *)
let failure_of_error (error : error_view) : Protocol.failure =
  Protocol.
    {
      message = error.message;
      source = "ocaml-temporal";
      stack_trace = "";
      encoded_attributes = None;
      cause = None;
      info =
        Application
          {
            type_name = "ocaml_temporal_native_worker";
            non_retryable = true;
            details = [];
          };
    }

(** Converts one protocol payload into the runtime payload representation. The
    runtime stores metadata as strings, so binary metadata is rejected rather
    than decoded with replacement characters. Both metadata and body bytes are
    copied before a workflow execution can retain them. *)
let runtime_payload path (payload : Protocol.payload) =
  let rec metadata_loop reversed = function
    | [] -> Ok (List.rev reversed)
    | (key, bytes) :: rest ->
        if String.length key = 0 || String.contains key '\000' then
          Error
            (make_error ~path:(path ^ ".metadata") "invalid_message"
               "metadata key must be non-empty and must not contain NUL")
        else
          let value = Bytes.to_string bytes in
          if not (Codec.valid_utf_8 value) then
            Error
              (make_error ~path:(path ^ ".metadata." ^ key) "unsupported"
                 "binary metadata cannot be represented by the runtime")
          else metadata_loop ((key, value) :: reversed) rest
  in
  let* metadata = metadata_loop [] payload.metadata in
  Ok
    {
      Temporal_base.Payload.metadata;
      data = Bytes.copy payload.data;
    }

(** The protocol uses an argument list, while a typed OCaml workflow definition
    accepts one value. Zero arguments are interpreted as the canonical unit
    payload; one argument is decoded normally and more than one is rejected so
    the adapter never silently drops a Core argument. *)
let decode_input definition arguments =
  let payload_result =
    match arguments with
    | [] -> Ok { Temporal_base.Payload.metadata = [ ("encoding", "binary/null") ]; data = Bytes.empty }
    | [ payload ] -> runtime_payload "$.jobs[0].arguments[0]" payload
    | _ ->
        Error
          (make_error ~path:"$.jobs[0].arguments" "unsupported"
             "workflow definitions currently accept exactly one input value")
  in
  let* payload = payload_result in
  match Codec.decode (Definition.input definition) payload with
  | Ok input -> Ok input
  | Error error -> Error (application_error ~path:"$.jobs[0].arguments" error)

(** Returns the one initialization record and rejects duplicate start markers.
    Initialization is expected to be the first job because later jobs may refer
    to the execution it creates; accepting a later marker would make run
    registration order-dependent. *)
let initialization (activation : Protocol.activation) :
    (Native_execution.initialization option, error_view) result =
  let rec collect index found = function
    | [] -> (
        match found with
        | Some (0, init) -> Ok (Some init)
        | Some (_, _) ->
            Error
              (make_error ~path:"$.jobs" "invalid_message"
                 "Initialize_workflow must be the first activation job")
        | None -> Ok None)
    | Protocol.Initialize_workflow
        { workflow_id; workflow_type; arguments; randomness_seed; attempt; context }
      :: rest -> (
        match found with
        | None ->
            let init : Native_execution.initialization =
              {
                workflow_id;
                workflow_type;
                arguments;
                randomness_seed;
                attempt;
                context;
              }
            in
            collect (index + 1) (Some (index, init)) rest
        | Some _ ->
            Error
              (make_error ~path:(Printf.sprintf "$.jobs[%d]" index)
                 "invalid_message"
                 "activation contains more than one Initialize_workflow job"))
    | _ :: rest -> collect (index + 1) found rest
  in
  collect 0 None activation.jobs

(** Checks whether a completion contains a terminal command. The semantic
    protocol encoder has already validated that any terminal command is last. *)
let is_terminal completion =
  List.exists
    (function
      | Protocol.Complete_workflow _
      | Protocol.Fail_workflow _
      | Protocol.Cancel_workflow_execution -> true
      | Protocol.Schedule_activity _
      | Protocol.Start_child_workflow _
      | Protocol.Request_cancel_activity _
      | Protocol.Start_timer _
      | Protocol.Cancel_timer _ -> false)
    completion.Protocol.commands

(** Reports one bounded lifecycle message without allowing a reporter defect to
    affect worker progress. *)
let report level ~operation ?error_kind () =
  try
    let tags = Observability.tags ~operation ?error_kind () in
    Observability.report ~src:Observability.Source.lifecycle level ~tags
      "native workflow worker adapter event"
  with _ -> ()

(** Adds or rejects one definition in the name map. *)
let add_definition definitions (Workflow definition) =
  let name = Definition.name definition in
  if Run_map.mem name definitions then
    Error
      (make_error ~path:"$.workflows" "duplicate_workflow"
         ("workflow type is registered more than once: " ^ name))
  else if Option.is_none (Definition.implementation definition) then
    Error
      (make_error ~path:("$.workflows." ^ name) "not_executable"
         "workflow registration has no local implementation")
  else Ok (Run_map.add name (Registered_definition definition) definitions)

(** Builds the immutable definition registry before publishing any mutable
    worker state. *)
let build_definitions workflows =
  List.fold_left
    (fun result workflow ->
      let* definitions = result in
      add_definition definitions workflow)
    (Ok Run_map.empty) workflows

(** Validates the worker's implicit activity queue before [create] publishes a
    definition registry. The same predicate is used by
    [Workflow_context_store.create], so an invalid queue cannot survive worker
    construction and later become an activation-time [Invalid_argument]. *)
let validate_task_queue task_queue =
  match Workflow_context_store.validate_task_queue task_queue with
  | Ok () -> Ok ()
  | Error message ->
      Error
        (make_error ~path:"$.task_queue" "invalid_configuration" message)

(** Finds one registered workflow by its Temporal type name. *)
let find_definition definitions workflow_type =
  match Run_map.find_opt workflow_type definitions with
  | Some definition -> Ok definition
  | None ->
      Error
        (make_error ~path:"$.jobs[0].workflow_type" "unknown_workflow_type"
           ("no executable workflow is registered for type " ^ workflow_type))

(** The functor implementation uses a concrete record containing the source
    module and source value. The public [t] stores that record behind the
    functor's abstract type, preserving both the source's abstract type and the
    invariant that all calls pass through the adapter mutex. *)
module Make (Supervisor : SUPERVISOR) = struct
  type adapter_state = {
    supervisor : Supervisor.t;
    task_queue : string;
    definitions : registered_definition Run_map.t;
    mutable runs : run Run_map.t;
    mutex : Mutex.t;
  }

  type t = adapter_state

  (** Creates the immutable definition registry and an empty run registry.
      Queue validation happens before definitions are published, so malformed
      empty, NUL-containing, oversized, or non-UTF-8 defaults fail as a typed
      configuration result rather than breaking the first workflow activation.
      No supervisor operation or workflow implementation runs on this path. *)
  let create ?(task_queue = "default") ~supervisor ~workflows () =
    let* () = validate_task_queue task_queue in
    let* definitions = build_definitions workflows in
    Ok
      {
        supervisor;
        task_queue;
        definitions;
        runs = Run_map.empty;
        mutex = Mutex.create ();
      }

  (** Distinguishes an ordinary source rejection from an exception raised while
      completing. The latter is allowed to reach [process_one]'s cleanup guard:
      the adapter can then make one explicit failure-completion attempt instead
      of silently abandoning the leased activation. *)
  type completion_attempt =
    | Accepted
    | Rejected_by_supervisor of error_view
    | Raised_by_supervisor of exn

  (** Calls the supervisor completion operation without losing whether an
      exception occurred. A returned source error still means that the
      supervisor completed the call normally but did not acknowledge it. *)
  let attempt_completion supervisor completion =
    try
      match Supervisor.complete_workflow supervisor completion with
      | Ok () -> Accepted
      | Error source_error ->
          let source =
            supervisor_error ~path:"$.completion"
              ~error_code:Supervisor.error_code
              ~error_message:Supervisor.error_message source_error
          in
          Rejected_by_supervisor
            (make_error ~path:"$.completion" "completion_failed"
               (Printf.sprintf "supervisor rejected completion (%s): %s"
                  source.code source.message))
    with exception_ -> Raised_by_supervisor exception_

  (** Converts a completion exception to the stable typed error used when a
      failure-completion attempt itself cannot be acknowledged. *)
  let completion_exception_error exception_ =
    make_error ~path:"$.completion" "completion_failed"
      (Printf.sprintf "supervisor completion raised: %s"
         (exception_error exception_).message)

  (** Encodes and submits an adapter-level failure. A successful submission is
      the lease-retirement proof for the activation; a failed submission
      preserves a source error rather than claiming the lease was retired.
      [remove_run] is used only when an execution was inserted before a local
      activation defect was discovered. *)
  let retire_with_failure ?(remove_run = false) adapter
      (activation : Protocol.activation) error =
    let completion : Protocol.completion =
      {
        run_id = activation.Protocol.run_id;
        commands =
          [ Protocol.Fail_workflow { failure = failure_of_error error } ];
      }
    in
    match attempt_completion adapter.supervisor completion with
    | Accepted ->
        if remove_run then
          adapter.runs <- Run_map.remove activation.run_id adapter.runs;
        report Logs.Warning ~operation:"workflow_activation_rejected"
          ~error_kind:error.code ();
        Ok
          (Rejected
             {
               run_id = Some activation.run_id;
               error;
               lease_retired = true;
             })
    | Rejected_by_supervisor error -> Error error
    | Raised_by_supervisor exception_ ->
        Error (completion_exception_error exception_)

  (** Produces a typed completion for a successfully executed activation and
      updates the registry only after the supervisor confirms retirement. *)
  let submit_completion adapter activation completion ~run_id =
    match attempt_completion adapter.supervisor completion with
    | Rejected_by_supervisor error -> Error error
    | Raised_by_supervisor exception_ -> raise exception_
    | Accepted ->
        (* The native lease is already retired at this point. Keep the
           post-acknowledgement bookkeeping inside a typed guard so an
           unexpected local exception cannot trigger a second completion. *)
        begin
          try
            let terminal = is_terminal completion in
            let evicted =
              List.exists
                (function Protocol.Remove_from_cache _ -> true | _ -> false)
                activation.Protocol.jobs
            in
            if terminal || evicted then
              adapter.runs <- Run_map.remove run_id adapter.runs;
            report Logs.Debug ~operation:"workflow_activation_completed" ();
            Ok
              (Completed
                 {
                   run_id;
                   command_count = List.length completion.commands;
                   terminal;
                 })
          with exception_ ->
            Error (exception_error ~path:"$.completion.bookkeeping" exception_)
        end

  (** Applies one activation while the adapter mutex is held. No source call or
      map mutation is performed after an error that has not been acknowledged
      by a successful completion. *)
  let process_one_unsafe adapter activation : (outcome, error_view) result =
    match Native_execution.translate_activation activation with
    | Error error ->
        retire_with_failure adapter activation (native_error error)
    | Ok _translated -> (
        match initialization activation with
        | Error error -> retire_with_failure adapter activation error
        | Ok (Some init) ->
            if Run_map.mem activation.run_id adapter.runs then
              retire_with_failure adapter activation
                (make_error ~path:"$.run_id" "duplicate_run_id"
                   "workflow run is already present in the execution registry")
            else (
              match find_definition adapter.definitions init.workflow_type with
              | Error error ->
                  retire_with_failure adapter activation error
              | Ok (Registered_definition definition) -> (
                  match decode_input definition init.arguments with
                  | Error error ->
                      retire_with_failure adapter activation error
                  | Ok input ->
                      let execution =
                        Execution.start ~task_queue:adapter.task_queue definition
                          input
                      in
                      let run = Run { definition; execution } in
                      adapter.runs <- Run_map.add activation.run_id run adapter.runs;
                      match Native_execution.activate execution activation with
                      | Error error ->
                          retire_with_failure ~remove_run:true adapter activation
                            (native_error error)
                      | Ok completion ->
                          submit_completion adapter activation completion
                            ~run_id:activation.run_id))
        | Ok None -> (
            match Run_map.find_opt activation.run_id adapter.runs with
            | None ->
                retire_with_failure adapter activation
                  (make_error ~path:"$.run_id" "unknown_run_id"
                     "activation does not identify a registered running workflow")
            | Some (Run { execution; _ }) -> (
                match Native_execution.activate execution activation with
                | Error error ->
                    retire_with_failure ~remove_run:true adapter activation
                      (native_error error)
                | Ok completion ->
                    submit_completion adapter activation completion
                      ~run_id:activation.run_id)))

  (** Applies one activation with a final cleanup guard. All expected
      rejections already use [retire_with_failure]; this catch handles a
      programmer defect or unexpected codec exception before completion. It
      attempts exactly one failure completion and reports [Error] if that
      acknowledgement cannot be proven, rather than claiming retirement. *)
  let process_one adapter activation : (outcome, error_view) result =
    try process_one_unsafe adapter activation with exception_ ->
      let error = exception_error ~path:"$.workflow_execution" exception_ in
      retire_with_failure
        ~remove_run:(Run_map.mem activation.run_id adapter.runs)
        adapter activation error

  (** Serializes one poll/execute/complete transaction. A mutex is required in
      addition to supervisor serialization because the run map and scheduler
      state are OCaml values owned by this adapter, not by Rust. *)
  let poll adapter =
    Mutex.lock adapter.mutex;
    Fun.protect
      ~finally:(fun () -> Mutex.unlock adapter.mutex)
      (fun () ->
        let polled =
          try Ok (Supervisor.try_poll_workflow adapter.supervisor)
          with exception_ -> Error (exception_error ~path:"$.poll" exception_)
        in
        let* polled = polled in
        match polled with
        | Error source_error ->
            Error
              (supervisor_error ~path:"$.poll"
                 ~error_code:Supervisor.error_code
                 ~error_message:Supervisor.error_message source_error)
        | Ok None ->
            report Logs.Debug ~operation:"workflow_poll_not_ready" ();
            Ok Not_ready
        | Ok (Some activation) ->
            process_one adapter activation)
end

(** Exposes registration without exposing its existential constructor. *)
let register definition = Workflow definition
