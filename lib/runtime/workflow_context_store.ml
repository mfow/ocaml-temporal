(** Function saved for each pending activity. It receives the raw payload,
    decodes it to the activity's declared output type, and completes the future
    returned to workflow code. *)
type activity_resolution =
  (Temporal_base.Codec.payload, Temporal_base.Error.t) result -> unit

(** Function saved for each pending child workflow. Child and activity
    resolutions have the same wire payload shape but remain in separate tables,
    preventing one job kind from resolving an operation of the other kind. *)
type child_workflow_resolution =
  (Temporal_base.Codec.payload, Temporal_base.Error.t) result -> unit

(** A child resolver survives its start acknowledgment. Core first reports the
    assigned run ID, then later reports the terminal payload or failure. Keeping
    both pieces in one table entry prevents a successful start from being
    mistaken for completion and gives duplicate acknowledgments a typed error. *)
type child_workflow_state = {
  resolve : child_workflow_resolution;
  mutable start_run_id : string option;
}

(** State shared by SDK operations in one workflow execution. Activities and
    timers use increasing sequence numbers so later activation jobs can identify
    the command they complete. Commands are stored in reverse order because
    adding to the front of an OCaml list is constant time. *)
type t = {
  scheduler : Scheduler.t;
  task_queue : string;
  mutable next_sequence : int64;
  activities : (int64, activity_resolution) Hashtbl.t;
  child_workflows : (int64, child_workflow_state) Hashtbl.t;
  timers : (int64, unit -> unit) Hashtbl.t;
  mutable commands_rev : Activation.command list;
}

(** Validates the worker queue before it becomes an implicit activity option.
    Queue names cross the strict JSON boundary even when workflow code omits
    [~task_queue], so rejecting an empty, NUL-containing, oversized, or
    non-UTF-8 default at execution construction keeps configuration failures
    out of the later workflow command path. The result carries the stable
    diagnostic so worker construction can reject the same value without
    catching an exception. *)
let validate_task_queue task_queue =
  if String.equal task_queue "" then
    Error "task_queue must not be empty"
  else if String.contains task_queue '\000' then
    Error "task_queue must not contain NUL"
  else if String.length task_queue > 65_536 then
    Error "task_queue exceeds 65536 bytes"
  else if not (Temporal_base.Codec.valid_utf_8 task_queue) then
    Error "task_queue must be valid UTF-8"
  else Ok ()

(** Creates empty activity and timer tables. The tables grow normally if a
    workflow has more than the small initial capacity. *)
let create ?(task_queue = "default") scheduler =
  match validate_task_queue task_queue with
  | Error message ->
      (* Preserve the existing execution-construction contract for callers
         that create a runtime directly: invalid worker configuration is a
         programmer defect at this lower-level API. The worker adapter uses
         [validate_task_queue] directly and returns a typed configuration
         error before it publishes any execution state. *)
      invalid_arg message
  | Ok () ->
      {
        scheduler;
        task_queue;
        next_sequence = 0L;
        activities = Hashtbl.create 16;
        child_workflows = Hashtbl.create 16;
        timers = Hashtbl.create 16;
        commands_rev = [];
      }

(** Stores the currently running workflow separately on each OCaml Domain, so
    workflow code running on different Domains cannot see the wrong context. *)
let current_key = Domain.DLS.new_key (fun () -> None)
let current () = Domain.DLS.get current_key

(** Makes [context] current while [action] runs, then restores the previous
    value even if [action] raises. This prevents later code on the same Domain
    from mistakenly appearing to run inside a workflow. *)
let with_context context action =
  let previous = current () in
  Domain.DLS.set current_key (Some context);
  Fun.protect ~finally:(fun () -> Domain.DLS.set current_key previous) action

(** Runs infrastructure code with no workflow installed, then restores the
    previous context. This prevents re-entrant callbacks such as application
    log reporters from mutating deterministic workflow state. *)
let without_context action =
  let previous = current () in
  Domain.DLS.set current_key None;
  Fun.protect ~finally:(fun () -> Domain.DLS.set current_key previous) action

(** Builds the error returned when code waits for a workflow future from the
    wrong scheduler or after workflow execution has ended. *)
let outside_error () =
  Temporal_base.Error.defect
    ~message:"future awaited outside its workflow scheduler"

(** Creates a future through the normal scheduler path and immediately supplies
    its result, keeping the pending-future count correct. *)
let resolved context result =
  let future, resolve = Scheduler.promise context.scheduler ~outside_error in
  resolve result;
  future

(** Allocates a scheduler-owned notification future without adding a history
    command. The normal scheduler teardown releases it if the workflow ends
    before the owning helper signals it. *)
let create_signal context = Scheduler.promise context.scheduler ~outside_error

(** Returns an already-failed future when an SDK operation is called without an
    active workflow, without creating fake global workflow state. *)
let detached_error ~message =
  Future_store.resolved ~outside_error
    (Error (Temporal_base.Error.defect ~message))

(** Returns the next command number. Zero is reserved, so the first is one. *)
let allocate_sequence context =
  context.next_sequence <- Int64.succ context.next_sequence;
  context.next_sequence

(** Adds a command to the front of the internal list; [take_commands] restores
    creation order before returning it. *)
let emit context command = context.commands_rev <- command :: context.commands_rev

(** Records a terminal command before aborting the current scheduler. The
    command is therefore retained even though the calling continuation is not
    resumed. *)
let terminate context command =
  emit context command;
  Scheduler.abort_workflow ()

(** Records the successor command before aborting the current scheduler. The
    command must be visible even though the terminal effect never resumes its
    caller. *)
let continue_as_new context ~workflow_type ~input =
  terminate context (Activation.Continue_as_new { workflow_type; input })

(** Saves the future resolver before emitting the schedule command. This order
    ensures even an immediate synthetic result can find the pending activity. *)
let schedule_activity context ~name ~input ?activity_id ?task_queue
    ?schedule_to_close_timeout ?schedule_to_start_timeout ?start_to_close_timeout
    ?heartbeat_timeout ?retry_policy ?(cancellation_type = Activation.Try_cancel)
    ?(do_not_eagerly_execute = false) ~decode () =
  let seq = allocate_sequence context in
  let future, resolve = Scheduler.promise context.scheduler ~outside_error in
  Hashtbl.add context.activities seq (fun result ->
      match result with
      | Error error -> resolve (Error error)
      | Ok payload ->
          (* User/codec decoders must not escape job processing half-applied.
             Contain unexpected exceptions as structured defects. *)
          (match decode payload with
          | result -> resolve result
          | exception exn ->
              resolve
                (Error
                   (Temporal_base.Error.defect
                      ~message:
                        ("activity result decoder raised: "
                        ^ Printexc.to_string exn)))));
  let activity_id =
    match activity_id with
    | Some value -> value
    | None -> "ocaml-activity-" ^ Int64.to_string seq
  in
  let task_queue = Option.value task_queue ~default:context.task_queue in
  (* Temporal requires at least one activity timeout. A deterministic
     start-to-close default keeps the long-standing [Activity.start] call
     usable while labelled timeout arguments remain available for workflows
     that need a different policy. *)
  let start_to_close_timeout =
    match (schedule_to_close_timeout, start_to_close_timeout) with
    | None, None -> Some 60_000L
    | _, value -> value
  in
  emit context
    (Activation.Schedule_activity
       {
         seq;
         activity_id;
         activity_type = name;
         task_queue;
         arguments = [ input ];
         schedule_to_close_timeout;
         schedule_to_start_timeout;
         start_to_close_timeout;
         heartbeat_timeout;
         retry_policy;
         cancellation_type;
         do_not_eagerly_execute;
       });
  future

(** Saves a child resolver before emitting its command. The explicit [id] is
    application-owned durable identity; the private [seq] only correlates Core
    completion jobs with this in-memory execution. *)
let start_child_workflow context ~id ~name ~input ~decode =
  let seq = allocate_sequence context in
  let future, resolve = Scheduler.promise context.scheduler ~outside_error in
  Hashtbl.add context.child_workflows seq
    {
      resolve =
        (fun result ->
          match result with
          | Error error -> resolve (Error error)
          | Ok payload -> (
              match decode payload with
              | result -> resolve result
              | exception exn ->
                  resolve
                    (Error
                       (Temporal_base.Error.defect
                          ~message:
                            ("child workflow result decoder raised: "
                            ^ Printexc.to_string exn)))));
      start_run_id = None;
    };
  emit context (Activation.Start_child_workflow { seq; id; name; input });
  future

(** Starts a timer whose future completes with [()] because a timer firing has
    no result payload. *)
let start_timer context milliseconds =
  let seq = allocate_sequence context in
  let future, resolve = Scheduler.promise context.scheduler ~outside_error in
  Hashtbl.add context.timers seq (fun () -> resolve (Ok ()));
  emit context (Activation.Start_timer { seq; milliseconds });
  future

(** Builds an error indicating that Core and the OCaml runtime disagree about
    which activity or timer is pending. *)
let bridge_error message =
  Temporal_base.Error.make ~non_retryable:true ~category:`Bridge ~message ()

(** Removes the activity before invoking its resolver. If resolving triggers a
    repeated completion immediately, the repeated sequence is rejected instead
    of completing the same future twice. *)
let resolve_activity context ~seq result =
  match Hashtbl.find_opt context.activities seq with
  | None ->
      Error
        (bridge_error
           (Printf.sprintf "unknown or duplicate activity sequence %Ld" seq))
  | Some resolve ->
      Hashtbl.remove context.activities seq;
      resolve result;
      Ok ()

(** Records the start acknowledgment or removes the child resolver on a start
    failure. Once a run ID is recorded, every later start result is rejected so
    a conflicting acknowledgment cannot complete or discard the child future. *)
let resolve_child_workflow_start context ~seq result =
  match Hashtbl.find_opt context.child_workflows seq with
  | None ->
      Error
        (bridge_error
           (Printf.sprintf
              "unknown or duplicate child workflow start sequence %Ld" seq))
  | Some state -> (
      match (state.start_run_id, result) with
      | Some _, _ ->
          Error
            (bridge_error
               (Printf.sprintf
                  "duplicate child workflow start sequence %Ld" seq))
      | None, Ok run_id ->
          if String.equal run_id "" then
            Error (bridge_error "child workflow start returned an empty run ID")
          else (
            state.start_run_id <- Some run_id;
            Ok ())
      | _, Error error ->
          Hashtbl.remove context.child_workflows seq;
          state.resolve (Error error);
          Ok ())

let resolve_child_workflow context ~seq result =
  match Hashtbl.find_opt context.child_workflows seq with
  | None ->
      Error
        (bridge_error
           (Printf.sprintf "unknown or duplicate child workflow sequence %Ld" seq))
  | Some state -> (
      match state.start_run_id with
      | None ->
          Error
            (bridge_error
               (Printf.sprintf
                  "child workflow sequence %Ld resolved before start acknowledgment"
                  seq))
      | Some _ ->
          Hashtbl.remove context.child_workflows seq;
          state.resolve result;
          Ok ())

(** Removes a timer before completing its future, matching activity handling. *)
let fire_timer context ~seq =
  match Hashtbl.find_opt context.timers seq with
  | None ->
      Error
        (bridge_error (Printf.sprintf "unknown or duplicate timer sequence %Ld" seq))
  | Some fire ->
      Hashtbl.remove context.timers seq;
      fire ();
      Ok ()

(** Returns commands created so far and clears the buffer. Commands created
    later are returned by the next call. *)
let take_commands context =
  let commands = List.rev context.commands_rev in
  context.commands_rev <- [];
  commands

(** Closes pending futures before clearing activity and timer callbacks. This
    releases paused workflow fibers and the OCaml values they reference. *)
let shutdown context =
  Scheduler.shutdown context.scheduler;
  Hashtbl.clear context.activities;
  Hashtbl.clear context.child_workflows;
  Hashtbl.clear context.timers
