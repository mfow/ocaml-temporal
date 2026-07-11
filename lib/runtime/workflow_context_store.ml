(** Function saved for each pending activity. It receives the raw payload,
    decodes it to the activity's declared output type, and completes the future
    returned to workflow code. *)
type activity_resolution =
  (Temporal_base.Codec.payload, Temporal_base.Error.t) result -> unit

(** State shared by SDK operations in one workflow execution. Activities and
    timers use increasing sequence numbers so later activation jobs can identify
    the command they complete. Commands are stored in reverse order because
    adding to the front of an OCaml list is constant time. *)
type t = {
  scheduler : Scheduler.t;
  mutable next_sequence : int64;
  activities : (int64, activity_resolution) Hashtbl.t;
  timers : (int64, unit -> unit) Hashtbl.t;
  mutable commands_rev : Activation.command list;
}

(** Creates empty activity and timer tables. The tables grow normally if a
    workflow has more than the small initial capacity. *)
let create scheduler =
  {
    scheduler;
    next_sequence = 0L;
    activities = Hashtbl.create 16;
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

(** Saves the future resolver before emitting the schedule command. This order
    ensures even an immediate synthetic result can find the pending activity. *)
let schedule_activity context ~name ~input ~decode =
  let seq = allocate_sequence context in
  let future, resolve = Scheduler.promise context.scheduler ~outside_error in
  Hashtbl.add context.activities seq (fun result ->
      match result with
      | Error error -> resolve (Error error)
      | Ok payload -> resolve (decode payload));
  emit context (Activation.Schedule_activity { seq; name; input });
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
  Hashtbl.clear context.timers
