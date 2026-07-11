type activity_resolution =
  (Temporal_base.Codec.payload, Temporal_base.Error.t) result -> unit

type t = {
  scheduler : Scheduler.t;
  mutable next_sequence : int64;
  activities : (int64, activity_resolution) Hashtbl.t;
  timers : (int64, unit -> unit) Hashtbl.t;
  mutable commands_rev : Activation.command list;
}

let create scheduler =
  {
    scheduler;
    next_sequence = 0L;
    activities = Hashtbl.create 16;
    timers = Hashtbl.create 16;
    commands_rev = [];
  }

let current_key = Domain.DLS.new_key (fun () -> None)
let current () = Domain.DLS.get current_key

let with_context context action =
  let previous = current () in
  Domain.DLS.set current_key (Some context);
  Fun.protect ~finally:(fun () -> Domain.DLS.set current_key previous) action

let outside_error () =
  Temporal_base.Error.defect
    ~message:"future awaited outside its workflow scheduler"

let resolved context result =
  let future, resolve = Scheduler.promise context.scheduler ~outside_error in
  resolve result;
  future

let detached_error ~message =
  Future_store.resolved ~outside_error
    (Error (Temporal_base.Error.defect ~message))

let allocate_sequence context =
  context.next_sequence <- Int64.succ context.next_sequence;
  context.next_sequence

let emit context command = context.commands_rev <- command :: context.commands_rev

let schedule_activity context ~name ~input ~decode =
  let seq = allocate_sequence context in
  let future, resolve = Scheduler.promise context.scheduler ~outside_error in
  Hashtbl.add context.activities seq (fun result ->
      match result with
      | Error error -> resolve (Error error)
      | Ok payload -> resolve (decode payload));
  emit context (Activation.Schedule_activity { seq; name; input });
  future

let start_timer context milliseconds =
  let seq = allocate_sequence context in
  let future, resolve = Scheduler.promise context.scheduler ~outside_error in
  Hashtbl.add context.timers seq (fun () -> resolve (Ok ()));
  emit context (Activation.Start_timer { seq; milliseconds });
  future

let bridge_error message =
  Temporal_base.Error.make ~non_retryable:true ~category:`Bridge ~message ()

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

let fire_timer context ~seq =
  match Hashtbl.find_opt context.timers seq with
  | None ->
      Error
        (bridge_error (Printf.sprintf "unknown or duplicate timer sequence %Ld" seq))
  | Some fire ->
      Hashtbl.remove context.timers seq;
      fire ();
      Ok ()

let take_commands context =
  let commands = List.rev context.commands_rev in
  context.commands_rev <- [];
  commands

let shutdown context =
  Scheduler.shutdown context.scheduler;
  Hashtbl.clear context.activities;
  Hashtbl.clear context.timers
