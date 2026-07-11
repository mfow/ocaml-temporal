type runnable = Runnable of int * (unit -> unit)

type t = {
  id : int;
  queue : runnable Queue.t;
  mutable next_sequence : int;
  mutable running : bool;
  mutable active : bool;
  mutable pending : int;
  mutable failures : exn list;
  mutable trace_rev : int list;
  mutable teardowns : (unit -> unit) list;
}

type status = Complete | Failed of exn | Blocked

let next_scheduler_id = Atomic.make 0

let create () =
  {
    id = Atomic.fetch_and_add next_scheduler_id 1;
    queue = Queue.create ();
    next_sequence = 0;
    running = false;
    active = true;
    pending = 0;
    failures = [];
    trace_rev = [];
    teardowns = [];
  }

let enqueue scheduler thunk =
  if scheduler.active then (
    let sequence = scheduler.next_sequence in
    scheduler.next_sequence <- sequence + 1;
    Queue.push (Runnable (sequence, thunk)) scheduler.queue)

let owner scheduler =
  Future_store.make_owner ~id:scheduler.id ~enqueue:(enqueue scheduler)
    ~is_running:(fun () -> scheduler.running && scheduler.active)
    ~on_create:(fun () -> scheduler.pending <- scheduler.pending + 1)
    ~on_settled:(fun () -> scheduler.pending <- scheduler.pending - 1)
    ~register_teardown:(fun teardown ->
      scheduler.teardowns <- teardown :: scheduler.teardowns)

let promise scheduler ~outside_error =
  if not scheduler.active then invalid_arg "Temporal scheduler is shut down";
  Future_store.create ~owner:(owner scheduler) ~outside_error

let handle scheduler thunk =
  Effect.Deep.match_with thunk ()
    {
      retc = (fun () -> ());
      exnc = (fun exception_ -> scheduler.failures <- exception_ :: scheduler.failures);
      effc =
        (fun (type result) (operation : result Effect.t) ->
          match operation with
          | Future_store.Await future ->
              Some
                (fun (continuation : (result, unit) Effect.Deep.continuation) ->
                  Future_store.add_waiter future continuation)
          | _ -> None);
    }

let spawn scheduler thunk =
  if not scheduler.active then invalid_arg "Temporal scheduler is shut down";
  enqueue scheduler (fun () -> handle scheduler thunk)

let run scheduler =
  if not scheduler.active then invalid_arg "Temporal scheduler is shut down";
  if scheduler.running then invalid_arg "Temporal scheduler is already running";
  scheduler.running <- true;
  Fun.protect
    ~finally:(fun () -> scheduler.running <- false)
    (fun () ->
      while not (Queue.is_empty scheduler.queue) do
        let (Runnable (sequence, thunk)) = Queue.pop scheduler.queue in
        scheduler.trace_rev <- sequence :: scheduler.trace_rev;
        (try thunk ()
         with exception_ -> scheduler.failures <- exception_ :: scheduler.failures)
      done;
      match List.rev scheduler.failures with
      | failure :: _ -> Failed failure
      | [] when scheduler.pending > 0 -> Blocked
      | [] -> Complete)

let run_label scheduler =
  match run scheduler with
  | Complete -> "complete"
  | Failed _ -> "failed"
  | Blocked -> "blocked"

let trace scheduler = List.rev scheduler.trace_rev

let shutdown scheduler =
  if scheduler.active then (
    scheduler.active <- false;
    List.iter (fun teardown -> teardown ()) (List.rev scheduler.teardowns);
    scheduler.teardowns <- [];
    Queue.clear scheduler.queue)
