(** Pairs queued work with a number assigned when it enters the queue. Tests use
    the number to verify first-in, first-out execution order. *)
type runnable = Runnable of int * (unit -> unit)

(** State for one workflow scheduler. [pending] counts futures without results,
    and [teardowns] stores the cleanup function for each of those futures. *)
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

(** Supplies a different ID to every scheduler in the process, allowing
    [Future.both] to reject futures from different workflow executions. *)
let next_scheduler_id = Atomic.make 0

(** Initializes an active scheduler with no runnable or pending work. *)
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

(** Assigns FIFO sequence numbers at enqueue time. Work submitted after shutdown
    is intentionally discarded only by internal callbacks already being torn down. *)
let enqueue scheduler thunk =
  if scheduler.active then (
    let sequence = scheduler.next_sequence in
    scheduler.next_sequence <- sequence + 1;
    Queue.push (Runnable (sequence, thunk)) scheduler.queue)

(** Gives a future access to this scheduler's queue and pending-future count. *)
let owner scheduler =
  Future_store.make_owner ~id:scheduler.id ~enqueue:(enqueue scheduler)
    ~is_running:(fun () -> scheduler.running && scheduler.active)
    ~on_create:(fun () -> scheduler.pending <- scheduler.pending + 1)
    ~on_settled:(fun () -> scheduler.pending <- scheduler.pending - 1)
    ~register_teardown:(fun teardown ->
      scheduler.teardowns <- teardown :: scheduler.teardowns)

(** Rejects new future allocation after shutdown, when no continuation could be
    safely resumed. *)
let promise scheduler ~outside_error =
  if not scheduler.active then invalid_arg "Temporal scheduler is shut down";
  Future_store.create ~owner:(owner scheduler) ~outside_error

(** Handles the private [Await] effect by saving the paused workflow fiber on
    its future. Effects not owned by this scheduler continue to an outer
    handler. *)
let handle scheduler thunk =
  Effect.Deep.match_with thunk ()
    {
      retc = (fun () -> ());
      exnc =
        (fun exception_ ->
          (* Scheduler_shutdown only releases paused fibers during teardown; it
             is control flow, not a workflow defect. *)
          match exception_ with
          | Future_store.Scheduler_shutdown -> ()
          | _ -> scheduler.failures <- exception_ :: scheduler.failures);
      effc =
        (fun (type result) (operation : result Effect.t) ->
          match operation with
          | Future_store.Await future ->
              Some
                (fun (continuation : (result, unit) Effect.Deep.continuation) ->
                  Future_store.add_waiter future continuation)
          | _ -> None);
    }

(** Wraps root user code in the scheduler handler before enqueueing it. *)
let spawn scheduler thunk =
  if not scheduler.active then invalid_arg "Temporal scheduler is shut down";
  enqueue scheduler (fun () -> handle scheduler thunk)

(** Runs queued work until the queue is empty, including fibers added by future
    completions during the run. An uncaught exception is reported before
    [Blocked] or [Complete]. *)
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
         with Future_store.Scheduler_shutdown -> ()
         | exception_ -> scheduler.failures <- exception_ :: scheduler.failures)
      done;
      match List.rev scheduler.failures with
      | failure :: _ -> Failed failure
      | [] when scheduler.pending > 0 -> Blocked
      | [] -> Complete)

(** Stable string adapter used by smoke tests and future diagnostics. *)
let run_label scheduler =
  match run scheduler with
  | Complete -> "complete"
  | Failed _ -> "failed"
  | Blocked -> "blocked"

(** Returns the recorded queue sequence numbers in the order they ran. *)
let trace scheduler = List.rev scheduler.trace_rev

(** Closes pending futures in creation order and clears queued functions so
    their captured workflow values can be collected. *)
let shutdown scheduler =
  if scheduler.active then (
    scheduler.active <- false;
    List.iter (fun teardown -> teardown ()) (List.rev scheduler.teardowns);
    scheduler.teardowns <- [];
    Queue.clear scheduler.queue)
