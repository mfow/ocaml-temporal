(** Contains the scheduler operations used by a future. Futures queue all
    resumptions through these functions, so workflow fibers still resume one at
    a time even when Rust reports several results concurrently. *)
type owner = {
  id : int;
  enqueue : (unit -> unit) -> unit;
  is_running : unit -> bool;
  on_create : unit -> unit;
  on_settled : unit -> unit;
  register_teardown : (unit -> unit) -> unit;
}

(** Collects the scheduler callbacks stored in each future. *)
let make_owner ~id ~enqueue ~is_running ~on_create ~on_settled
    ~register_teardown =
  { id; enqueue; is_running; on_create; on_settled; register_teardown }

(** Gives the standard library [result] type a local name distinct from [state]. *)
type ('value, 'error) result = ('value, 'error) Stdlib.result

(** A paused workflow fiber waiting for a result. OCaml continuations can be
    resumed only once, which matches a future's single-result rule. *)
type ('value, 'error) waiter =
  (('value, 'error) result, unit) Effect.Deep.continuation

(** Holds paused fibers and mapping callbacks while a result is unavailable.
    Lists are reversed so adding a new entry is constant time. *)
type ('value, 'error) pending = {
  mutable waiters : ('value, 'error) waiter list;
  mutable observers : (('value, 'error) result -> unit) list;
}

(** [Closed] means the workflow ended before this future received a result and
    all paused fibers have been released. *)
type ('value, 'error) state =
  | Pending of ('value, 'error) pending
  | Ready of ('value, 'error) result
  | Closed

(** Stores the scheduler, the error to return when used from the wrong context,
    and the future's current state. *)
type ('value, 'error) t = {
  owner : owner;
  outside_error : unit -> 'error;
  mutable state : ('value, 'error) state;
}

type ('value, 'error) resolver = ('value, 'error) result -> unit

(** Identifies which input completed a heterogeneous two-way race. The public
    SDK re-exports this type while keeping the observer machinery private. *)
type ('left, 'right) race = Left of 'left | Right of 'right

type _ Effect.t +=
  | Await : ('value, 'error) t -> ('value, 'error) result Effect.t

(** Internal exception used only to release a paused fiber during shutdown. It
    is caught inside this module and never becomes a workflow error. *)
exception Scheduler_shutdown

(** Closes a pending future during workflow completion, eviction, or shutdown.
    It updates the scheduler's pending count, clears callback references, and
    releases every paused fiber so the workflow's memory can be collected. *)
let teardown promise () =
  match promise.state with
  | Ready _ | Closed -> ()
  | Pending pending ->
      promise.state <- Closed;
      promise.owner.on_settled ();
      let waiters = List.rev pending.waiters in
      pending.waiters <- [];
      pending.observers <- [];
      List.iter
        (fun continuation ->
          try Effect.Deep.discontinue continuation Scheduler_shutdown
          with Scheduler_shutdown -> ())
        waiters

(** Creates a pending future and its resolver. The resolver stores the result
    before queueing paused fibers and callbacks, so any callback that inspects
    the future immediately sees it as ready. *)
let create ~owner ~outside_error =
  let promise =
    { owner; outside_error; state = Pending { waiters = []; observers = [] } }
  in
  owner.on_create ();
  owner.register_teardown (teardown promise);
  let resolve result =
    match promise.state with
    | Ready _ | Closed -> invalid_arg "Temporal future resolved more than once"
    | Pending pending ->
        promise.state <- Ready result;
        owner.on_settled ();
        List.iter
          (fun continuation ->
            owner.enqueue (fun () -> Effect.Deep.continue continuation result))
          (List.rev pending.waiters);
        List.iter
          (fun observer -> owner.enqueue (fun () -> observer result))
          (List.rev pending.observers);
        pending.waiters <- [];
        pending.observers <- []
  in
  (promise, resolve)

(** Scheduler substitute for an already-known result created outside workflow
    execution. It runs callbacks immediately and never allows [await] to pause. *)
let inert_owner =
  make_owner ~id:(-1) ~enqueue:(fun thunk -> thunk ())
    ~is_running:(fun () -> false) ~on_create:(fun () -> ())
    ~on_settled:(fun () -> ()) ~register_teardown:(fun _ -> ())

(** Creates an outside-workflow future and immediately gives it a result using
    the same resolver logic as ordinary futures. *)
let resolved ~outside_error result =
  let promise, resolve = create ~owner:inert_owner ~outside_error in
  resolve result;
  promise

let owner_id promise = promise.owner.id

(** Queues one callback on the scheduler that owns [promise]. This is the
    low-level escape hatch used by public wrappers to preserve the same FIFO
    execution ordering as native future completions. *)
let enqueue promise thunk = promise.owner.enqueue thunk

(** Returns a ready result, or pauses only when called from the active scheduler
    that owns this future. *)
let await promise =
  match promise.state with
  | Ready result -> result
  | Closed -> Error (promise.outside_error ())
  | Pending _ ->
      if promise.owner.is_running () then Effect.perform (Await promise)
      else Error (promise.outside_error ())

(** Saves a paused fiber on a pending future. If the result or shutdown happened
    first, queues the fiber immediately with the appropriate result. *)
let add_waiter promise continuation =
  match promise.state with
  | Pending pending -> pending.waiters <- continuation :: pending.waiters
  | Ready result ->
      promise.owner.enqueue (fun () -> Effect.Deep.continue continuation result)
  | Closed ->
      promise.owner.enqueue (fun () ->
          Effect.Deep.continue continuation (Error (promise.outside_error ())))

(** Registers a callback used by [map], [map_error], and [both]. It always runs
    through the workflow scheduler and never pauses a fiber itself. *)
let observe promise observer =
  match promise.state with
  | Pending pending -> pending.observers <- observer :: pending.observers
  | Ready result -> promise.owner.enqueue (fun () -> observer result)
  | Closed ->
      promise.owner.enqueue (fun () -> observer (Error (promise.outside_error ())))

(** Waits for an arbitrary notification associated with [promise] without
    exposing an OCaml effect to higher layers. [register] receives a
    single-use signal callback; the callback must be invoked by the observer
    that makes the notification ready. The temporary gate is owned by the
    same scheduler, so the calling workflow fiber is suspended rather than a
    scheduler thread being blocked. *)
let await_gate promise register =
  let gate, resolve = create ~owner:promise.owner ~outside_error:(fun () -> ()) in
  let signaled = ref false in
  let signal () =
    if not !signaled then (
      signaled := true;
      resolve (Ok ()))
  in
  register signal;
  ignore (await gate)

(** Maps a successful result into a new future owned by the same scheduler. *)
let map mapper source =
  let mapped, resolve =
    create ~owner:source.owner ~outside_error:source.outside_error
  in
  observe source (fun result -> resolve (Result.map mapper result));
  mapped

(** Maps both a stored error and the error returned for use outside the owning
    workflow scheduler. *)
let map_error mapper source =
  let mapped, resolve =
    create ~owner:source.owner
      ~outside_error:(fun () -> mapper (source.outside_error ()))
  in
  observe source (fun result -> resolve (Result.map_error mapper result));
  mapped

(** Creates a ready failure owned by [source]. Aggregate ownership errors use
    this path so application mistakes remain typed values while subsequent
    combinators still belong to the original workflow scheduler. *)
let failed_from source error =
  let failed, resolve =
    create ~owner:source.owner ~outside_error:source.outside_error
  in
  resolve (Error error);
  failed

(** Reports whether every future belongs to the scheduler that owns [first]. *)
let same_owner first futures =
  List.for_all (fun future -> future.owner.id = first.owner.id) futures

(** Records both results before completing the combined future. One failure
    does not implicitly cancel or stop observing the other operation. A
    cross-scheduler pair produces [ownership_error] as a value. *)
let both ~ownership_error left right =
  if left.owner.id <> right.owner.id then failed_from left (ownership_error ())
  else
  let combined, resolve =
    create ~owner:left.owner ~outside_error:left.outside_error
  in
  let left_result = ref None in
  let right_result = ref None in
  let finish () =
    match (!left_result, !right_result) with
    | Some (Ok left), Some (Ok right) -> resolve (Ok (left, right))
    | Some (Error error), Some _ -> resolve (Error error)
    | Some _, Some (Error error) -> resolve (Error error)
    | _ -> ()
  in
  observe left (fun result ->
      left_result := Some result;
      finish ());
  observe right (fun result ->
      right_result := Some result;
      finish ());
  combined

(** Completes after every input and preserves input order. Errors are selected
    in input order only after all siblings settle, so aggregation never implies
    cancellation. *)
let all ~ownership_error futures =
  match futures with
  | [] -> resolved ~outside_error:ownership_error (Ok [])
  | first :: _ when not (same_owner first futures) ->
      failed_from first (ownership_error ())
  | first :: _ ->
      let combined, resolve =
        create ~owner:first.owner ~outside_error:first.outside_error
      in
      let remaining = ref (List.length futures) in
      let results = Array.make !remaining None in
      let finish_if_complete () =
        if !remaining = 0 then
          let ordered = Array.to_list results in
          match List.find_map (function Some (Error error) -> Some error | _ -> None) ordered with
          | Some error -> resolve (Error error)
          | None ->
              resolve
                (Ok
                   (List.map
                      (function
                        | Some (Ok value) -> value
                        | Some (Error _) | None ->
                            failwith "Temporal.Future.all result invariant violated")
                      ordered))
      in
      List.iteri
        (fun index future ->
          observe future (fun result ->
              results.(index) <- Some result;
              remaining := !remaining - 1;
              finish_if_complete ()))
        futures;
      combined

(** Settles with the first observed completion of two differently typed inputs.
    Observer registration order makes an already-ready left input win; pending
    inputs follow the scheduler's deterministic callback order. *)
let race ~ownership_error left right =
  if left.owner.id <> right.owner.id then failed_from left (ownership_error ())
  else
    let combined, resolve =
      create ~owner:left.owner ~outside_error:left.outside_error
    in
    let settled = ref false in
    let finish wrap result =
      if not !settled then (
        settled := true;
        resolve (Result.map wrap result))
    in
    observe left (finish (fun value -> Left value));
    observe right (finish (fun value -> Right value));
    combined

(** Settles with the first completion from a non-empty homogeneous collection.
    The mandatory [first] argument makes an empty selection unrepresentable. *)
let first ~ownership_error leading rest =
  if not (same_owner leading rest) then failed_from leading (ownership_error ())
  else
    let combined, resolve =
      create ~owner:leading.owner ~outside_error:leading.outside_error
    in
    let settled = ref false in
    let finish result =
      if not !settled then (
        settled := true;
        resolve result)
    in
    List.iter (fun future -> observe future finish) (leading :: rest);
    combined

(** Checks the state directly without pausing or scheduling work. *)
let is_ready promise = match promise.state with Ready _ -> true | _ -> false

(** Returns a ready result for inspection without removing it from the future. *)
let peek promise =
  match promise.state with Ready result -> Some result | Pending _ | Closed -> None
