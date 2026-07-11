type owner = {
  id : int;
  enqueue : (unit -> unit) -> unit;
  is_running : unit -> bool;
  on_create : unit -> unit;
  on_settled : unit -> unit;
  register_teardown : (unit -> unit) -> unit;
}

let make_owner ~id ~enqueue ~is_running ~on_create ~on_settled
    ~register_teardown =
  { id; enqueue; is_running; on_create; on_settled; register_teardown }

type ('value, 'error) result = ('value, 'error) Stdlib.result

type ('value, 'error) waiter =
  (('value, 'error) result, unit) Effect.Deep.continuation

type ('value, 'error) pending = {
  mutable waiters : ('value, 'error) waiter list;
  mutable observers : (('value, 'error) result -> unit) list;
}

type ('value, 'error) state =
  | Pending of ('value, 'error) pending
  | Ready of ('value, 'error) result
  | Closed

type ('value, 'error) t = {
  owner : owner;
  outside_error : unit -> 'error;
  mutable state : ('value, 'error) state;
}

type ('value, 'error) resolver = ('value, 'error) result -> unit

type _ Effect.t +=
  | Await : ('value, 'error) t -> ('value, 'error) result Effect.t

exception Scheduler_shutdown

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

let owner_id promise = promise.owner.id

let await promise =
  match promise.state with
  | Ready result -> result
  | Closed -> Error (promise.outside_error ())
  | Pending _ ->
      if promise.owner.is_running () then Effect.perform (Await promise)
      else Error (promise.outside_error ())

let add_waiter promise continuation =
  match promise.state with
  | Pending pending -> pending.waiters <- continuation :: pending.waiters
  | Ready result ->
      promise.owner.enqueue (fun () -> Effect.Deep.continue continuation result)
  | Closed ->
      promise.owner.enqueue (fun () ->
          Effect.Deep.continue continuation (Error (promise.outside_error ())))

let observe promise observer =
  match promise.state with
  | Pending pending -> pending.observers <- observer :: pending.observers
  | Ready result -> promise.owner.enqueue (fun () -> observer result)
  | Closed ->
      promise.owner.enqueue (fun () -> observer (Error (promise.outside_error ())))

let map mapper source =
  let mapped, resolve =
    create ~owner:source.owner ~outside_error:source.outside_error
  in
  observe source (fun result -> resolve (Result.map mapper result));
  mapped

let map_error mapper source =
  let mapped, resolve =
    create ~owner:source.owner
      ~outside_error:(fun () -> mapper (source.outside_error ()))
  in
  observe source (fun result -> resolve (Result.map_error mapper result));
  mapped

let both left right =
  if left.owner.id <> right.owner.id then
    invalid_arg "Temporal.Future.both received futures from different schedulers";
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

let is_ready promise = match promise.state with Ready _ -> true | _ -> false

let peek promise =
  match promise.state with Ready result -> Some result | Pending _ | Closed -> None
