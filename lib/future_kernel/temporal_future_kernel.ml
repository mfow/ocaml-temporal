(** Private callback representation shared by the public future facade and its
    package-internal runtime adapters.  The public [Temporal.Future] module
    aliases this type but exposes no constructor or record fields. *)
type ('value, 'error) t = {
  (* Retrieves the result while the owning scheduler is active. *)
  await_impl : unit -> ('value, 'error) result;
  (* Registers a continuation with the scheduler's suspension gate. *)
  await_gate_impl : (((unit -> unit) -> unit) -> unit);
  (* Registers a callback for the next owner-scheduler notification. *)
  observe_impl : (('value, 'error) result -> unit) -> unit;
  (* Reports whether this operation has already settled. *)
  is_ready_impl : unit -> bool;
  (* Reads a settled result without consuming it. *)
  peek_impl : unit -> ('value, 'error) result option;
  (* Identifies the workflow execution that owns this future. *)
  owner_id_impl : int;
  (* Builds the typed error returned when the future is used off-owner. *)
  outside_error_impl : unit -> 'error;
  (* Queues a callback on the owning scheduler. *)
  enqueue_impl : (unit -> unit) -> unit;
}

(** Constructs a kernel future from callbacks owned by one scheduler.  The
    callbacks remain the source of truth for lifecycle and cleanup; this
    record only groups those callbacks behind the private package boundary. *)
let make ~await ~await_gate ~observe ~is_ready ~peek ~owner_id ~outside_error
    ~enqueue =
  {
    await_impl = await;
    await_gate_impl = await_gate;
    observe_impl = observe;
    is_ready_impl = is_ready;
    peek_impl = peek;
    owner_id_impl = owner_id;
    outside_error_impl = outside_error;
    enqueue_impl = enqueue;
  }

(** Invokes the scheduler-owned result callback. *)
let await future = future.await_impl ()

(** Registers a continuation with the scheduler-owned suspension gate. *)
let await_gate future register = future.await_gate_impl register

(** Registers an observer for a scheduler-owned result notification. *)
let observe future callback = future.observe_impl callback

(** Reports whether the scheduler-owned result is settled. *)
let is_ready future = future.is_ready_impl ()

(** Reads a settled scheduler-owned result without consuming it. *)
let peek future = future.peek_impl ()

(** Returns the workflow-execution identity that owns this value. *)
let owner_id future = future.owner_id_impl

(** Builds the error returned when the value is used outside its owner. *)
let outside_error future = future.outside_error_impl

(** Queues a callback on the scheduler that owns this value. *)
let enqueue future callback = future.enqueue_impl callback
