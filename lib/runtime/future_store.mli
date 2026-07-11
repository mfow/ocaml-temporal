(** The scheduler operations a future needs. Each future keeps this value so
    its callbacks are queued by the workflow execution that created it and can
    be cleaned up when that execution ends. *)
type owner

(** Builds the scheduler connection used by futures. [on_create] is called once
    for each new pending future; [on_settled] is called once when that future
    receives a result or is closed during shutdown. *)
val make_owner :
  id:int ->
  enqueue:((unit -> unit) -> unit) ->
  is_running:(unit -> bool) ->
  on_create:(unit -> unit) ->
  on_settled:(unit -> unit) ->
  register_teardown:((unit -> unit) -> unit) ->
  owner

(** A result that starts pending and can become ready exactly once. *)
type ('value, 'error) t

(** A function that supplies a future's result. Calling it twice raises
    [Invalid_argument], because one Temporal operation cannot have two results. *)
type ('value, 'error) resolver = ('value, 'error) result -> unit

(** Internal OCaml 5 effect used when [await] needs to pause a workflow fiber.
    Only [Scheduler] handles it; it is not exposed by the public SDK. *)
type _ Effect.t +=
  | Await : ('value, 'error) t -> ('value, 'error) result Effect.t

(** Creates a pending future and the function that will provide its result. It
    also registers cleanup for workflow completion, eviction, or shutdown. *)
val create :
  owner:owner ->
  outside_error:(unit -> 'error) ->
  ('value, 'error) t * ('value, 'error) resolver

(** Creates an already-completed future when no workflow scheduler is active,
    for example when input encoding fails before a command can be scheduled. *)
val resolved :
  outside_error:(unit -> 'error) ->
  ('value, 'error) result ->
  ('value, 'error) t

(** Returns the identity of the workflow scheduler that created the future. *)
val owner_id : ('value, 'error) t -> int

(** Returns the result when ready. If called by the future's active workflow
    scheduler, pauses the current fiber until the result arrives. Otherwise,
    returns the error produced by [outside_error]. *)
val await : ('value, 'error) t -> ('value, 'error) result

(** Saves the paused fiber so it can resume when the future completes. If the
    result arrived first, queues the fiber for immediate resumption. Only the
    scheduler's effect handler calls this function. *)
val add_waiter :
  ('value, 'error) t ->
  (('value, 'error) result, unit) Effect.Deep.continuation ->
  unit

(** Creates a future that transforms a successful result without waiting. *)
val map : ('value -> 'mapped) -> ('value, 'error) t -> ('mapped, 'error) t

(** Creates a future that transforms an error without waiting. *)
val map_error :
  ('error -> 'mapped_error) ->
  ('value, 'error) t ->
  ('value, 'mapped_error) t

(** Completes after both inputs have results. If both fail, returns the left
    error. Both futures must belong to the same workflow scheduler. *)
val both :
  ('left, 'error) t ->
  ('right, 'error) t ->
  ('left * 'right, 'error) t

(** Reports whether a result is available without suspending. *)
val is_ready : ('value, 'error) t -> bool

(** Returns a ready result or [None]. A future closed during shutdown also
    returns [None], because it has no result that application code may use. *)
val peek : ('value, 'error) t -> ('value, 'error) result option
