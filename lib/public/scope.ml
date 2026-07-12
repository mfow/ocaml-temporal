(** Implements workflow-local cancellation scopes without exposing the
    scheduler effect or the internal notification future. A scope's signal is
    an ordinary scheduler-owned future, so cancellation and waiter resumption
    use the same deterministic FIFO queue as every other workflow future. *)

type state = Active | Cancelled

(** The private state retained by one scope. The resolver and liveness callback
    are kept only to complete the scheduler-owned signal exactly once and to
    avoid touching a future already closed by workflow teardown. *)
type t = {
  cancellation : (unit, Error.t) Future.t;
  resolve : (unit, Temporal_base.Error.t) result -> unit;
  owner_id : int;
  callbacks_live : unit -> bool;
  mutable state : state;
}

(** Constructs the stable public error returned when a scope has been
    cancelled. This is a normal operational result, not an exception. *)
let cancellation_error () =
  Error.make ~category:`Cancelled ~message:"workflow scope cancelled" ()

(** Constructs the defect used when a scope operation is attempted from an
    unrelated Domain or outside its workflow scheduler. *)
let ownership_error operation =
  Error.defect
    ~message:("Temporal.Scope." ^ operation ^ " used outside its owning workflow scheduler")

(** Creates a scheduler-owned scope signal only while a workflow context is
    active. The runtime context owns the underlying future and registers its
    teardown, while this public module retains only the typed façade and its
    one resolver. *)
let create () =
  match Temporal_runtime.Workflow_context_store.current () with
  | None ->
      Error
        (Error.defect
           ~message:"Temporal.Scope.create used outside a workflow execution")
  | Some context ->
      let cancellation_base, resolve =
        Temporal_runtime.Workflow_context_store.create_signal context
      in
      let cancellation =
        Future_private.of_internal
          ~outside_error:cancellation_error cancellation_base
      in
      Ok
        {
          cancellation;
          resolve;
          owner_id =
            Temporal_runtime.Future_store.owner_id cancellation_base;
          callbacks_live =
            (fun () ->
              Temporal_runtime.Future_store.callbacks_live cancellation_base);
          state = Active;
        }

(** Checks whether the current scheduler is the one that owns [scope]. This
    prevents a caller on another Domain from enqueueing a cancellation signal
    into a queue it does not own. *)
let owns_scheduler scope =
  Temporal_runtime.Future_store.current_owner_matches scope.owner_id

(** Records cancellation and signals waiters through the owning scheduler.
    An active scope cannot be cancelled between scheduler runs or from another
    Domain: doing so would mutate state without a queue turn that can resume
    its waiters. Once a scope is already cancelled, repeated calls are pure
    idempotent reads and therefore remain safe. *)
let cancel scope =
  match scope.state with
  | Cancelled -> Ok ()
  | Active ->
      if not (owns_scheduler scope) then
        Error (ownership_error "cancel")
      else (
        scope.state <- Cancelled;
        if scope.callbacks_live () then scope.resolve (Ok ());
        Ok ())

(** Reports the scope state without scheduling work or touching the resolver. *)
let is_cancelled scope =
  match scope.state with Cancelled -> true | Active -> false

(** Returns the scope's current cancellation result. *)
let check scope =
  if is_cancelled scope then Error (cancellation_error ()) else Ok ()

(** Verifies that [scope] is being used by its owner scheduler before it can
    suspend a workflow fiber. Ready futures still obey this check: accepting a
    ready value from another execution would make ownership errors depend on
    timing rather than on the API contract. *)
let ensure_owner scope =
  if owns_scheduler scope then Ok () else Error (ownership_error "await")

(** Waits for either the operation or the scope signal. The operation is
    registered first, so if both inputs are already ready the deterministic
    Future race rule gives the operation precedence; a cancellation already
    recorded in [state] wins before registration. *)
let await scope future =
  match ensure_owner scope with
  | Error error -> Error error
  | Ok () -> (
      match check scope with
      | Error error -> Error error
      | Ok () ->
          match Future.await (Future.race future scope.cancellation) with
          | Error error -> Error error
          | Ok (Future.Left value) -> Ok value
          | Ok (Future.Right ()) -> Error (cancellation_error ()))

(** Runs a body inside a fresh scope and always requests cancellation during
    cleanup. The body remains responsible for awaiting all branches it wants
    to observe; cleanup closes the scope's notification future when it returns
    so no helper-created signal remains pending. *)
let with_scope body =
  match create () with
  | Error _ as error -> error
  | Ok scope ->
      Fun.protect
        ~finally:(fun () -> ignore (cancel scope))
        (fun () -> body scope)
