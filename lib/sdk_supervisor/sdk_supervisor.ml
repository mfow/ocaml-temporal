(** A backend whose state is confined to one supervisor owner Domain. *)
module type Backend = sig
  type config
  type state
  type error
  type _ operation

  val create : config -> (state, error) result
  val perform : state -> 'result operation -> ('result, error) result
  val shutdown : state -> (unit, error) result
end

(** Implements serialized lifecycle ownership for one backend. *)
module Make (Backend : Backend) = struct
  (** Supervisor-level failures, distinct from expected backend failures. *)
  type error =
    | Backend of Backend.error
    | Closed
    | Supervisor_failed of exn

  (** Typed messages accepted by the sole owner Domain. *)
  module Request = struct
    type _ t =
      | Initialize : Backend.config -> (unit, error) result t
      | Perform : 'result Backend.operation -> ('result, error) result t
      | Shutdown : (unit, error) result t
  end

  module Mailbox = Mailbox_processor.Make (Request)

  (** Owner-only lifecycle. A closed value retains the exact shutdown result
      so every shutdown message already admitted before mailbox close agrees. *)
  type owner_state =
    | Not_started
    | Running of Backend.state
    | Closed_graph of (unit, error) result

  (** Progress of the one terminal mailbox request. Keeping the admitted reply
      separate from its eventual result makes shutdown admission observable
      without waiting for earlier backend work to finish. *)
  type shutdown_progress =
    | Shutdown_open
    | Shutdown_submitted of (unit, error) result Mailbox.pending
    | Shutdown_admission_failed of (unit, error) result
    | Shutdown_finished of (unit, error) result

  (** Shared instance state. [shutdown_mutex] serializes terminal submission,
      joining, and updates to [shutdown_progress]. [shutdown_finished] lets the
      finalizer avoid spawning redundant cleanup after explicit closure. All
      native graph access remains in [mailbox]. *)
  type t = {
    mailbox : Mailbox.t;
    shutdown_mutex : Mutex.t;
    mutable shutdown_progress : shutdown_progress;
    shutdown_finished : bool Atomic.t;
  }

  (** Runs an operation with [mutex] held and always releases it. *)
  let with_mutex mutex operation =
    Mutex.lock mutex;
    Fun.protect ~finally:(fun () -> Mutex.unlock mutex) operation

  (** Converts a mailbox terminal condition to the supervisor contract. *)
  let mailbox_failure = function
    | Mailbox.Closed -> Closed
    | Mailbox.Handler_raised exn -> Supervisor_failed exn

  (** Attempts best-effort graph release after a backend defect, records a
      terminal owner state first, and preserves the original exception. The
      backend contract makes expected shutdown errors release-complete; an
      unexpected shutdown exception is contained here because it must not
      replace the defect which initiated cleanup. *)
  let fail_running state_cell state exn =
    let failure = Error (Supervisor_failed exn) in
    state_cell := Closed_graph failure;
    (match Backend.shutdown state with
    | Ok () | Error _ -> ()
    | exception _ -> ());
    raise exn

  (** Creates the rank-2 owner handler. Its state cell is never read or written
      outside the owner Domain after the mailbox starts. *)
  let owner_handler () =
    let state = ref Not_started in
    let handle : type result. result Request.t -> result = function
      | Initialize config ->
          (match !state with
          | Not_started ->
              (match Backend.create config with
              | Ok backend_state ->
                  state := Running backend_state;
                  Ok ()
              | Error error -> Error (Backend error))
          | Running _ | Closed_graph _ ->
              invalid_arg "SDK supervisor initialized more than once")
      | Perform operation ->
          (match !state with
          | Running backend_state ->
              (match Backend.perform backend_state operation with
              | Ok value -> Ok value
              | Error error -> Error (Backend error)
              | exception exn -> fail_running state backend_state exn)
          | Closed_graph _ -> Error Closed
          | Not_started ->
              invalid_arg "SDK supervisor used before initialization")
      | Shutdown ->
          (match !state with
          | Running backend_state ->
              (match Backend.shutdown backend_state with
              | result ->
                  let result = Result.map_error (fun error -> Backend error) result in
                  state := Closed_graph result;
                  result
              | exception exn ->
                  let result = Error (Supervisor_failed exn) in
                  state := Closed_graph result;
                  raise exn)
          | Closed_graph result -> result
          | Not_started -> Ok ())
    in
    { Mailbox.handle }

  (** Closes and joins a mailbox whose backend graph either was never created
      or has already reached a terminal owner state. *)
  let stop_mailbox mailbox =
    Mailbox.close mailbox;
    Mailbox.join mailbox

  (** Submits one typed operation without exposing backend state. *)
  let perform supervisor operation =
    match Mailbox.call supervisor.mailbox (Perform operation) with
    | Ok result -> result
    | Error failure -> Error (mailbox_failure failure)

  (** Atomically submits the terminal request while the shutdown mutex is held.
      This function does not wait for earlier backend work or join the owner. *)
  let initiate_shutdown_locked supervisor =
    match supervisor.shutdown_progress with
    | Shutdown_open ->
        (match Mailbox.submit_and_close supervisor.mailbox Shutdown with
        | Ok pending ->
            supervisor.shutdown_progress <- Shutdown_submitted pending
        | Error failure ->
            supervisor.shutdown_progress <-
              Shutdown_admission_failed (Error (mailbox_failure failure)))
    | Shutdown_submitted _ | Shutdown_admission_failed _ | Shutdown_finished _ ->
        ()

  (** Closes operation admission synchronously without waiting for backend
      teardown. This seam remains in the private supervisor library so tests
      and future lifecycle orchestration can observe the linearization point. *)
  let initiate_shutdown supervisor =
    with_mutex supervisor.shutdown_mutex (fun () ->
        initiate_shutdown_locked supervisor)

  (** Performs shutdown, closes admissions, and joins exactly once. The mutex
      covers the blocking sequence because concurrent shutdown callers must
      await one admitted terminal request and observe one cached result rather
      than attempt multiple Domain joins. *)
  let shutdown supervisor =
    with_mutex supervisor.shutdown_mutex (fun () ->
        initiate_shutdown_locked supervisor;
        match supervisor.shutdown_progress with
        | Shutdown_submitted pending ->
            let result =
              match Mailbox.await pending with
              | Ok result -> result
              | Error failure -> Error (mailbox_failure failure)
            in
            ignore (Mailbox.join supervisor.mailbox);
            supervisor.shutdown_progress <- Shutdown_finished result;
            Atomic.set supervisor.shutdown_finished true;
            result
        | Shutdown_admission_failed result ->
            ignore (Mailbox.join supervisor.mailbox);
            supervisor.shutdown_progress <- Shutdown_finished result;
            Atomic.set supervisor.shutdown_finished true;
            result
        | Shutdown_finished result -> result
        | Shutdown_open ->
            invalid_arg "SDK supervisor shutdown did not submit a terminal request")

  (** Schedules forgotten-instance cleanup on a system thread. A finalizer must
      not block while waiting for mailbox capacity or the owner Domain. The
      detached thread uses the ordinary serialized shutdown path; if creating
      it is impossible during process teardown, closing the mailbox at least
      releases the owner and permits backend-native finalizers to run. *)
  let cleanup_abandoned supervisor =
    if not (Atomic.get supervisor.shutdown_finished) then
      match
        Thread.create
          (fun instance -> ignore (shutdown instance))
          supervisor
      with
      | _thread -> ()
      | exception _ -> Mailbox.close supervisor.mailbox

  (** Starts the mailbox first so backend construction itself occurs on the
      owner Domain. Failed initialization stops and joins that Domain before
      returning, ensuring no partially published supervisor remains. A
      non-blocking finalizer schedules the same shutdown path as a last-resort
      safeguard for callers which abandon a live instance. *)
  let create ~capacity config =
    let mailbox = Mailbox.create ~capacity ~handler:(owner_handler ()) in
    match Mailbox.call mailbox (Initialize config) with
    | Ok (Ok ()) ->
        let supervisor =
          {
            mailbox;
            shutdown_mutex = Mutex.create ();
            shutdown_progress = Shutdown_open;
            shutdown_finished = Atomic.make false;
          }
        in
        Gc.finalise cleanup_abandoned supervisor;
        Ok supervisor
    | Ok (Error error) ->
        ignore (stop_mailbox mailbox);
        Error error
    | Error failure ->
        ignore (stop_mailbox mailbox);
        Error (mailbox_failure failure)
end

(** The current real backend owns exactly one Rust runtime. *)
module Native_backend = struct
  module Bridge = Temporal_core_bridge.Native_bridge

  type config = unit
  type state = Bridge.runtime
  type error = Bridge.error
  type _ operation = Check_compatibility : unit operation

  (** Creates the runtime through the ownership-safe C stubs. *)
  let create () = Bridge.runtime_create ()

  (** Revalidates the statically linked ABI without exposing the runtime. The
      state argument proves the operation remains ordered with lifecycle use. *)
  let perform : type value. state -> value operation -> (value, error) result =
   fun _runtime -> function
    | Check_compatibility ->
        Bridge.check_abi_version Bridge.abi_version

  (** Explicitly destroys the runtime; the native close operation atomically
      detaches its Rust pointer and is itself idempotent. *)
  let shutdown runtime = Bridge.runtime_close runtime
end

(** Specializes the generic supervisor to the real native runtime. *)
module Native = struct
  include Make (Native_backend)

  type 'result operation = 'result Native_backend.operation =
    | Check_compatibility : unit operation
end
