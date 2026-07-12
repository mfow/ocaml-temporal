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
      detached thread uses the ordinary serialized shutdown path. If creating
      that thread is impossible, the finalizer runs [shutdown] inline as a last
      resort: blocking the finalizer is preferable to [Mailbox.close] alone,
      which never admits the terminal Backend.shutdown request and would leave
      the native runtime/client/worker graph unreclaimed. *)
  let cleanup_abandoned supervisor =
    if not (Atomic.get supervisor.shutdown_finished) then
      match
        Thread.create
          (fun instance -> ignore (shutdown instance))
          supervisor
      with
      | _thread -> ()
      | exception _ ->
          (* Never block the finalizer Domain waiting on the mailbox owner
             (which may be this Domain). Admit terminal shutdown without
             awaiting, then close so an idle owner can drain and exit. The
             runtime custom-block finalizer remains the last-resort native
             reclaim path if the owner never joins. *)
          (try initiate_shutdown supervisor with _ -> ());
          Mailbox.close supervisor.mailbox

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

(** Converts between OCaml-owned native bytes and the closed semantic protocol
    types used by the workflow runtime. This layer is deliberately pure: it
    does not own native handles, mutate lease state, or perform network I/O. *)
module Protocol_adapter = struct
  module Bridge = Temporal_core_bridge.Native_bridge
  module Client = Temporal_protocol.Client_protocol
  module Workflow = Temporal_protocol.Workflow_protocol
  module Activity = Temporal_protocol.Activity_protocol

  (** Converts a workflow protocol failure to the bridge error vocabulary used
      by the native supervisor. Protocol diagnostics contain only stable code,
      path, and validation prose; source JSON and payload bytes are omitted. *)
  let workflow_error operation error =
    let view = Workflow.error_view error in
    Error
      {
        Bridge.status = Protocol;
        message =
          Printf.sprintf "%s failed: %s at %s: %s" operation view.code view.path
            view.message;
      }

  (** Converts an activity protocol failure without exposing the rejected
      task token, payload data, or original JSON document. *)
  let activity_error operation error =
    let view = Activity.error_view error in
    Error
      {
        Bridge.status = Protocol;
        message =
          Printf.sprintf "%s failed: %s at %s: %s" operation view.code view.path
            view.message;
      }

  (** Strictly validates one workflow activation copied from Rust. *)
  let decode_workflow_activation input =
    match Workflow.decode_activation (Bytes.to_string input) with
    | Ok activation -> Ok activation
    | Error error -> workflow_error "workflow activation decoding" error

  (** Keeps the original protocol failure primary while recording a native
      rejection failure with bounded structural status and diagnostic text. *)
  let rejection_failed (protocol_error : Bridge.error) rejection_error =
    let status =
      match rejection_error.Bridge.status with
      | Invalid_argument -> "invalid_argument"
      | Abi_mismatch -> "abi_mismatch"
      | Panic -> "panic"
      | Internal -> "internal"
      | Invalid_state -> "invalid_state"
      | Configuration -> "configuration"
      | Connection -> "connection"
      | Worker -> "worker"
      | Outstanding_tasks -> "outstanding_tasks"
      | Not_ready -> "not_ready"
      | Protocol -> "protocol"
      | Already_started -> "already_started"
      | Unknown code -> Printf.sprintf "unknown(%d)" code
    in
    {
      protocol_error with
      message =
        Printf.sprintf "%s; native lease rejection failed (%s): %s"
          protocol_error.Bridge.message status rejection_error.Bridge.message;
    }

  (** Converts a nonblocking native workflow poll into an optional typed
      activation. [Not_ready] is the empty-lane state, not a worker failure. *)
  let workflow_poll_result ~reject = function
    | Ok input -> (
        match decode_workflow_activation input with
        | Ok activation -> Ok (Some activation)
        | Error protocol_error -> (
            match reject input with
            | Ok () -> Error protocol_error
            | Error rejection_error ->
                Error (rejection_failed protocol_error rejection_error)))
    | Error { Bridge.status = Not_ready; _ } -> Ok None
    | Error _ as error -> error

  (** Canonically serializes and reparses one workflow completion before it
      can be submitted across the C boundary. *)
  let encode_workflow_completion completion =
    match Workflow.encode_completion completion with
    | Ok output -> Ok (Bytes.of_string output)
    | Error error -> workflow_error "workflow completion encoding" error

  (** Strictly validates one remote activity task copied from Rust. *)
  let decode_activity_task input =
    match Activity.decode_task (Bytes.to_string input) with
    | Ok task -> Ok task
    | Error error -> activity_error "activity task decoding" error

  (** Converts a nonblocking native activity poll into an optional typed task
      while preserving every bridge failure other than an empty lane. *)
  let activity_poll_result ~reject = function
    | Ok input -> (
        match decode_activity_task input with
        | Ok task -> Ok (Some task)
        | Error protocol_error -> (
            match reject input with
            | Ok () -> Error protocol_error
            | Error rejection_error ->
                Error (rejection_failed protocol_error rejection_error)))
    | Error { Bridge.status = Not_ready; _ } -> Ok None
    | Error _ as error -> error

  (** Canonically serializes and reparses one activity completion before it
      can be submitted across the C boundary. *)
  let encode_activity_completion completion =
    match Activity.encode_completion completion with
    | Ok output -> Ok (Bytes.of_string output)
    | Error error -> activity_error "activity completion encoding" error

  (** Converts a client codec diagnostic to the same bounded bridge error
      vocabulary used by worker protocol adapters. The source JSON is never
      copied into the message, so malformed native output cannot leak payload
      data through logs or exceptions. *)
  let client_error operation error =
    let view = Client.error_view error in
    Error
      {
        Bridge.status = Protocol;
        message =
          Printf.sprintf "%s failed: %s at %s: %s" operation view.code
            view.path view.message;
      }

  (** Canonically serializes a typed start request before it reaches the C
      boundary. The native bridge remains the only layer that handles raw
      bytes; callers of this adapter see a typed protocol value. *)
  let encode_client_start_request request =
    match Client.encode_start_request request with
    | Ok output -> Ok (Bytes.of_string output)
    | Error error -> client_error "client start request encoding" error

  (** Canonically serializes a typed exact-run cancellation request before it
      reaches the native bridge. The reason is validated by the codec so a
      malformed operator message cannot cross the C boundary. *)
  let encode_client_cancel_request request =
    match Client.encode_cancel_request request with
    | Ok output -> Ok (Bytes.of_string output)
    | Error error -> client_error "client cancellation request encoding" error

  (** Decodes the opaque ticket returned by native asynchronous-start
      admission. The ticket is bound to [request] before it is published to
      the supervisor caller, so later poll operations cannot mix identities. *)
  let decode_client_start_ticket request = function
    | Ok input -> (
        match
          Client.decode_start_ticket ~request (Bytes.to_string input)
        with
        | Ok ticket -> Ok (Ok ticket)
        | Error error -> client_error "client start ticket decoding" error)
    | Error native_error -> Error native_error

  (** Serializes a previously decoded ticket without exposing its opaque
      native value to this supervisor layer. *)
  let encode_client_start_ticket ticket =
    match Client.encode_start_ticket ticket with
    | Ok output -> Ok (Bytes.of_string output)
    | Error error -> client_error "client start ticket encoding" error

  (** Canonically serializes a typed exact-run wait request before it reaches
      the native bridge. *)
  let encode_client_wait_request request =
    match Client.encode_wait_request request with
    | Ok output -> Ok (Bytes.of_string output)
    | Error error -> client_error "client wait request encoding" error

  (** Converts a malformed native client error document into a privacy-safe
      protocol failure. Native status text is intentionally not included. *)
  let malformed_client_error operation error =
    let view = Client.error_view error in
    Error
      {
        Bridge.status = Protocol;
        message =
          Printf.sprintf "%s returned an invalid client error: %s at %s: %s"
            operation view.code view.path view.message;
      }

  (** Checks that the native numeric status agrees with the structured JSON
      category. This prevents a future bridge regression from turning a
      connection failure into an already-started workflow result or vice
      versa. *)
  let client_error_status = function
    | Client.Already_started _ -> Bridge.Already_started
    | Client.Rpc _ -> Bridge.Connection
    | Client.Protocol _ -> Bridge.Protocol

  (** Decodes and status-checks one structured start failure. Statuses outside
      the client protocol's structured vocabulary remain ordinary bridge
      failures, for example [Invalid_state] before a connection exists. *)
  let decode_client_start_failure request native_error =
    match native_error.Bridge.status with
    | Already_started | Connection | Protocol -> (
        match Client.decode_start_error ~request native_error.message with
        | Error error -> malformed_client_error "client start" error
        | Ok client_error ->
            if native_error.Bridge.status = client_error_status client_error then
              Ok (Error client_error)
            else
              Error
                {
                  Bridge.status = Protocol;
                  message =
                    "client start error status does not match its JSON kind";
                })
    | _ -> Error native_error

  (** Decodes and status-checks one structured exact-run wait failure. A wait
      never returns [already_started], so that category is rejected by the
      operation-specific codec even if a native status is accidentally reused. *)
  let decode_client_wait_failure request native_error =
    match native_error.Bridge.status with
    | Connection | Protocol -> (
        match Client.decode_wait_error ~request native_error.message with
        | Error error -> malformed_client_error "client wait" error
        | Ok client_error ->
            if native_error.Bridge.status = client_error_status client_error then
              Ok (Error client_error)
            else
              Error
                {
                  Bridge.status = Protocol;
                  message =
                    "client wait error status does not match its JSON kind";
                })
    | Already_started ->
        Error
          {
            Bridge.status = Protocol;
            message = "client wait returned an impossible already-started status";
          }
    | _ -> Error native_error

  (** Decodes and status-checks one structured cancellation failure. The
      native status must agree with the closed JSON error kind, and the
      start-only [Already_started] category is rejected by the codec. *)
  let decode_client_cancel_failure native_error =
    match native_error.Bridge.status with
    | Connection | Protocol -> (
        match Client.decode_cancel_error native_error.message with
        | Error error -> malformed_client_error "client cancellation" error
        | Ok client_error ->
            if native_error.Bridge.status = client_error_status client_error then
              Ok (Error client_error)
            else
              Error
                {
                  Bridge.status = Protocol;
                  message =
                    "client cancellation error status does not match its JSON kind";
                })
    | Already_started ->
        Error
          {
            Bridge.status = Protocol;
            message =
              "client cancellation returned an impossible already-started status";
          }
    | _ -> Error native_error

  (** Validates a successful native start response and translates it to the
      typed protocol result. Response identity correlation happens in the
      codec using the original request. *)
  let decode_client_start_result request = function
    | Ok input -> (
        match
          Client.decode_start_response ~request (Bytes.to_string input)
        with
        | Ok response -> Ok (Ok response)
        | Error error -> client_error "client start response decoding" error)
    | Error native_error -> decode_client_start_failure request native_error

  (** Decodes one terminal asynchronous-start outcome. A bounded poll timeout
      becomes [None], while terminal accepted/rejected/unknown values are
      validated against the request retained by the ticket. *)
  let decode_client_start_outcome ticket = function
    | Ok input -> (
        let request = Client.start_ticket_request ticket in
        match
          Client.decode_start_outcome ~request (Bytes.to_string input)
        with
        | Ok outcome -> Ok (Some outcome)
        | Error error -> client_error "client start outcome decoding" error)
    | Error { Bridge.status = Not_ready; _ } -> Ok None
    | Error native_error -> Error native_error

  (** Validates a successful native exact-run response and translates it to
      the typed protocol result. [Not_ready] remains an outer bridge result so
      orchestration code can retry without manufacturing a terminal outcome. *)
  let decode_client_wait_result request = function
    | Ok input -> (
        match Client.decode_wait_response ~request (Bytes.to_string input) with
        | Ok response -> Ok (Ok response)
        | Error error -> client_error "client wait response decoding" error)
    | Error native_error -> decode_client_wait_failure request native_error

  (** Decodes the positive cancellation acknowledgement or a structured
      server failure. An acknowledgement is intentionally represented as
      [unit]: the public caller must wait separately for the terminal outcome. *)
  let decode_client_cancel_result = function
    | Ok input -> (
        match Client.decode_cancel_response (Bytes.to_string input) with
        | Ok _response -> Ok (Ok ())
        | Error error -> client_error "client cancellation response decoding" error)
    | Error native_error -> decode_client_cancel_failure native_error
end

(** The production backend owns one runtime-client-worker graph. Every
    operation is invoked by the one supervisor Domain, so no native handle can
    race another operation or teardown. *)
module Native_backend = struct
  module Bridge = Temporal_core_bridge.Native_bridge
  module Client = Temporal_protocol.Client_protocol

  type config = unit
  type state = Bridge.runtime
  type error = Bridge.error
  type _ operation =
    | Check_compatibility : unit operation
    | Connect_client : Bridge.client_config -> unit operation
    | Client_start_workflow :
        Client.start_request ->
        (Client.start_response, Client.client_error) result operation
    | Client_begin_start_workflow :
        Client.start_request ->
        (Client.start_ticket, Client.client_error) result operation
    | Client_poll_start_workflow :
        Client.start_ticket -> Client.start_outcome option operation
    | Client_wait_start_workflow :
        Client.start_ticket -> Client.start_outcome option operation
    | Client_wait_workflow :
        Client.wait_request ->
        (Client.wait_response, Client.client_error) result operation
    | Client_cancel_workflow :
        Client.cancel_request ->
        (unit, Client.client_error) result operation
    | Start_worker : Bridge.worker_config -> unit operation
    | Try_poll_workflow :
        Temporal_protocol.Workflow_protocol.activation option operation
    | Wait_workflow : unit operation
    | Complete_workflow :
        Temporal_protocol.Workflow_protocol.completion -> unit operation
    | Try_poll_activity :
        Temporal_protocol.Activity_protocol.task option operation
    | Wait_activity : unit operation
    | Complete_activity :
        Temporal_protocol.Activity_protocol.completion -> unit operation
    | Shutdown_worker : unit operation
    | Disconnect_client : unit operation

  (** Creates the runtime through the ownership-safe C stubs. *)
  let create () = Bridge.runtime_create ()

  (** Revalidates the statically linked ABI without exposing the runtime. The
      state argument proves the operation remains ordered with lifecycle use. *)
  let perform : type value. state -> value operation -> (value, error) result =
   fun runtime -> function
    | Check_compatibility ->
        Bridge.check_abi_version Bridge.abi_version
    | Connect_client config -> Bridge.client_connect runtime config
    | Client_start_workflow request -> (
        match Protocol_adapter.encode_client_start_request request with
        | Error error -> Error error
        | Ok input ->
            Protocol_adapter.decode_client_start_result request
              (Bridge.client_start_workflow_json runtime input))
    | Client_begin_start_workflow request -> (
        match Protocol_adapter.encode_client_start_request request with
        | Error error -> Error error
        | Ok input ->
            Protocol_adapter.decode_client_start_ticket request
              (Bridge.client_begin_start_workflow_json runtime input))
    | Client_poll_start_workflow ticket -> (
        match Protocol_adapter.encode_client_start_ticket ticket with
        | Error error -> Error error
        | Ok input ->
            Protocol_adapter.decode_client_start_outcome ticket
              (Bridge.client_poll_start_workflow_json runtime input))
    | Client_wait_start_workflow ticket -> (
        match Protocol_adapter.encode_client_start_ticket ticket with
        | Error error -> Error error
        | Ok input ->
            Protocol_adapter.decode_client_start_outcome ticket
              (Bridge.client_wait_start_workflow_json runtime input))
    | Client_wait_workflow request -> (
        match Protocol_adapter.encode_client_wait_request request with
        | Error error -> Error error
        | Ok input ->
            Protocol_adapter.decode_client_wait_result request
              (Bridge.client_wait_workflow_json runtime input))
    | Client_cancel_workflow request -> (
        match Protocol_adapter.encode_client_cancel_request request with
        | Error error -> Error error
        | Ok input ->
            Protocol_adapter.decode_client_cancel_result
              (Bridge.client_cancel_workflow_json runtime input))
    | Start_worker config -> Bridge.worker_start runtime config
    | Try_poll_workflow ->
        Protocol_adapter.workflow_poll_result
          ~reject:(Bridge.worker_reject_workflow_json runtime)
          (Bridge.worker_try_poll_workflow runtime)
    | Wait_workflow -> Bridge.worker_wait_workflow runtime
    | Complete_workflow completion ->
        Result.bind
          (Protocol_adapter.encode_workflow_completion completion)
          (Bridge.worker_complete_workflow_json runtime)
    | Try_poll_activity ->
        Protocol_adapter.activity_poll_result
          ~reject:(Bridge.worker_reject_activity_json runtime)
          (Bridge.worker_try_poll_activity runtime)
    | Wait_activity -> Bridge.worker_wait_activity runtime
    | Complete_activity completion ->
        Result.bind
          (Protocol_adapter.encode_activity_completion completion)
          (Bridge.worker_complete_activity_json runtime)
    | Shutdown_worker -> Bridge.worker_shutdown runtime
    | Disconnect_client -> Bridge.client_disconnect runtime

  (** Requests reverse-order child teardown and always closes the parent graph.
      Runtime close is itself defensive and reclaims any child remaining after
      an earlier error; the first diagnostic is preserved for the caller. *)
  let shutdown runtime =
    let worker_result = Bridge.worker_shutdown runtime in
    let client_result =
      match worker_result with
      | Ok () -> Bridge.client_disconnect runtime
      | Error _ as error -> error
    in
    let runtime_result = Bridge.runtime_close runtime in
    match (worker_result, client_result, runtime_result) with
    | Error _ as error, _, _ -> error
    | Ok (), (Error _ as error), _ -> error
    | Ok (), Ok (), result -> result
end

(** Specializes the generic supervisor to the real native runtime. *)
module Native = struct
  include Make (Native_backend)

  module Protocol_adapter = Protocol_adapter
  module Client = Temporal_protocol.Client_protocol

  type client_config = Native_backend.Bridge.client_config
  type worker_config = Native_backend.Bridge.worker_config

  type 'result operation = 'result Native_backend.operation =
    | Check_compatibility : unit operation
    | Connect_client : client_config -> unit operation
    | Client_start_workflow :
        Client.start_request ->
        (Client.start_response, Client.client_error) result operation
    | Client_begin_start_workflow :
        Client.start_request ->
        (Client.start_ticket, Client.client_error) result operation
    | Client_poll_start_workflow :
        Client.start_ticket -> Client.start_outcome option operation
    | Client_wait_start_workflow :
        Client.start_ticket -> Client.start_outcome option operation
    | Client_wait_workflow :
        Client.wait_request ->
        (Client.wait_response, Client.client_error) result operation
    | Client_cancel_workflow :
        Client.cancel_request ->
        (unit, Client.client_error) result operation
    | Start_worker : worker_config -> unit operation
    | Try_poll_workflow :
        Temporal_protocol.Workflow_protocol.activation option operation
    | Wait_workflow : unit operation
    | Complete_workflow :
        Temporal_protocol.Workflow_protocol.completion -> unit operation
    | Try_poll_activity :
        Temporal_protocol.Activity_protocol.task option operation
    | Wait_activity : unit operation
    | Complete_activity :
        Temporal_protocol.Activity_protocol.completion -> unit operation
    | Shutdown_worker : unit operation
    | Disconnect_client : unit operation

  let client_config = Native_backend.Bridge.client_config
  let worker_config = Native_backend.Bridge.worker_config
end
