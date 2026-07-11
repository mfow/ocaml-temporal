(** OCaml names for Rust bridge status codes. An unrecognized number is kept in
    [Unknown] so diagnostics remain available across a version mismatch. *)
type status =
  | Invalid_argument
  | Abi_mismatch
  | Panic
  | Internal
  | Invalid_state
  | Configuration
  | Connection
  | Worker
  | Outstanding_tasks
  | Not_ready
  | Protocol
  | Already_started
  | Unknown of int

(** Error data copied into OCaml. It never owns Rust memory. *)
type error = {
  status : status;
  message : string;
}

(** Shared source and tag vocabulary. It contains reporter exceptions so a
    consumer's logging setup cannot change bridge behavior. *)
module Observability = Temporal_base.Observability

(** Version requested by this binding layer. *)
let abi_version = 1l

(** Private OCaml value implemented in C. It owns a Rust result allocation until
    [decode] frees it or the OCaml garbage collector runs its finalizer. *)
type response

(** Opaque owner of one Temporal Core runtime and its Tokio executor. Only the
    SDK supervisor may use or close it; workflow code never sees this type. *)
type runtime

(** Validated client connection settings. The concrete JSON representation is
    private so callers cannot bypass sender-side checks. *)
type client_config = {
  target_url : string;
  identity : string;
}

(** Validated workflow-only worker settings retained as ordinary OCaml data
    until the supervisor serializes worker construction. *)
type worker_config = {
  namespace : string;
  task_queue : string;
  build_id : string;
  max_cached_workflows : int;
  max_outstanding_workflow_tasks : int;
  max_concurrent_workflow_task_polls : int;
  graceful_shutdown_timeout_ms : int64;
}

(** Private transport-safety ceiling mirrored and revalidated by Rust. This is
    not a Temporal Server identifier policy; Core and Server perform semantic
    field validation. *)
let max_transport_string_bytes = 65_536

(** Resource ceiling mirrored by the Rust worker-config adapter. *)
let max_worker_count = 1_000_000

(** Maximum accepted graceful shutdown period in milliseconds. *)
let max_graceful_shutdown_timeout_ms = 86_400_000L

external check_abi_version_raw : int32 -> response
  = "ocaml_temporal_check_abi_version"

external echo_raw : bytes -> response = "ocaml_temporal_echo"

external conformance_wait_ms_raw : int -> response
  = "ocaml_temporal_conformance_wait_ms"

external response_status : response -> int = "ocaml_temporal_response_status"
external response_value : response -> bytes = "ocaml_temporal_response_value"
external response_error : response -> string = "ocaml_temporal_response_error"
external response_free : response -> unit = "ocaml_temporal_response_free"
external runtime_create_raw : unit -> runtime * response
  = "ocaml_temporal_runtime_create"

external runtime_close_raw : runtime -> int = "ocaml_temporal_runtime_close"

external client_connect_raw : runtime -> bytes -> response
  = "ocaml_temporal_client_connect"

external client_start_workflow_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_client_start_workflow_json"

external client_wait_workflow_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_client_wait_workflow_json"

external worker_start_raw : runtime -> bytes -> response
  = "ocaml_temporal_worker_start"

external worker_try_poll_workflow_raw : runtime -> response
  = "ocaml_temporal_worker_try_poll_workflow"

external worker_wait_workflow_raw : runtime -> response
  = "ocaml_temporal_worker_wait_workflow"

external worker_complete_workflow_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_worker_complete_workflow_json"

external worker_reject_workflow_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_worker_reject_workflow_json"

external worker_try_poll_activity_raw : runtime -> response
  = "ocaml_temporal_worker_try_poll_activity"

external worker_wait_activity_raw : runtime -> response
  = "ocaml_temporal_worker_wait_activity"

external worker_complete_activity_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_worker_complete_activity_json"

external worker_reject_activity_json_raw : runtime -> bytes -> response
  = "ocaml_temporal_worker_reject_activity_json"

external worker_shutdown_raw : runtime -> response
  = "ocaml_temporal_worker_shutdown"

external client_disconnect_raw : runtime -> response
  = "ocaml_temporal_client_disconnect"

(** Converts known numeric statuses and retains every newer value as [Unknown]. *)
let status = function
  | 1 -> Invalid_argument
  | 2 -> Abi_mismatch
  | 3 -> Panic
  | 4 -> Internal
  | 5 -> Invalid_state
  | 6 -> Configuration
  | 7 -> Connection
  | 8 -> Worker
  | 9 -> Outstanding_tasks
  | 10 -> Not_ready
  | 11 -> Protocol
  | 12 -> Already_started
  | code -> Unknown code

(** Converts a bridge status to a bounded stable tag value without exposing the
    Rust-owned diagnostic message. *)
let status_name = function
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
  | Unknown _ -> "unknown"

(** Constructs a local configuration failure without entering native code. *)
let configuration_error message = Error { status = Configuration; message }

(** Validates only bridge-owned string invariants before Core sees the value. *)
let validate_identifier name value =
  if String.length value = 0 then
    configuration_error (name ^ " must not be empty")
  else if String.length value > max_transport_string_bytes then
    configuration_error
      (Printf.sprintf "%s exceeds %d UTF-8 bytes" name
         max_transport_string_bytes)
  else Ok ()

(** Performs the inexpensive sender-side absolute HTTP(S) shape check. Rust's
    URL parser repeats and completes validation before network access. *)
let validate_target_url value =
  let host_after prefix =
    let prefix_length = String.length prefix in
    String.starts_with ~prefix value
    && String.length value > prefix_length
    &&
    let remainder =
      String.sub value prefix_length (String.length value - prefix_length)
    in
    let host =
      match String.index_opt remainder '/' with
      | None -> remainder
      | Some index -> String.sub remainder 0 index
    in
    String.length host > 0
    && not (String.exists (fun character -> Char.code character <= 32) host)
  in
  if String.length value > max_transport_string_bytes then
    configuration_error
      (Printf.sprintf "target_url exceeds %d UTF-8 bytes"
         max_transport_string_bytes)
  else if host_after "http://" || host_after "https://" then Ok ()
  else configuration_error "target_url must be an absolute http or https URL"

(** Validates one bounded count, allowing zero only for disabled cache size. *)
let validate_count ~allow_zero name value =
  let minimum = if allow_zero then 0 else 1 in
  if value < minimum || value > max_worker_count then
    configuration_error
      (Printf.sprintf "%s must be between %d and %d" name minimum
         max_worker_count)
  else Ok ()

(** Creates client settings only after all sender-side invariants hold. *)
let client_config ~target_url ~identity =
  match validate_target_url target_url with
  | Error _ as error -> error
  | Ok () ->
      Result.map
        (fun () -> { target_url; identity })
        (validate_identifier "identity" identity)

(** Creates workflow-only worker settings after validating every field. *)
let worker_config ~namespace ~task_queue ~build_id ~max_cached_workflows
    ~max_outstanding_workflow_tasks ~max_concurrent_workflow_task_polls
    ~graceful_shutdown_timeout_ms =
  let validations =
    [
      validate_identifier "namespace" namespace;
      validate_identifier "task_queue" task_queue;
      validate_identifier "build_id" build_id;
      validate_count ~allow_zero:true "max_cached_workflows"
        max_cached_workflows;
      validate_count ~allow_zero:false "max_outstanding_workflow_tasks"
        max_outstanding_workflow_tasks;
      validate_count ~allow_zero:false "max_concurrent_workflow_task_polls"
        max_concurrent_workflow_task_polls;
      (if
         Int64.compare graceful_shutdown_timeout_ms 0L >= 0
         && Int64.compare graceful_shutdown_timeout_ms
              max_graceful_shutdown_timeout_ms
            <= 0
       then Ok ()
       else
         configuration_error
           "graceful_shutdown_timeout_ms must be between 0 and 86400000");
    ]
  in
  match List.find_opt Result.is_error validations with
  | Some (Error _ as error) -> error
  | Some (Ok ()) -> assert false
  | None ->
      Ok
        {
          namespace;
          task_queue;
          build_id;
          max_cached_workflows;
          max_outstanding_workflow_tasks;
          max_concurrent_workflow_task_polls;
          graceful_shutdown_timeout_ms;
        }

(** Encodes the exact strict client document accepted by the Rust adapter. *)
let encode_client_config config =
  `Assoc
    [
      ("target_url", `String config.target_url);
      ("identity", `String config.identity);
    ]
  |> Yojson.Safe.to_string |> Bytes.of_string

(** Encodes the exact strict workflow-worker document accepted by Rust. *)
let encode_worker_config config =
  `Assoc
    [
      ("namespace", `String config.namespace);
      ("task_queue", `String config.task_queue);
      ("build_id", `String config.build_id);
      ("max_cached_workflows", `Int config.max_cached_workflows);
      ( "max_outstanding_workflow_tasks",
        `Int config.max_outstanding_workflow_tasks );
      ( "max_concurrent_workflow_task_polls",
        `Int config.max_concurrent_workflow_task_polls );
      ( "graceful_shutdown_timeout_ms",
        `Intlit (Int64.to_string config.graceful_shutdown_timeout_ms) );
    ]
  |> Yojson.Safe.to_string |> Bytes.of_string

(** Copies either the successful bytes or error message into OCaml, then always
    frees the Rust allocation. [Fun.protect] still runs cleanup if copying
    raises an OCaml exception. *)
let decode response =
  Fun.protect
    ~finally:(fun () -> response_free response)
    (fun () ->
      let code = response_status response in
      if code = 0 then Ok (response_value response)
      else
        Error
          { status = status code; message = response_error response })

(** Chooses a log level and constant message for each typed bridge status. *)
let bridge_error_log_level = function
  | Not_ready ->
      (* Polling is deliberately non-blocking, so an empty lane is normal
         scheduler state rather than an error that should page an operator. *)
      (Logs.Debug, "bridge operation not ready")
  | Outstanding_tasks ->
      (* Shutdown can be retried after the language side finishes its leased
         work. Keep this visible without classifying it as a bridge failure. *)
      (Logs.Warning, "bridge operation waiting for outstanding tasks")
  | _ ->
      (* Protocol, lifecycle, configuration, and native failures all indicate
         that the requested operation did not complete and need investigation. *)
      (Logs.Error, "bridge operation failed")

(** Measures one complete bridge operation, reports its structural outcome,
    and returns the original [result] unchanged. *)
let bridge_call operation action =
  let result, duration_ms = Observability.measure_ms action in
  let duration_tags = Observability.tags ~operation ~duration_ms () in
  Observability.report ~src:Observability.Source.bridge Logs.Debug
    ~tags:duration_tags "bridge operation completed";
  (match result with
  | Ok _ -> ()
  | Error error ->
      let tags =
        Observability.tags ~operation
          ~bridge_status:(status_name error.status) ()
      in
      let level, message = bridge_error_log_level error.status in
      Observability.report ~src:Observability.Source.bridge level ~tags message);
  result

(** Converts successful test operations with no useful output to [Ok ()] after
    [decode] has performed the normal memory cleanup. *)
let check_abi_version version =
  bridge_call "check_abi_version" (fun () ->
      Result.map (fun _ -> ()) (decode (check_abi_version_raw version)))

let echo input = bridge_call "echo" (fun () -> decode (echo_raw input))

let conformance_wait_ms milliseconds =
  bridge_call "conformance_wait_ms" (fun () ->
      Result.map (fun _ -> ())
        (decode (conformance_wait_ms_raw milliseconds)))

(** Connects the official Temporal client through the Rust-owned runtime. *)
let client_connect runtime config =
  bridge_call "client_connect" (fun () ->
      Result.map (fun _ -> ())
        (decode (client_connect_raw runtime (encode_client_config config))))

(** Starts a workflow through the Rust-owned client. The response or closed
    error document is copied before the Rust allocation is released. *)
let client_start_workflow_json runtime input =
  bridge_call "client_start_workflow_json" (fun () ->
      decode (client_start_workflow_json_raw runtime input))

(** Waits for one exact run through the Rust-owned client. Rust performs the
    long poll while the C binding has released the OCaml runtime lock. *)
let client_wait_workflow_json runtime input =
  bridge_call "client_wait_workflow_json" (fun () ->
      decode (client_wait_workflow_json_raw runtime input))

(** Constructs and namespace-validates the official workflow-only worker. *)
let worker_start runtime config =
  bridge_call "worker_start" (fun () ->
      Result.map (fun _ -> ())
        (decode (worker_start_raw runtime (encode_worker_config config))))

(** Takes one already-ready workflow activation without waiting for Core. The
    [Not_ready] status is an expected result while both poll lanes are empty;
    callers should yield or use the future readiness wait rather than treat it
    as worker failure. The returned bytes are a validated semantic JSON
    document owned by the OCaml heap after [decode] copies it. *)
let worker_try_poll_workflow runtime =
  bridge_call "worker_try_poll_workflow" (fun () ->
      decode (worker_try_poll_workflow_raw runtime))

(** Waits for workflow-lane readiness without consuming the activation. The C
    stub releases the OCaml runtime lock while Rust waits. [Not_ready] means
    the bounded wait elapsed; retry from the supervisor mailbox so lifecycle
    messages remain serviceable. A successful result is only a wake signal, so
    callers must drain with [worker_try_poll_workflow]. *)
let worker_wait_workflow runtime =
  bridge_call "worker_wait_workflow" (fun () ->
      Result.map (fun _ -> ()) (decode (worker_wait_workflow_raw runtime)))

(** Validates and submits one workflow activation completion. The caller must
    use the exact run identifier from a previously leased activation; Rust's
    task ledger rejects unknown or duplicate completions before Core sees them.
    Input bytes are copied by the C stub before the OCaml runtime lock is
    released and are never retained after this call. *)
let worker_complete_workflow_json runtime input =
  bridge_call "worker_complete_workflow_json" (fun () ->
      Result.map (fun _ -> ())
        (decode (worker_complete_workflow_json_raw runtime input)))

(** Returns an activation document produced by Rust when OCaml's semantic
    decoder cannot accept it. Rust reparses and compares the complete value
    with its retained activation before retiring the one-shot lease. *)
let worker_reject_workflow_json runtime input =
  bridge_call "worker_reject_workflow_json" (fun () ->
      Result.map (fun _ -> ())
        (decode (worker_reject_workflow_json_raw runtime input)))

(** Takes one already-ready remote activity task without waiting for Core. The
    returned bytes contain the closed activity-task JSON document; activity
    cancellation remains correlated by its opaque token in that document. *)
let worker_try_poll_activity runtime =
  bridge_call "worker_try_poll_activity" (fun () ->
      decode (worker_try_poll_activity_raw runtime))

(** Waits for remote-activity-lane readiness under the same bounded,
    runtime-lock-free contract as [worker_wait_workflow]. The wake does not
    consume a task; drain it with [worker_try_poll_activity]. *)
let worker_wait_activity runtime =
  bridge_call "worker_wait_activity" (fun () ->
      Result.map (fun _ -> ()) (decode (worker_wait_activity_raw runtime)))

(** Validates and submits one remote activity completion. Rust checks the
    opaque task token against the outstanding ledger before completing Core, so
    a stale or duplicated completion is reported as a typed bridge error. *)
let worker_complete_activity_json runtime input =
  bridge_call "worker_complete_activity_json" (fun () ->
      Result.map (fun _ -> ())
        (decode (worker_complete_activity_json_raw runtime input)))

(** Returns a Rust-produced activity-task document after OCaml decode failure.
    Rust reparses and compares the complete task with retained handoff state;
    only then may its opaque-token obligation be retired. *)
let worker_reject_activity_json runtime input =
  bridge_call "worker_reject_activity_json" (fun () ->
      Result.map (fun _ -> ())
        (decode (worker_reject_activity_json_raw runtime input)))

(** Gracefully closes the worker. Rust treats repetition as success. *)
let worker_shutdown runtime =
  bridge_call "worker_shutdown" (fun () ->
      Result.map (fun _ -> ()) (decode (worker_shutdown_raw runtime)))

(** Drops the connected client after its worker is absent. *)
let client_disconnect runtime =
  bridge_call "client_disconnect" (fun () ->
      Result.map (fun _ -> ()) (decode (client_disconnect_raw runtime)))

(** Closes the native owner after first clearing its OCaml-held pointer. This
    makes repeated sequential calls safe; the production supervisor serializes
    all lifecycle calls across Domains. *)
let runtime_close runtime =
  let result =
    bridge_call "runtime_close" (fun () ->
        match runtime_close_raw runtime with
        | 0 -> Ok ()
        | code ->
            Error
              {
                status = status code;
                message = "Temporal Core runtime close failed";
              })
  in
  let level, message, bridge_status =
    match result with
    | Ok () -> (Logs.Info, "runtime closed", None)
    | Error error ->
        (Logs.Error, "runtime shutdown failed", Some (status_name error.status))
  in
  let tags =
    Observability.tags ~operation:"runtime_close" ?bridge_status ()
  in
  Observability.report ~src:Observability.Source.lifecycle level ~tags message;
  result

(** Checks the linked bridge contract once, then creates the native runtime.
    If creation fails after allocating the OCaml owner, cleanup remains safe
    because its native pointer is either null or explicitly closed here. *)
let runtime_create () =
  let result =
    bridge_call "runtime_create" (fun () ->
        match check_abi_version abi_version with
        | Error _ as error -> error
        | Ok () ->
            let runtime, response = runtime_create_raw () in
            (match decode response with
            | Ok _ -> Ok runtime
            | Error error ->
                ignore (runtime_close runtime);
                Error error))
  in
  let level, message, bridge_status =
    match result with
    | Ok _ -> (Logs.Info, "runtime initialized", None)
    | Error error ->
        (Logs.Error, "runtime initialization failed", Some (status_name error.status))
  in
  let tags =
    Observability.tags ~operation:"runtime_create" ?bridge_status ()
  in
  Observability.report ~src:Observability.Source.lifecycle level ~tags message;
  result
