(** Unit tests for the private native activity execution adapter.

    The fake supervisor models only the typed semantic boundary: it leases one
    decoded task, accepts a completion for the exact opaque token, and retains
    the lease when completion transport is deliberately rejected. These tests
    therefore exercise codec ownership, typed dispatch, cancellation, failure
    completion, and retry without requiring Rust, C, a network, or Temporal
    Server. *)

module Protocol = Temporal_protocol.Activity_protocol
module Adapter = Temporal_runtime.Native_activity_execution

type source_error = { code : string; message : string }
(** A deterministic source error used by the fake supervisor. *)

type fake_supervisor = {
  queue : Protocol.task Queue.t;
  leased : bytes list ref;
  completions : Protocol.completion list ref;
  reject_next_completion : bool ref;
  poll_error : source_error option ref;
}
(** Mutable fake-supervisor state. The adapter itself serializes access to all
    fields through its poll mutex; assertions inspect them only after a poll
    returns. *)

(** Allocates an empty semantic task queue and lease ledger. *)
let fake_supervisor () =
  {
    queue = Queue.create ();
    leased = ref [];
    completions = ref [];
    reject_next_completion = ref false;
    poll_error = ref None;
  }

(** Copies a validated completion by traversing the same strict JSON semantic
    codec used at the Rust boundary. The fake therefore never retains a mutable
    token or payload buffer owned by the adapter. *)
let copy_completion completion =
  match Protocol.encode_completion completion with
  | Error _ -> failwith "adapter submitted an invalid completion to fake source"
  | Ok json -> (
      match Protocol.decode_completion json with
      | Ok value -> value
      | Error _ -> failwith "fake source could not reparse its own completion")

(** Removes one exact opaque token from a list and reports whether it was found.
    [Bytes.equal] is intentional: tokens are binary correlation data, not text.
*)
let remove_token token tokens =
  let rec loop reversed = function
    | [] -> (false, List.rev reversed)
    | current :: rest when Bytes.equal current token ->
        (true, List.rev_append reversed rest)
    | current :: rest -> loop (current :: reversed) rest
  in
  loop [] tokens

(** Implements the typed supervisor contract over the deterministic queue. *)
module Fake_supervisor = struct
  type t = fake_supervisor
  type error = source_error

  (** Takes one queued task and records an owned copy of its token as leased. *)
  let try_poll_activity supervisor =
    match !(supervisor.poll_error) with
    | Some error -> Error error
    | None ->
        if Queue.is_empty supervisor.queue then Ok None
        else
          let task = Queue.take supervisor.queue in
          supervisor.leased :=
            Bytes.copy task.task_token :: !(supervisor.leased);
          Ok (Some task)

  (** Accepts a completion only for a currently leased exact token. An injected
      rejection leaves the native lease untouched so the adapter must retry the
      same completion without invoking the OCaml implementation again. *)
  let complete_activity supervisor (completion : Protocol.completion) =
    if !(supervisor.reject_next_completion) then begin
      supervisor.reject_next_completion := false;
      Error
        {
          code = "temporarily_unavailable";
          message = "completion transport unavailable";
        }
    end
    else
      let found, remaining =
        remove_token completion.Protocol.task_token !(supervisor.leased)
      in
      if not found then
        Error { code = "stale_lease"; message = "activity token is not leased" }
      else begin
        supervisor.leased := remaining;
        supervisor.completions :=
          copy_completion completion :: !(supervisor.completions);
        Ok ()
      end

  (** Exposes the bounded source classification expected by the adapter. *)
  let error_code error = error.code

  (** Exposes the bounded source message expected by the adapter. *)
  let error_message error = error.message
end

module Worker = Adapter.Make (Fake_supervisor)
(** The production functor is tested against the deterministic source, without
    coupling the tests to a concrete native handle or transport. *)

(** Converts an ordinary codec payload into the binary metadata representation
    consumed by [Activity_protocol]. *)
let protocol_payload (payload : Temporal.Payload.t) : Protocol.payload =
  {
    Protocol.metadata =
      List.map
        (fun (key, value) -> (key, Bytes.of_string value))
        payload.metadata;
    data = Bytes.copy payload.data;
  }

(** Encodes one OCaml value with the requested codec and converts it into a task
    payload. Fixtures fail immediately if a public codec violates its own
    contract. *)
let encode_input codec value =
  match Temporal.Codec.encode codec value with
  | Ok payload -> protocol_payload payload
  | Error error -> failwith (Temporal.Error.message error)

(** Decodes an activity completion payload with a public codec after converting
    binary metadata back to the runtime string representation. *)
let decode_output codec (payload : Protocol.payload) =
  let runtime_payload : Temporal.Payload.t =
    {
      metadata =
        List.map
          (fun (key, value) -> (key, Bytes.to_string value))
          payload.metadata;
      data = Bytes.copy payload.data;
    }
  in
  match Temporal.Codec.decode codec runtime_payload with
  | Ok value -> value
  | Error error -> failwith (Temporal.Error.message error)

(** Constructs the complete start-task context required by the strict protocol.
    Optional timing/retry fields stay absent because dispatch tests focus on the
    activity type, token, input, and completion semantics. *)
let start_task ~token ~activity_type ~input : Protocol.task =
  let start : Protocol.activity_start =
    {
      workflow_namespace = "default";
      workflow_type = "test_workflow";
      workflow_execution =
        { Protocol.workflow_id = "workflow-1"; run_id = "run-1" };
      activity_id = "activity-1";
      activity_type;
      header_fields = [];
      input;
      heartbeat_details = [];
      scheduled_time = None;
      current_attempt_scheduled_time = None;
      started_time = None;
      attempt = 1L;
      schedule_to_close_timeout = None;
      start_to_close_timeout = None;
      heartbeat_timeout = None;
      retry_policy = None;
      priority = None;
      standalone_run_id = "";
    }
  in
  { Protocol.task_token = Bytes.copy token; variant = Start start }

(** Constructs a cancellation task while retaining arbitrary binary token bytes,
    including values that are not valid UTF-8 text. *)
let cancel_task token : Protocol.task =
  {
    Protocol.task_token = Bytes.copy token;
    variant =
      Cancel
        {
          reason = Cancellation_requested;
          details =
            Some
              {
                is_not_found = false;
                is_cancelled = true;
                is_paused = false;
                is_timed_out = false;
                is_worker_shutdown = false;
                is_reset = false;
              };
        };
  }

(** Adds a task in producer order to the fake supervisor queue. *)
let enqueue supervisor task = Queue.add task supervisor.queue

(** Creates a worker and turns configuration failures into a test diagnostic. *)
let worker supervisor activities =
  match Worker.create ~supervisor ~activities with
  | Ok worker -> worker
  | Error (error : Adapter.error_view) ->
      failwith
        (Printf.sprintf "worker creation failed: %s at %s (%s)" error.message
           error.path error.code)

(** Returns the most recent completion or fails with a useful lease diagnostic.
*)
let latest_completion supervisor =
  match !(supervisor.completions) with
  | completion :: _ -> completion
  | [] -> failwith "expected the activity adapter to submit a completion"

(** Requires one successful outcome of the expected kind. *)
let expect_completed expected_kind = function
  | Ok (Adapter.Completed { kind; _ }) when kind = expected_kind -> ()
  | Ok (Adapter.Completed _) -> failwith "activity completion kind differed"
  | Ok Adapter.Not_ready ->
      failwith "activity poll unexpectedly reported Not_ready"
  | Ok (Adapter.Rejected { error; _ }) ->
      failwith ("activity task was rejected: " ^ error.message)
  | Error (error : Adapter.error_view) ->
      failwith
        (Printf.sprintf "activity poll failed: %s at %s (%s)" error.message
           error.path error.code)

(** A successful typed activity receives decoded input, returns encoded output,
    and retires the exact binary task token. *)
let test_successful_dispatch () =
  let supervisor = fake_supervisor () in
  let calls = ref 0 in
  let activity =
    Temporal.Activity.define ~name:"native_activity_upper"
      ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun input ->
        incr calls;
        Ok (String.uppercase_ascii input))
  in
  let token = Bytes.of_string "\000opaque\255-token" in
  enqueue supervisor
    (start_task ~token ~activity_type:"native_activity_upper"
       ~input:[ encode_input Temporal.Codec.string "hello" ]);
  let worker = worker supervisor [ Adapter.register activity ] in
  expect_completed Adapter.Succeeded (Worker.poll worker);
  if !calls <> 1 then
    failwith "activity implementation was not invoked exactly once";
  if !(supervisor.leased) <> [] then
    failwith "successful activity lease remained active";
  let completion = latest_completion supervisor in
  if not (Bytes.equal completion.Protocol.task_token token) then
    failwith "completion did not preserve the exact opaque task token";
  begin match completion.Protocol.result with
  | Protocol.Completed (Some payload) ->
      if decode_output Temporal.Codec.string payload <> "HELLO" then
        failwith "activity output was not decoded from the completion payload"
  | _ -> failwith "successful activity used a non-completed result variant"
  end;
  begin match Worker.poll worker with
  | Ok Adapter.Not_ready -> ()
  | _ -> failwith "empty activity queue did not report Not_ready"
  end

(** A typed [Error.t] from an activity becomes a structured application failure,
    preserves its retryability flag, and is acknowledged without raising an
    exception. *)
let test_typed_failure () =
  let supervisor = fake_supervisor () in
  let activity =
    Temporal.Activity.define ~name:"native_activity_failure"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Error
          (Temporal_base.Error.make ~category:`Activity
             ~message:"deliberate activity failure" ()))
  in
  let token = Bytes.of_string "failure-token" in
  enqueue supervisor
    (start_task ~token ~activity_type:"native_activity_failure"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  let worker = worker supervisor [ Adapter.register activity ] in
  begin match Worker.poll worker with
  | Ok (Adapter.Rejected { lease_retired = true; error; _ })
    when String.equal error.code "activity" ->
      ()
  | _ ->
      failwith "typed activity failure was not reported as a retired rejection"
  end;
  begin match (latest_completion supervisor).Protocol.result with
  | Protocol.Failed
      { info = Protocol.Application { non_retryable = false; _ }; _ } ->
      ()
  | _ ->
      failwith
        "typed activity failure did not preserve application retryability"
  end

(** An unknown activity type is acknowledged with a typed non-retryable failure,
    preventing a leased task from being silently abandoned. *)
let test_unknown_activity_retires_lease () =
  let supervisor = fake_supervisor () in
  let token = Bytes.of_string "unknown-token" in
  enqueue supervisor
    (start_task ~token ~activity_type:"missing_activity" ~input:[]);
  let worker = worker supervisor [] in
  begin match Worker.poll worker with
  | Ok
      (Adapter.Rejected
         {
           activity_type = Some "missing_activity";
           lease_retired = true;
           error;
           _;
         })
    when String.equal error.code "unknown_activity_type" ->
      ()
  | _ -> failwith "unknown activity was not rejected with a retired lease"
  end;
  if !(supervisor.leased) <> [] then
    failwith "unknown activity lease remained active"

(** Cancellation is represented as a canceled Temporal failure and preserves the
    same opaque token even when no activity implementation is registered. *)
let test_cancellation () =
  let supervisor = fake_supervisor () in
  let token = Bytes.of_string "\000cancel\255" in
  enqueue supervisor (cancel_task token);
  let worker = worker supervisor [] in
  expect_completed Adapter.Cancelled (Worker.poll worker);
  if !(supervisor.leased) <> [] then
    failwith "cancellation lease remained active";
  let completion = latest_completion supervisor in
  if not (Bytes.equal completion.Protocol.task_token token) then
    failwith "cancellation completion changed its opaque task token";
  begin match completion.Protocol.result with
  | Protocol.Cancelled { info = Protocol.Canceled { identity; _ }; _ }
    when String.equal identity "ocaml-temporal" ->
      ()
  | _ -> failwith "cancellation did not produce a canceled failure"
  end

(** A completion transport rejection leaves one exact pending completion. The
    next poll retries it and does not execute the implementation a second time.
*)
let test_completion_retry_does_not_redo_activity () =
  let supervisor = fake_supervisor () in
  let calls = ref 0 in
  let activity =
    Temporal.Activity.define ~name:"native_activity_retry"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.string (fun () ->
        incr calls;
        Ok "once")
  in
  let token = Bytes.of_string "retry-token" in
  enqueue supervisor
    (start_task ~token ~activity_type:"native_activity_retry"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  supervisor.reject_next_completion := true;
  let worker = worker supervisor [ Adapter.register activity ] in
  begin match Worker.poll worker with
  | Error { code = "completion_failed"; _ } -> ()
  | _ -> failwith "completion transport rejection did not remain a typed error"
  end;
  if !calls <> 1 then failwith "failed completion unexpectedly reran activity";
  if !(supervisor.leased) = [] then
    failwith "fake source retired rejected lease";
  expect_completed Adapter.Succeeded (Worker.poll worker);
  if !calls <> 1 then failwith "pending completion retry reran activity";
  if List.length !(supervisor.completions) <> 1 then
    failwith "pending completion retry submitted more than one completion";
  if !(supervisor.leased) <> [] then
    failwith "retried completion left lease active"

(** More than one input payload is rejected explicitly rather than silently
    dropping later values. *)
let test_extra_input_is_rejected () =
  let supervisor = fake_supervisor () in
  let activity =
    Temporal.Activity.define ~name:"native_activity_one_input"
      ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun input ->
        Ok input)
  in
  enqueue supervisor
    (start_task
       ~token:(Bytes.of_string "extra-input")
       ~activity_type:"native_activity_one_input"
       ~input:
         [
           encode_input Temporal.Codec.string "first";
           encode_input Temporal.Codec.string "second";
         ]);
  let worker = worker supervisor [ Adapter.register activity ] in
  begin match Worker.poll worker with
  | Ok (Adapter.Rejected { lease_retired = true; error; _ })
    when String.equal error.code "unsupported" ->
      ()
  | _ -> failwith "extra activity inputs were not rejected"
  end

(** Duplicate and remote registrations are rejected before any native task is
    polled, keeping the registry unambiguous. *)
let test_registration_validation () =
  let supervisor = fake_supervisor () in
  let definition () =
    Temporal.Activity.define ~name:"duplicate_activity"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () -> Ok ())
  in
  begin match
    Worker.create ~supervisor
      ~activities:
        [ Adapter.register (definition ()); Adapter.register (definition ()) ]
  with
  | Error { code = "duplicate_activity"; _ } -> ()
  | _ -> failwith "duplicate activity registration was accepted"
  end;
  let remote =
    Temporal.Activity.remote ~name:"remote_activity" ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit
  in
  begin match
    Worker.create ~supervisor ~activities:[ Adapter.register remote ]
  with
  | Error { code = "not_executable"; _ } -> ()
  | _ -> failwith "remote activity registration was accepted as executable"
  end

(** Exceptions from application activity code are converted into a typed,
    retired non-retryable failure instead of escaping the worker loop. *)
let test_implementation_exception_is_retired () =
  let supervisor = fake_supervisor () in
  let activity =
    Temporal.Activity.define ~name:"native_activity_exception"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        raise (Failure "defect in activity"))
  in
  enqueue supervisor
    (start_task
       ~token:(Bytes.of_string "exception-token")
       ~activity_type:"native_activity_exception"
       ~input:[ encode_input Temporal.Codec.unit () ]);
  let worker = worker supervisor [ Adapter.register activity ] in
  begin match Worker.poll worker with
  | Ok (Adapter.Rejected { lease_retired = true; error; _ })
    when String.equal error.code "ocaml_exception" ->
      ()
  | _ -> failwith "activity exception did not retire its task lease"
  end

(** A source-side poll failure remains a typed adapter error because no task
    token was available to acknowledge. *)
let test_poll_error_is_typed () =
  let supervisor = fake_supervisor () in
  supervisor.poll_error :=
    Some { code = "poll_failed"; message = "native activity poll failed" };
  let worker = worker supervisor [] in
  begin match Worker.poll worker with
  | Error (error : Adapter.error_view)
    when String.equal error.code "poll_failed"
         && String.equal error.path "$.poll" ->
      ()
  | Error _ -> failwith "poll error had the wrong typed diagnostic"
  | Ok _ -> failwith "poll error unexpectedly produced an activity outcome"
  end

(** Runs every adapter assertion with a stable test-process failure. *)
let () =
  test_successful_dispatch ();
  test_typed_failure ();
  test_unknown_activity_retires_lease ();
  test_cancellation ();
  test_completion_retry_does_not_redo_activity ();
  test_extra_input_is_rejected ();
  test_registration_validation ();
  test_implementation_exception_is_retired ();
  test_poll_error_is_typed ()
