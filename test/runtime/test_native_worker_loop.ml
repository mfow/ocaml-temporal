(** Regression tests for the private native worker lane scheduler.

    These tests use callbacks instead of a native worker so they can make the
    liveness boundary deterministic: one activity completion rejection is
    followed by a bounded activity wait, then the exact same completion is
    accepted and the loop can observe a subsequent task. A permanent protocol
    error is kept fatal, proving that retry support cannot hide malformed or
    unsafe boundary data. *)

module Loop = Temporal_runtime.Native_worker_loop

(** A small diagnostic type is enough for the scheduler because it does not
    inspect error contents. The [retryable] bit is produced by the adapter's
    explicit source classification, never inferred from message text. *)
type source_error = { code : string; retryable : bool }

(** A fake source records lane calls and exposes a single transient completion
    rejection. [closed] is set only after the retry succeeds so the test proves
    that the loop actually performed the retry before stopping. *)
type fake_source = {
  mutable activity_polls : int;
  mutable activity_rejections : int;
  mutable waits : bool list;
  mutable closed : bool;
}

(** Creates the deterministic source used by the success-after-retry test. *)
let fake_source () =
  { activity_polls = 0; activity_rejections = 1; waits = []; closed = false }

(** The workflow lane has no work in this focused test. *)
let idle_workflow () = Ok Loop.Not_ready

(** The first activity poll reports a retained completion rejection. The second
    poll accepts that exact completion, and the third poll represents a
    subsequent activity task. Only after that subsequent task does the fixture
    close, proving that retry does not strand the worker loop. *)
let activity_after_one_rejection source () =
  source.activity_polls <- source.activity_polls + 1;
  if source.activity_rejections > 0 then begin
    source.activity_rejections <- source.activity_rejections - 1;
    (* The adapter has already classified and retained the exact completion;
       the scheduler receives a scheduling outcome rather than a fatal error. *)
    Ok Loop.Retry_pending
  end
  else if source.activity_polls = 2 then
    Ok Loop.Progress
  else begin
    source.closed <- true;
    Ok Loop.Progress
  end

(** Records the requested lane without sleeping. Production supplies a native
    bounded readiness wait; the fake only proves that the retry selects the
    activity lane rather than spinning or waiting on workflow readiness. *)
let record_wait source ~workflow_lane =
  source.waits <- workflow_lane :: source.waits;
  Ok ()

(** Proves one transient completion rejection is retried after exactly one
    bounded activity wait and then allows the worker loop to finish normally. *)
let test_transient_completion_retries_and_progresses () =
  let source = fake_source () in
  begin match
    Loop.run ~closed:(fun () -> source.closed)
      ~poll_workflow:idle_workflow
      ~poll_activity:(activity_after_one_rejection source)
      ~wait_for_lane:(record_wait source)
  with
  | Ok () -> ()
  | Error _ -> failwith "transient completion rejection stopped worker loop"
  end;
  if source.activity_polls <> 3 then
    failwith
      "worker loop did not retry the retained completion and then process a subsequent task";
  begin match source.waits with
  | [ false ] -> ()
  | _ -> failwith "worker loop did not apply bounded activity-lane backoff"
  end

(** A permanent/protocol failure must stop the loop immediately. In
    particular, the scheduler must not reinterpret a protocol error as a
    transport retry merely because both errors use the same result channel. *)
let test_permanent_protocol_error_remains_fatal () =
  let waits = ref [] in
  let error = { code = "protocol"; retryable = false } in
  begin match
    Loop.run ~closed:(fun () -> false)
      ~poll_workflow:idle_workflow
      ~poll_activity:(fun () -> Error error)
      ~wait_for_lane:(fun ~workflow_lane ->
        waits := workflow_lane :: !waits;
        Ok ())
  with
  | Error returned
    when returned.code = error.code && returned.retryable = error.retryable ->
      ()
  | Error _ -> failwith "permanent protocol error was rewritten"
  | Ok () -> failwith "permanent protocol error did not stop worker loop"
  end;
  if !waits <> [] then
    failwith "permanent protocol error incorrectly entered retry wait"

(** Runs the focused scheduler regressions. *)
let () =
  test_transient_completion_retries_and_progresses ();
  test_permanent_protocol_error_remains_fatal ()
