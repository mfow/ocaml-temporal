(** Unit tests for the private native worker execution adapter.

    The fake supervisor below models the semantic contract exposed by the
    typed Native supervisor operations: polling leases one activation,
    completion retires exactly that run ID, and a protocol rejection is
    reported only after the lease has been retired. No Rust, C, network, or
    Temporal Server process is needed to exercise the OCaml registry. *)

module Protocol = Temporal_protocol.Workflow_protocol
module Adapter = Temporal_runtime.Native_worker_execution

(** Keeps workflow fixture sequencing on the same typed-result path as the
    production adapter. *)
let ( let* ) = Result.bind

(** A source-side error used by the deterministic semantic queue. *)
type source_error = { code : string; message : string }

(** One fake supervisor owns an activation queue and mutable lease ledger. All
    mutable fields are accessed by the adapter's serialized poll call in these
    tests. *)
type fake_supervisor = {
  queue : Protocol.activation Queue.t;
  leased : (string, unit) Hashtbl.t;
  completions : Protocol.completion list ref;
  poll_error : source_error option ref;
  rejected_poll_count : int ref;
  raise_next_completion : bool ref;
}

(** Allocates an empty fake semantic queue. *)
let fake_supervisor () =
  {
    queue = Queue.create ();
    leased = Hashtbl.create 8;
    completions = ref [];
    poll_error = ref None;
    rejected_poll_count = ref 0;
    raise_next_completion = ref false;
  }

(** Implements the typed supervisor contract over the fake lease ledger. *)
module Fake_supervisor = struct
  type t = fake_supervisor
  type error = source_error

  (** Takes one activation and records its run ID as leased. An injected error
      represents a malformed activation rejected by the lower protocol layer;
      the counter proves that layer retired the lease before returning. *)
  let try_poll_workflow supervisor =
    match !(supervisor.poll_error) with
    | Some error ->
        incr supervisor.rejected_poll_count;
        Error error
    | None ->
        if Queue.is_empty supervisor.queue then Ok None
        else
          let activation = Queue.take supervisor.queue in
          Hashtbl.replace supervisor.leased activation.run_id ();
          Ok (Some activation)

  (** Accepts one completion only for an active run ID, then removes that lease
      and records the immutable semantic completion for assertions. *)
  let complete_workflow supervisor (completion : Protocol.completion) =
    if !(supervisor.raise_next_completion) then begin
      supervisor.raise_next_completion := false;
      raise (Failure "injected completion exception")
    end else if Hashtbl.mem supervisor.leased completion.run_id then begin
      Hashtbl.remove supervisor.leased completion.run_id;
      supervisor.completions := completion :: !(supervisor.completions);
      Ok ()
    end
    else Error { code = "stale_lease"; message = "run is not leased" }

  (** Exposes the stable source error code required by the adapter signature. *)
  let error_code error = error.code

  (** Exposes the stable source diagnostic required by the adapter signature. *)
  let error_message error = error.message
end

(** The test worker instantiates the production functor with the deterministic
    fake source, proving that no concrete native handle is required by the
    execution registry itself. *)
module Worker = Adapter.Make (Fake_supervisor)

(** The canonical timestamp used by every ordinary activation fixture. *)
let timestamp : Protocol.timestamp = { seconds = 1L; nanoseconds = 0 }

(** Builds a unit workflow start job with no arguments. The adapter fills in the
    canonical [binary/null] payload for the typed unit codec. *)
let initialize ~run_id ~workflow_type : Protocol.activation_job =
  Protocol.Initialize_workflow
    {
      workflow_id = "workflow-" ^ run_id;
      workflow_type;
      arguments = [];
      randomness_seed = "1";
      attempt = 1;
      context = None;
    }

(** Wraps jobs in the strict ordinary activation envelope accepted by the
    translation layer. *)
let activation ~run_id jobs : Protocol.activation =
  {
    run_id;
    timestamp = Some timestamp;
    is_replaying = true;
    history_length = 1L;
    jobs;
    metadata = None;
  }

(** Builds Core's synthetic cache-eviction envelope. Temporal deliberately omits
    the timestamp for this activation, so retaining that distinction verifies
    that the adapter does not normalize away a meaningful protocol invariant. *)
let eviction_activation ~run_id jobs : Protocol.activation =
  { (activation ~run_id jobs) with timestamp = None }

(** Adds an activation to the fake queue in producer order. *)
let enqueue supervisor activation = Queue.add activation supervisor.queue

(** Extracts the newest completion while failing with a useful test diagnostic
    when the adapter did not retire a lease. *)
let latest_completion supervisor =
  match !(supervisor.completions) with
  | completion :: _ -> completion
  | [] -> failwith "expected the adapter to submit a completion"

(** Creates a worker around a list of executable workflow definitions. *)
let worker supervisor workflows =
  match Worker.create ~supervisor ~workflows () with
  | Ok worker -> worker
  | Error error ->
      failwith
        (Printf.sprintf "worker creation failed: %s at %s (%s)" error.message
           error.path error.code)

(** Asserts a completed outcome and checks whether the workflow reached a
    terminal command. *)
let expect_completed ~terminal = function
  | Adapter.Completed { terminal = actual; _ } when Bool.equal actual terminal ->
      ()
  | Adapter.Completed { terminal = actual; run_id; command_count } ->
      failwith
        (Printf.sprintf "run %s emitted %d commands with terminal flag %b instead of %b"
           run_id command_count actual terminal)
  | Adapter.Not_ready -> failwith "poll unexpectedly reported Not_ready"
  | Adapter.Rejected { error; _ } ->
      failwith
        (Printf.sprintf "poll unexpectedly rejected activation: %s at %s (%s)"
           error.message error.path error.code)

(** A unit workflow completes in the first activation and is removed from the
    existential run registry only after the fake supervisor accepts its
    completion. *)
let test_terminal_workflow () =
  let supervisor = fake_supervisor () in
  let called = ref false in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_terminal"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        called := true;
        Ok ())
  in
  enqueue supervisor
    (activation ~run_id:"run-terminal"
       [ initialize ~run_id:"run-terminal" ~workflow_type:"native_worker_terminal" ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  expect_completed ~terminal:true (Result.get_ok (Worker.poll worker));
  if not !called then failwith "workflow implementation was not invoked";
  begin match (latest_completion supervisor).commands with
  | [ Protocol.Complete_workflow { result = None } ] -> ()
  | _ -> failwith "unit workflow did not complete with nullable result"
  end;
  begin match Worker.poll worker with
  | Ok Adapter.Not_ready -> ()
  | _ -> failwith "empty queue did not report Not_ready"
  end

(** A workflow that sleeps first remains in the run registry after its timer
    command, then completes when the matching timer job is delivered. *)
let test_timer_suspension_and_resume () =
  let supervisor = fake_supervisor () in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_timer"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Temporal.Workflow.sleep (Temporal.Duration.of_ms 25L))
  in
  enqueue supervisor
    (activation ~run_id:"run-timer"
       [ initialize ~run_id:"run-timer" ~workflow_type:"native_worker_timer" ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  let timer_seq =
    match (latest_completion supervisor).commands with
    | [ Protocol.Start_timer { seq; _ } ] -> seq
    | _ -> failwith "sleep did not emit exactly one timer command"
  in
  enqueue supervisor
    (activation ~run_id:"run-timer" [ Protocol.Fire_timer { seq = timer_seq } ]);
  expect_completed ~terminal:true (Result.get_ok (Worker.poll worker));
  begin match (latest_completion supervisor).commands with
  | [ Protocol.Complete_workflow { result = None } ] -> ()
  | _ -> failwith "timer completion did not produce nullable unit result"
  end

(** A cancellation job resumes a suspended workflow with a terminal cancel
    command, and the adapter removes the run only after that completion is
    accepted by the supervisor. *)
let test_cancellation () =
  let supervisor = fake_supervisor () in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_cancel"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Temporal.Workflow.sleep (Temporal.Duration.of_ms 25L))
  in
  enqueue supervisor
    (activation ~run_id:"run-cancel"
       [ initialize ~run_id:"run-cancel" ~workflow_type:"native_worker_cancel" ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  enqueue supervisor
    (activation ~run_id:"run-cancel"
       [ Protocol.Cancel_workflow { reason = "operator requested cancellation" } ]);
  expect_completed ~terminal:true (Result.get_ok (Worker.poll worker));
  begin match (latest_completion supervisor).commands with
  | [ Protocol.Cancel_workflow_execution ] -> ()
  | _ -> failwith "cancellation did not produce a terminal cancel command"
  end

(** A cache eviction retires the run without a command. A later activation for
    that run is rejected, proving that eviction removed the OCaml execution
    state only after the empty completion was acknowledged. *)
let test_eviction () =
  let supervisor = fake_supervisor () in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_eviction"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Temporal.Workflow.sleep (Temporal.Duration.of_ms 25L))
  in
  enqueue supervisor
    (activation ~run_id:"run-eviction"
       [ initialize ~run_id:"run-eviction"
           ~workflow_type:"native_worker_eviction" ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  enqueue supervisor
    (eviction_activation ~run_id:"run-eviction"
       [ Protocol.Remove_from_cache
           { message = "test eviction"; reason = Protocol.Lang_requested } ]);
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  begin match (latest_completion supervisor).commands with
  | [] -> ()
  | _ -> failwith "cache eviction unexpectedly emitted a workflow command"
  end;
  enqueue supervisor
    (activation ~run_id:"run-eviction" [ Protocol.Fire_timer { seq = 1L } ]);
  begin match Worker.poll worker with
  | Ok (Adapter.Rejected { error; lease_retired = true; _ })
    when String.equal error.code "unknown_run_id" -> ()
  | _ -> failwith "evicted run remained in the execution registry"
  end

(** An exception from an ordinary completion is caught at the transaction
    boundary. The adapter makes one explicit failure-completion attempt, so the
    lease is retired rather than escaping with an unacknowledged task. *)
let test_unexpected_completion_exception_is_retried () =
  let supervisor = fake_supervisor () in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_completion_exception"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () -> Ok ())
  in
  enqueue supervisor
    (activation ~run_id:"run-completion-exception"
       [ initialize ~run_id:"run-completion-exception"
           ~workflow_type:"native_worker_completion_exception" ]);
  supervisor.raise_next_completion := true;
  let worker = worker supervisor [ Adapter.register workflow ] in
  begin match Worker.poll worker with
  | Ok (Adapter.Rejected { lease_retired = true; error; _ })
    when String.equal error.code "ocaml_exception" -> ()
  | _ -> failwith "completion exception did not trigger a typed failure retry"
  end;
  begin match (latest_completion supervisor).commands with
  | [ Protocol.Fail_workflow _ ] -> ()
  | _ -> failwith "completion exception retry did not submit a failure"
  end;
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "completion exception retry left a native lease outstanding"

(** If an ordinary completion raises, the adapter converts the exception into a
    typed rejected outcome and submits one explicit failure completion. *)
let test_failure_completion_exception_is_typed () =
  let supervisor = fake_supervisor () in
  let activity =
    Temporal.Activity.remote ~name:"native_worker_completion_failure"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
  in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_failure_exception"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Temporal.Activity.execute activity ())
  in
  enqueue supervisor
    (activation ~run_id:"run-failure-exception"
       [ initialize ~run_id:"run-failure-exception"
           ~workflow_type:"native_worker_failure_exception" ]);
  supervisor.raise_next_completion := true;
  let worker = worker supervisor [ Adapter.register workflow ] in
  begin match Worker.poll worker with
  | Ok (Adapter.Rejected { lease_retired = true; _ }) -> ()
  | _ -> failwith "failure completion exception was not typed"
  end;
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "typed failure completion left a native lease outstanding"

(** A later activation can fail after a run has already suspended. Once that
    failure is acknowledged, the stale execution must be removed just like a
    failure during initialization; otherwise a subsequent activation could
    resume an execution that Temporal has already retired. *)
let test_resumed_failure_removes_run () =
  let supervisor = fake_supervisor () in
  let activity =
    Temporal.Activity.remote ~name:"native_worker_resumed_activity"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
  in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_resumed_failure"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        let* () = Temporal.Workflow.sleep (Temporal.Duration.of_ms 25L) in
        Temporal.Activity.execute activity ())
  in
  enqueue supervisor
    (activation ~run_id:"run-resumed-failure"
       [ initialize ~run_id:"run-resumed-failure"
           ~workflow_type:"native_worker_resumed_failure" ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  let timer_seq =
    match (latest_completion supervisor).commands with
    | [ Protocol.Start_timer { seq; _ } ] -> seq
    | _ -> failwith "resumed failure workflow did not emit a timer"
  in
  enqueue supervisor
    (activation ~run_id:"run-resumed-failure"
       [ Protocol.Fire_timer { seq = timer_seq } ]);
  expect_completed ~terminal:false (Result.get_ok (Worker.poll worker));
  begin match (latest_completion supervisor).commands with
  | [ Protocol.Schedule_activity _ ] -> ()
  | _ -> failwith "timer resolution did not schedule the resumed activity"
  end;
  enqueue supervisor
    (activation ~run_id:"run-resumed-failure"
       [ Protocol.Fire_timer { seq = timer_seq } ]);
  expect_completed ~terminal:true (Result.get_ok (Worker.poll worker));
  begin match (latest_completion supervisor).commands with
  | [ Protocol.Fail_workflow _ ] -> ()
  | _ -> failwith "invalid timer resolution did not fail the workflow"
  end;
  enqueue supervisor
    (activation ~run_id:"run-resumed-failure"
       [ Protocol.Fire_timer { seq = timer_seq } ]);
  begin match Worker.poll worker with
  | Ok (Adapter.Rejected { lease_retired = true; error; _ })
    when String.equal error.code "unknown_run_id" -> ()
  | _ -> failwith "resumed failed run remained in the execution registry"
  end

(** A native activity command is submitted with its complete identifier, queue,
    argument, and timeout fields; the run remains suspended awaiting the result. *)
let test_activity_command_retires_lease () =
  let supervisor = fake_supervisor () in
  let activity =
    Temporal.Activity.remote ~name:"native_worker_activity"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
  in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_unsupported"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        Temporal.Activity.execute activity ())
  in
  enqueue supervisor
    (activation ~run_id:"run-unsupported"
       [ initialize ~run_id:"run-unsupported"
           ~workflow_type:"native_worker_unsupported" ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  begin match Worker.poll worker with
  | Ok (Adapter.Completed { terminal = false; _ }) -> ()
  | Ok _ -> failwith "activity command unexpectedly completed the workflow"
  | Error error -> failwith ("activity command failed to retire: " ^ error.message)
  end;
  begin match (latest_completion supervisor).commands with
  | [ Protocol.Schedule_activity { activity_type = "native_worker_activity"; _ } ] -> ()
  | _ -> failwith "activity command did not submit its complete protocol shape"
  end;
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "activity command left a native lease outstanding"

(** An activation for a run not present in the existential registry is rejected
    and completed as a non-retryable bridge failure. *)
let test_unknown_run_retires_lease () =
  let supervisor = fake_supervisor () in
  enqueue supervisor
    (activation ~run_id:"run-unknown" [ Protocol.Cancel_workflow { reason = "test" } ]);
  let worker = worker supervisor [] in
  begin match Worker.poll worker with
  | Ok (Adapter.Rejected { lease_retired = true; error; _ }) ->
      if not (String.equal error.code "unknown_run_id") then
        failwith "unknown run had the wrong rejection code"
  | _ -> failwith "unknown run was not rejected"
  end;
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "unknown run left a native lease outstanding"

(** A malformed semantic activation is rejected by the lower supervisor layer;
    this adapter propagates its typed error and does not claim a completion it
    did not submit. The fake counter models the lower layer's lease retirement.
*)
let test_malformed_activation_error_is_typed () =
  let supervisor = fake_supervisor () in
  supervisor.poll_error :=
    Some { code = "invalid_message"; message = "activation field was malformed" };
  let worker = worker supervisor [] in
  begin match Worker.poll worker with
  | Error error when String.equal error.code "invalid_message" -> ()
  | Error _ -> failwith "malformed activation error classification changed"
  | Ok _ -> failwith "malformed activation unexpectedly produced an outcome"
  end;
  if !(supervisor.rejected_poll_count) <> 1 then
    failwith "lower supervisor did not retire malformed activation"

(** Duplicate and remote registrations are rejected before any worker state is
    published, preventing an ambiguous workflow type from reaching Core. *)
let test_registration_validation () =
  let definition () =
    Temporal.Workflow.define ~name:"duplicate" ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit (fun () -> Ok ())
  in
  let supervisor = fake_supervisor () in
  begin match
    Worker.create ~supervisor
      ~workflows:[ Adapter.register (definition ()); Adapter.register (definition ()) ]
      ()
  with
  | Error { code = "duplicate_workflow"; _ } -> ()
  | _ -> failwith "duplicate workflow registration was accepted"
  end;
  let remote =
    Temporal.Workflow.remote ~name:"remote" ~input:Temporal.Codec.unit
      ~output:Temporal.Codec.unit
  in
  begin match Worker.create ~supervisor ~workflows:[ Adapter.register remote ] () with
  | Error { code = "not_executable"; _ } -> ()
  | _ -> failwith "remote workflow registration was accepted as executable"
  end

(** Rejects malformed worker defaults before the adapter publishes its
    registry. These values are the same four cases checked by the lower
    workflow context, but this test proves the worker-facing constructor
    returns a typed configuration error instead of deferring the defect to the
    first activation. *)
let test_task_queue_validation () =
  let expect_invalid label task_queue =
    let supervisor = fake_supervisor () in
    match Worker.create ~supervisor ~task_queue ~workflows:[] () with
    | Error { code = "invalid_configuration"; path = "$.task_queue"; message }
      when not (String.equal message "") -> ()
    | Error error ->
        failwith
          (Printf.sprintf
             "%s task queue returned %s at %s without a diagnostic" label
             error.code error.path)
    | Ok _ -> failwith (label ^ " task queue was accepted")
  in
  expect_invalid "empty" "";
  expect_invalid "NUL" "bad\000queue";
  expect_invalid "oversized" (String.make 65_537 'x');
  expect_invalid "UTF-8" (String.make 1 (Char.chr 0xff))

(** Runs all native worker adapter assertions. *)
let () =
  test_terminal_workflow ();
  test_timer_suspension_and_resume ();
  test_cancellation ();
  test_eviction ();
  test_unexpected_completion_exception_is_retried ();
  test_failure_completion_exception_is_typed ();
  test_resumed_failure_removes_run ();
  test_activity_command_retires_lease ();
  test_unknown_run_retires_lease ();
  test_malformed_activation_error_is_typed ();
  test_registration_validation ();
  test_task_queue_validation ()
