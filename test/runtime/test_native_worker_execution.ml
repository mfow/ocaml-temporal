(** Unit tests for the private native worker execution adapter.

    The fake supervisor below models the semantic contract exposed by the
    typed Native supervisor operations: polling leases one activation,
    completion retires exactly that run ID, and a protocol rejection is
    reported only after the lease has been retired. No Rust, C, network, or
    Temporal Server process is needed to exercise the OCaml registry. *)

module Protocol = Temporal_protocol.Workflow_protocol
module Raw_adapter = Temporal_runtime.Native_worker_execution

(** Copies a public payload into the base representation consumed by the
    private worker execution adapter. Keeping this conversion in the fixture
    makes the public package boundary visible in a low-level runtime test. *)
let base_payload (payload : Temporal.Payload.t) : Temporal_base.Payload.t =
  {
    Temporal_base.Payload.metadata = List.map (fun (key, value) -> (key, value)) payload.metadata;
    data = Bytes.copy payload.data;
  }

(** Converts a public structured error to the base error representation used by
    the native execution registry. *)
let base_error (error : Temporal.Error.t) : Temporal_base.Error.t =
  let view = Temporal.Error.view error in
  Temporal_base.Error.make ~non_retryable:view.non_retryable
    ~details:(List.map base_payload view.details) ~category:view.category
    ~message:view.message ()

(** Installs public codec callbacks in a base codec without rewriting their
    encoding metadata. The adapter therefore preserves codecs such as option. *)
let base_codec (codec : 'a Temporal.Codec.t) : 'a Temporal_base.Codec.t =
  Temporal_base.Codec.of_payload
    ~encode:(fun value ->
      match Temporal.Codec.encode codec value with
      | Ok payload -> Ok (base_payload payload)
      | Error error -> Error (base_error error))
    ~decode:(fun payload ->
      let public_payload : Temporal.Payload.t =
        {
          Temporal.Payload.metadata =
            List.map (fun (key, value) -> (key, value)) payload.metadata;
          data = Bytes.copy payload.data;
        }
      in
      match Temporal.Codec.decode codec public_payload with
      | Ok value -> Ok value
      | Error error -> Error (base_error error))

(** Rebuilds a public workflow as the private base definition accepted by the
    native worker registry. Public implementation errors are converted only at
    this test boundary, matching the production adapter's ownership rule. *)
let base_workflow (definition : ('input, 'output) Temporal.Workflow.t) =
  let implementation =
    Option.map
      (fun implementation input ->
        Result.map_error base_error (implementation input))
      (Temporal.Workflow.implementation definition)
  in
  Temporal_base.Definition.make ~name:(Temporal.Workflow.name definition)
    ~input:(base_codec (Temporal.Workflow.input definition))
    ~output:(base_codec (Temporal.Workflow.output definition)) ~implementation

(** Keeps workflow fixture registration readable while making the public-to-base
    conversion explicit at the private adapter boundary. *)
module Adapter = struct
  include Raw_adapter

  let register definition = Raw_adapter.register (base_workflow definition)
end

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
  reject_next_completion : bool ref;
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
    reject_next_completion = ref false;
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
    end else if !(supervisor.reject_next_completion) then begin
      supervisor.reject_next_completion := false;
      Error { code = "temporarily_unavailable"; message = "completion transport unavailable" }
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

(** A normal terminal completion removes its run before Core sends the later
    cache-eviction activation. The adapter must still acknowledge that leased
    eviction with Core's exact successful empty completion instead of trying
    to report an invalid workflow failure. *)
let test_eviction_after_terminal_completion () =
  let supervisor = fake_supervisor () in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_terminal_eviction"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () -> Ok ())
  in
  enqueue supervisor
    (activation ~run_id:"run-terminal-eviction"
       [ initialize ~run_id:"run-terminal-eviction"
           ~workflow_type:"native_worker_terminal_eviction" ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  expect_completed ~terminal:true (Result.get_ok (Worker.poll worker));
  enqueue supervisor
    (eviction_activation ~run_id:"run-terminal-eviction"
       [ Protocol.Remove_from_cache
           {
             message = "terminal run eviction";
             reason = Protocol.Cache_full;
           } ]);
  (* A raised native completion must preserve the empty acknowledgement rather
     than entering the ordinary failure-completion fallback. The next poll
     therefore retries the same leased eviction and has no workflow command. *)
  supervisor.raise_next_completion := true;
  begin
    match Worker.poll worker with
    | Error error when String.equal error.code "completion_failed" -> ()
    | Error error ->
        failwith
          (Printf.sprintf "terminal eviction raised the wrong error: %s" error.code)
    | Ok _ -> failwith "terminal eviction exception was unexpectedly acknowledged"
  end;
  if Hashtbl.length supervisor.leased <> 1 then
    failwith "raised terminal eviction did not retain its native lease";
  begin
    match Worker.poll worker with
    | Ok
        (Adapter.Completed
          { run_id = "run-terminal-eviction"; command_count = 0; terminal = false }) ->
        ()
    | Ok _ -> failwith "terminal eviction did not return an empty completion"
    | Error error ->
        failwith
          (Printf.sprintf "terminal eviction was rejected: %s" error.message)
  end;
  begin
    match (latest_completion supervisor).commands with
    | [] -> ()
    | _ -> failwith "terminal eviction emitted a workflow command"
  end;
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "terminal eviction left a native lease outstanding"

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

(** A workflow completion that is rejected after execution is retained exactly
    as produced. Draining the adapter acknowledges it without invoking the
    workflow implementation again, which is the shutdown safety property the
    native worker relies on. *)
let test_completion_rejection_is_drained_without_redo () =
  let supervisor = fake_supervisor () in
  let calls = ref 0 in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_completion_retry"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        incr calls;
        Ok ())
  in
  enqueue supervisor
    (activation ~run_id:"run-completion-retry"
       [ initialize ~run_id:"run-completion-retry"
           ~workflow_type:"native_worker_completion_retry" ]);
  supervisor.reject_next_completion := true;
  let worker = worker supervisor [ Adapter.register workflow ] in
  begin match Worker.poll worker with
  | Error { code = "completion_failed"; _ } -> ()
  | _ -> failwith "completion rejection did not remain a typed error"
  end;
  if !calls <> 1 then failwith "rejected completion reran the workflow";
  if Hashtbl.length supervisor.leased <> 1 then
    failwith "rejected completion unexpectedly retired the native lease";
  begin match Worker.drain worker with
  | Ok () -> ()
  | Error error ->
      failwith
        (Printf.sprintf "pending workflow completion was not drained: %s" error.message)
  end;
  if !calls <> 1 then failwith "draining the completion reran the workflow";
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "drained workflow completion left a native lease outstanding";
  if List.length !(supervisor.completions) <> 1 then
    failwith "draining submitted more than one workflow completion"

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

(** Child starts and their two-stage Core resolutions share one worker lease.
    The first completion records the start command, a successful start
    acknowledgment leaves the workflow pending, and the terminal child result
    finally retires the run with the parent output. *)
let test_child_command_and_resolution_lifecycle () =
  let supervisor = fake_supervisor () in
  let child =
    Temporal.Workflow.remote ~name:"native_worker_child"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit
  in
  let workflow =
    Temporal.Workflow.define ~name:"native_worker_child_lifecycle"
      ~input:Temporal.Codec.unit ~output:Temporal.Codec.unit (fun () ->
        let pending = Temporal.Child_workflow.start ~id:"child-1" child () in
        Temporal.Future.await pending)
  in
  enqueue supervisor
    (activation ~run_id:"run-child-gate"
       [ initialize ~run_id:"run-child-gate"
           ~workflow_type:"native_worker_child_lifecycle" ]);
  let worker = worker supervisor [ Adapter.register workflow ] in
  begin match Worker.poll worker with
  | Ok (Adapter.Completed { run_id = "run-child-gate"; terminal = false; command_count = 1 }) ->
      ()
  | Ok _ -> failwith "child command was not submitted as a pending completion"
  | Error error ->
      failwith ("child command lifecycle returned an adapter error: " ^ error.message)
  end;
  begin match (latest_completion supervisor).commands with
  | [ Protocol.Start_child_workflow { workflow_id = "child-1"; _ } ] -> ()
  | _ -> failwith "child command did not submit its protocol completion"
  end;
  enqueue supervisor
    (activation ~run_id:"run-child-gate"
       [
         Protocol.Resolve_child_workflow_start
           {
             seq = 1L;
             result = Protocol.Child_start_succeeded "child-run";
           };
       ]);
  begin match Worker.poll worker with
  | Ok (Adapter.Completed { run_id = "run-child-gate"; terminal = false; command_count = 0 }) ->
      ()
  | Ok _ -> failwith "child start acknowledgment unexpectedly completed the parent"
  | Error error ->
      failwith ("child start acknowledgment failed: " ^ error.message)
  end;
  enqueue supervisor
    (activation ~run_id:"run-child-gate"
       [
         Protocol.Resolve_child_workflow
           { seq = 1L; result = Protocol.Child_completed None };
       ]);
  begin match Worker.poll worker with
  | Ok (Adapter.Completed { run_id = "run-child-gate"; terminal = true; command_count = 1 }) ->
      ()
  | Ok _ -> failwith "child terminal result did not complete the parent"
  | Error error ->
      failwith ("child terminal result failed: " ^ error.message)
  end;
  begin match (latest_completion supervisor).commands with
  | [ Protocol.Complete_workflow { result = None } ] -> ()
  | _ -> failwith "child terminal result did not submit parent completion"
  end;
  if Hashtbl.length supervisor.leased <> 0 then
    failwith "child lifecycle left a native lease outstanding"

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
  test_eviction_after_terminal_completion ();
  test_unexpected_completion_exception_is_retried ();
  test_completion_rejection_is_drained_without_redo ();
  test_failure_completion_exception_is_typed ();
  test_resumed_failure_removes_run ();
  test_activity_command_retires_lease ();
  test_child_command_and_resolution_lifecycle ();
  test_unknown_run_retires_lease ();
  test_malformed_activation_error_is_typed ();
  test_registration_validation ();
  test_task_queue_validation ()
