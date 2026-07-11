module Activation = Temporal_runtime.Activation
module Bridge = Temporal_core_bridge.Native_bridge
module Execution = Temporal_runtime.Execution
module Observability = Temporal_base.Observability

(** One event captured without retaining its formatting closure. Tests inspect
    stable source, level, and tag contracts rather than exact prose. *)
type event = {
  source : string;
  level : Logs.level;
  tags : Logs.Tag.set;
  message : string;
}

(** Captured events are process-local because each Dune test is a separate
    executable. *)
let events = ref []

(** Reporter that eagerly renders the bounded SDK message and preserves tags
    for structural assertions. *)
let capture_reporter =
  let report source level ~over continuation messagef =
    messagef (fun ?header:_ ?(tags = Logs.Tag.empty) format ->
        Format.kasprintf
          (fun message ->
            events :=
              {
                source = Logs.Src.name source;
                level;
                tags;
                message;
              }
              :: !events;
            over ();
            continuation ())
          format)
  in
  { Logs.report }

(** Reporter used to prove that an application reporter defect cannot alter an
    SDK operation's result or escape through workflow processing. *)
let raising_reporter =
  { Logs.report = (fun _ _ ~over:_ _ _ -> failwith "reporter defect") }

(** Runs an action with all SDK sources enabled and restores process-global
    Logs state even when an assertion fails. *)
let with_capture action =
  let reporter = Logs.reporter () in
  let level = Logs.level () in
  events := [];
  Logs.set_reporter capture_reporter;
  Logs.set_level (Some Logs.Debug);
  Fun.protect
    ~finally:(fun () ->
      Logs.set_reporter reporter;
      Logs.set_level level)
    action

(** Finds the newest event matching a stable source, level, and operation tag. *)
let find_event ~source ~level ~operation =
  List.find_opt
    (fun event ->
      event.source = source
      && event.level = level
      && Logs.Tag.find Observability.Tag.operation event.tags = Some operation)
    !events

(** Extracts an event or fails with a structural description. *)
let require_event ~source ~level ~operation =
  match find_event ~source ~level ~operation with
  | Some event -> event
  | None ->
      failwith
        (Printf.sprintf "missing %s event for %s" source operation)

(** Returns whether [needle] occurs in [haystack]. This local helper keeps the
    privacy assertion compatible with the project's OCaml 5.2 floor. *)
let contains ~haystack ~needle =
  let haystack_length = String.length haystack in
  let needle_length = String.length needle in
  let rec search offset =
    offset + needle_length <= haystack_length
    &&
    (String.sub haystack offset needle_length = needle || search (offset + 1))
  in
  needle_length = 0 || search 0

(** Builds a string payload for the synthetic workflow fixture. *)
let payload value =
  match Temporal.Codec.encode Temporal.Codec.string value with
  | Ok payload -> payload
  | Error error -> failwith (Temporal.Error.message error)

(** Activity that keeps the workflow blocked after its first activation. *)
let waiting_activity =
  Temporal.Activity.remote ~name:"wait" ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

(** Workflow fixture whose payload-like input must never enter log prose. *)
let waiting_workflow =
  Temporal.Workflow.define ~name:"logging_fixture"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun input ->
      Temporal.Activity.execute waiting_activity input)

(** Stable source and tag names are the filtering contract applications use. *)
let test_source_and_tag_names () =
  assert (Logs.Src.name Observability.Source.lifecycle = "temporal.sdk.lifecycle");
  assert (Logs.Src.name Observability.Source.bridge = "temporal.sdk.bridge");
  assert (Logs.Src.name Observability.Source.workflow = "temporal.sdk.workflow");
  assert (Logs.Tag.name Observability.Tag.operation = "temporal.operation");
  assert (Logs.Tag.name Observability.Tag.duration_ms = "temporal.duration_ms");
  assert
    (Logs.Tag.name Observability.Tag.workflow_type = "temporal.workflow_type");
  assert (Logs.Tag.name Observability.Tag.job_count = "temporal.job_count");
  assert
    (Logs.Tag.name Observability.Tag.command_count = "temporal.command_count");
  assert
    (Logs.Tag.name Observability.Tag.bridge_status = "temporal.bridge_status");
  assert
    (Logs.Tag.name Observability.Tag.error_kind = "temporal.error_kind")

(** Bridge calls report status and non-negative millisecond latency without
    changing their typed result behavior. *)
let test_bridge_events () =
  with_capture (fun () ->
      (match Bridge.check_abi_version 2l with
      | Error { status = Abi_mismatch; _ } -> ()
      | _ -> failwith "ABI mismatch contract changed");
      let failure =
        require_event ~source:"temporal.sdk.bridge" ~level:Logs.Error
          ~operation:"check_abi_version"
      in
      assert
        (Logs.Tag.find Observability.Tag.bridge_status failure.tags
        = Some "abi_mismatch");
      let latency =
        require_event ~source:"temporal.sdk.bridge" ~level:Logs.Debug
          ~operation:"check_abi_version"
      in
      assert
        (match Logs.Tag.find Observability.Tag.duration_ms latency.tags with
        | Some duration -> duration >= 0.
        | None -> false);
      let runtime =
        match Bridge.runtime_create () with
        | Ok runtime -> runtime
        | Error error -> failwith error.message
      in
      assert (Bridge.runtime_close runtime = Ok ());
      ignore
        (require_event ~source:"temporal.sdk.lifecycle" ~level:Logs.Info
           ~operation:"runtime_create");
      ignore
        (require_event ~source:"temporal.sdk.lifecycle" ~level:Logs.Info
           ~operation:"runtime_close"))

(** Workflow processing reports bounded structural metadata and never renders
    raw workflow input. *)
let test_workflow_events_and_privacy () =
  with_capture (fun () ->
      let secret = "payload-do-not-log-4ef315" in
      let execution = Execution.start waiting_workflow secret in
      let commands = Execution.activate execution [ Activation.Start_workflow ] in
      assert
        (commands
        = [
            Activation.Schedule_activity
              { seq = 1L; name = "wait"; input = payload secret };
          ]);
      let activation =
        require_event ~source:"temporal.sdk.workflow" ~level:Logs.Debug
          ~operation:"activate"
      in
      assert
        (Logs.Tag.find Observability.Tag.workflow_type activation.tags
        = Some "logging_fixture");
      assert
        (Logs.Tag.find Observability.Tag.job_count activation.tags = Some 1);
      assert
        (Logs.Tag.find Observability.Tag.command_count activation.tags = Some 1);
      assert
        (match Logs.Tag.find Observability.Tag.duration_ms activation.tags with
        | Some duration -> duration >= 0.
        | None -> false);
      assert
        (not
           (List.exists
              (fun event -> contains ~haystack:event.message ~needle:secret)
              !events));
      ignore (Execution.activate execution [ Activation.Start_workflow ]);
      let failure =
        require_event ~source:"temporal.sdk.workflow" ~level:Logs.Error
          ~operation:"workflow_failed"
      in
      assert
        (Logs.Tag.find Observability.Tag.error_kind failure.tags = Some "bridge"))

(** Reporter exceptions are swallowed at the common wrapper, including both a
    direct bridge call and workflow activation. *)
let test_reporter_exceptions_are_contained () =
  let reporter = Logs.reporter () in
  let level = Logs.level () in
  Logs.set_reporter raising_reporter;
  Logs.set_level (Some Logs.Debug);
  Fun.protect
    ~finally:(fun () ->
      Logs.set_reporter reporter;
      Logs.set_level level)
    (fun () ->
      (match Bridge.check_abi_version 2l with
      | Error { status = Abi_mismatch; _ } -> ()
      | _ -> failwith "reporter changed bridge behavior");
      let execution = Execution.start waiting_workflow "private-input" in
      match Execution.activate execution [ Activation.Start_workflow ] with
      | [ Activation.Schedule_activity _ ] -> ()
      | _ -> failwith "reporter changed workflow behavior")

let () =
  test_source_and_tag_names ();
  test_bridge_events ();
  test_workflow_events_and_privacy ();
  test_reporter_exceptions_are_contained ()
