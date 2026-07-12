module Activation = Temporal_runtime.Activation
module Execution = Temporal_runtime.Execution
module Context = Temporal_runtime.Workflow_context_store

(** Runtime-level retry policy record used to verify the activation command
    after the public constructor tests have checked the user-facing API. *)
let retry_policy : Activation.retry_policy =
  {
    initial_interval = 1_000L;
    backoff_coefficient_bits = "4609434218613702656";
    maximum_interval = 60_000L;
    maximum_attempts = 4;
    non_retryable_error_types = [ "InvalidInput" ];
  }

(** Encodes one string argument using the same base codec the public activity
    adapter supplies before it reaches the private workflow context. *)
let encode_input input =
  match Temporal_base.Codec.encode Temporal_base.Codec.string input with
  | Ok payload -> payload
  | Error error -> failwith (Temporal_base.Error.message error)

(** Defines a base workflow whose implementation schedules an activity directly
    through the private context; this isolates command translation from the
    public adapter tested by [test_activity_retry_policy]. *)
let make_workflow ?policy name =
  Temporal_base.Definition.make ~name ~input:Temporal_base.Codec.string
    ~output:Temporal_base.Codec.string
    ~implementation:
      (Some (fun input ->
           match Context.current () with
           | None ->
               Error
                 (Temporal_base.Error.defect
                    ~message:"retry-policy test ran outside workflow")
           | Some context ->
               let future =
                 Context.schedule_activity context ~name:"retryable_activity"
                   ~input:(encode_input input) ?retry_policy:policy
                   ~decode:(Temporal_base.Codec.decode Temporal_base.Codec.string)
                   ()
               in
               Temporal_runtime.Future_store.await future))

(** Asserts the exact private representation emitted for an explicit policy. *)
let test_explicit_policy () =
  match
    Execution.activate
      (Execution.start (make_workflow ~policy:retry_policy "retry_policy") "input")
      [ Activation.Start_workflow ]
  with
  | [
      Activation.Schedule_activity
        {
          retry_policy = Some policy;
          _;
        };
    ] ->
      if policy <> retry_policy then failwith "retry policy was changed"
  | _ -> failwith "explicit retry policy was not emitted"

(** Asserts omitted policy remains an absent optional Core field. *)
let test_omitted_policy () =
  match
    Execution.activate
      (Execution.start (make_workflow "default_retry") "input")
      [ Activation.Start_workflow ]
  with
  | [ Activation.Schedule_activity { retry_policy = None; _ } ] -> ()
  | _ -> failwith "omitted retry policy was changed into a concrete policy"

(** Verifies that callers of the exported one-command translator cannot bypass
    retry-policy validation by avoiding [completion_of_commands]. *)
let test_direct_translation_validation () =
  let malformed_policy = { retry_policy with backoff_coefficient_bits = "0" } in
  let command =
    Activation.Schedule_activity
      {
        seq = 1L;
        activity_id = "activity-1";
        activity_type = "retryable_activity";
        task_queue = "default";
        arguments = [ encode_input "input" ];
        schedule_to_close_timeout = Some 60_000L;
        schedule_to_start_timeout = None;
        start_to_close_timeout = Some 30_000L;
        heartbeat_timeout = None;
        retry_policy = Some malformed_policy;
        cancellation_type = Activation.Try_cancel;
        do_not_eagerly_execute = false;
      }
  in
  match Temporal_runtime.Native_execution.command_to_protocol command with
  | Error _ -> ()
  | Ok _ -> failwith "direct command translation accepted malformed policy"

let () =
  test_explicit_policy ();
  test_omitted_policy ();
  test_direct_translation_validation ()
