(** Client-only driver for the live parent/child worker-restart acceptance.

    This process owns no worker and never executes workflow code. It starts one
    fixed parent workflow through the public client API, publishes only durable
    identifiers needed by the external controller, and waits on that exact
    parent run while the controller replaces the dedicated worker. *)

module Client = Temporal.Client
(** The public client interface used to start and wait for the parent run. *)

module Error = Temporal.Error
(** Structured SDK errors used for all expected driver failures. *)

module Definitions = Smoke_definitions
(** Shared, typed fixture definitions and stable acceptance identities. *)

(** Reads one required non-empty environment setting without raising during
    executable initialization. The Compose service supplies these paths and
    connection values; a missing value is a typed fixture configuration error.
*)
let required_env (name : string) : (string, Error.t) result =
  match Sys.getenv_opt name with
  | Some value when value <> "" -> Ok value
  | _ -> Error (Error.defect ~message:(name ^ " must not be empty"))

(** Publishes a complete marker by writing beside the destination and renaming
    it atomically. The controller consequently sees either no marker or the
    complete parent/run identity, never a partial write while this client is
    still constructing it. *)
let publish_marker (path : string) (contents : string) : (unit, Error.t) result
    =
  let temporary = ref None in
  try
    let generated =
      Filename.temp_file ~temp_dir:(Filename.dirname path)
        (Filename.basename path ^ ".tmp.")
        ""
    in
    temporary := Some generated;
    let channel = open_out_bin generated in
    Fun.protect
      ~finally:(fun () -> close_out_noerr channel)
      (fun () ->
        output_string channel contents;
        flush channel);
    Sys.rename generated path;
    temporary := None;
    Ok ()
  with exception_ ->
    Option.iter
      (fun generated -> try Sys.remove generated with _ -> ())
      !temporary;
    Error
      (Error.defect
         ~message:
           (Printf.sprintf "cannot publish parent/child restart marker %s: %s"
              path
              (Printexc.to_string exception_)))

(** Accepts only the exact parent completion promised by the fixture. All other
    terminal variants are converted into typed defects so the controller cannot
    mistake a failed child, cancellation, or successor run for a successful
    post-restart parent resolution. *)
let require_completed : string Client.terminal_result -> (unit, Error.t) result
    = function
  | Client.Completed value
    when String.equal value Definitions.parent_child_restart_result ->
      Ok ()
  | Client.Completed _ ->
      Error
        (Error.defect
           ~message:
             "parent/child restart workflow returned an unexpected result")
  | Client.Failed error
  | Client.Cancelled error
  | Client.Terminated error
  | Client.Timed_out error ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "parent/child restart workflow ended with %s: %s"
                (Error.kind error) (Error.message error)))
  | Client.Continued_as_new execution ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf
                "parent/child restart workflow continued as new at run %s"
                execution.run_id))

(** Runs [body] with [client] and always attempts public client shutdown after
    the body has returned. A body failure takes precedence over shutdown
    failure, preserving the causal workflow error while still releasing the
    client graph. Unexpected exceptions are contained as typed defects so they
    do not skip shutdown. *)
let run_with_client (client : Client.t) (body : unit -> (unit, Error.t) result)
    : (unit, Error.t) result =
  let body_result =
    try body ()
    with exception_ ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "parent/child restart driver body raised: %s"
                (Printexc.to_string exception_)))
  in
  let shutdown_result =
    try Client.shutdown client
    with exception_ ->
      Error
        (Error.defect
           ~message:
             (Printf.sprintf "parent/child restart client shutdown raised: %s"
                (Printexc.to_string exception_)))
  in
  match (body_result, shutdown_result) with
  | Error body_error, _ -> Error body_error
  | Ok (), Ok () -> Ok ()
  | Ok (), Error shutdown_error -> Error shutdown_error

(** Starts the fixed parent workflow and waits on the exact run returned by
    [Client.start]. The published child ID is derivable from the fixed input,
    but recording it alongside the parent run keeps the external history
    controller independent of a duplicate string literal. *)
let run () : (unit, Error.t) result =
  match Sys.getenv_opt "TEMPORAL_TWO_BINARY_LIVE" with
  | Some "1" ->
      let open Temporal.Result_syntax in
      let* target_url = required_env "TEMPORAL_ADDRESS" in
      let* namespace = required_env "TEMPORAL_NAMESPACE" in
      let* accepted_file =
        required_env "SMOKE_PARENT_CHILD_RESTART_ACCEPTED_FILE"
      in
      let* result_file =
        required_env "SMOKE_PARENT_CHILD_RESTART_RESULT_FILE"
      in
      let* client =
        Client.create ~target_url ~namespace
          ~identity:"ocaml-temporal-parent-child-restart-driver" ()
      in
      run_with_client client (fun () ->
          let* handle =
            Client.start client
              ~workflow:Definitions.parent_child_restart_parent
              ~task_queue:Definitions.task_queue
              ~id:Definitions.parent_child_restart_parent_id
              ~input:Definitions.parent_child_restart_input ()
          in
          let child_workflow_id =
            Definitions.parent_child_restart_child_id
              Definitions.parent_child_restart_input
          in
          let* () =
            publish_marker accepted_file
              (Printf.sprintf
                 "workflow_id=%s\nrun_id=%s\nchild_workflow_id=%s\n"
                 (Client.workflow_id handle)
                 (Client.run_id handle) child_workflow_id)
          in
          let* outcome = Client.wait handle in
          let* () = require_completed outcome in
          publish_marker result_file "completed\n")
  | _ ->
      Error
        (Error.defect
           ~message:
             "parent/child restart acceptance is not enabled; set \
              TEMPORAL_TWO_BINARY_LIVE=1")

(** Converts the final typed driver result into a useful process status while
    keeping workflow payloads and native bridge state out of the process log. *)
let () =
  match run () with
  | Ok () -> Printf.printf "parent/child restart driver completed\n%!"
  | Error error ->
      Printf.eprintf "parent/child restart driver failed (%s): %s\n%!"
        (Error.kind error) (Error.message error);
      exit 1
