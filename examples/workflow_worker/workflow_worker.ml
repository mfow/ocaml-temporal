(** Long-lived workflow-worker example.

    This process owns one Temporal worker connection and registers only the
    deterministic workflow. The workflow's activity reference is remote, so
    Temporal dispatches its external work to the separate activity worker. *)

(** Builds and runs the workflow-only worker, returning expected configuration
    and Temporal failures through the public [result] API. *)
let run () =
  let open Temporal.Result_syntax in
  let* config = Example_support.Config.from_environment () in
  let* worker =
    Temporal.Worker.create ~target_url:config.target_url ~namespace:config.namespace
      ~identity:"ocaml-temporal-example-workflow-worker"
      ~task_queue:config.task_queue
      ~workflows:
        [
          Temporal.Worker.workflow
            Example_support.Definitions.local_compose_message;
        ]
      ~activities:[] ()
  in
  Example_support.Lifecycle.run_worker worker

(** Reports the terminal result after [run] has disposed of the worker graph. *)
let () =
  match run () with
  | Ok () -> Printf.printf "workflow worker stopped cleanly\n%!"
  | Error error ->
      Example_support.report_error "workflow worker" error;
      exit 1
