(** Long-lived activity-worker example.

    This process owns a separate Temporal worker connection and registers only
    the activity implementation. It is the correct boundary for external work;
    the sample formats text locally so it can run without another service. *)

(** Builds and runs the activity-only worker, ensuring signal-triggered or
    ordinary exit invokes the public worker shutdown operation. *)
let run () =
  let open Temporal.Result_syntax in
  let* config = Example_support.Config.from_environment () in
  let* worker =
    Temporal.Worker.create ~target_url:config.target_url ~namespace:config.namespace
      ~identity:"ocaml-temporal-example-activity-worker"
      ~task_queue:config.task_queue ~workflows:[]
      ~activities:
        [
          Temporal.Worker.activity
            Example_support.Definitions.local_render_message;
        ]
      ()
  in
  Example_support.Lifecycle.run_worker worker

(** Reports the terminal result after [run] has disposed of the worker graph. *)
let () =
  match run () with
  | Ok () -> Printf.printf "activity worker stopped cleanly\n%!"
  | Error error ->
      Example_support.report_error "activity worker" error;
      exit 1
