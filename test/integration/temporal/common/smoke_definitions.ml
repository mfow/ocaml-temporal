(** Definitions shared by the future two-process Temporal acceptance test.

    Keeping the workflow and activity values in one private test library makes
    the driver and worker compile against exactly the same names and codecs. The
    module deliberately contains no process, filesystem, network, or clock
    access: those operations would make the workflow bodies non-replayable. *)

(** The task queue used only by this fixture. It is intentionally distinct from
    every production queue so an accidentally reused local namespace cannot
    dispatch test work to an application worker. *)
let task_queue = "ocaml-temporal-two-binary-smoke"

(** The mock activity uppercases its input. The implementation is deterministic
    so the driver can assert the exact result without depending on external
    services or wall-clock state. *)
let mock_transform =
  Temporal.Activity.define ~name:"smoke.mock_transform"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun input ->
      Ok (String.uppercase_ascii input))

(** Starts two independent activity commands before awaiting either result.
    [Future.all] preserves input order, making this scenario test both fan-out
    and deterministic aggregation once the native worker loop is connected. *)
let fan_out =
  Temporal.Workflow.define ~name:"smoke.fan_out" ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string (fun seed ->
      let left = Temporal.Activity.start mock_transform (seed ^ ":left") in
      let right = Temporal.Activity.start mock_transform (seed ^ ":right") in
      match Temporal.Future.await (Temporal.Future.all [ left; right ]) with
      | Error error -> Error error
      | Ok values -> Ok (String.concat "|" values))

(** Waits for a short durable timer before scheduling one activity. The timer is
    an SDK command rather than an OCaml sleep, so replay can resolve it from
    Temporal history when this definition is eventually run by Core. *)
let timer_then_activity =
  Temporal.Workflow.define ~name:"smoke.timer_then_activity"
    ~input:Temporal.Codec.string ~output:Temporal.Codec.string (fun seed ->
      match Temporal.Workflow.sleep (Temporal.Duration.of_ms 10L) with
      | Error error -> Error error
      | Ok () -> Temporal.Activity.execute mock_transform (seed ^ ":timer"))
