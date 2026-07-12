(** The private native worker lane scheduler.

    It deliberately does not know about Temporal protocol values or native
    handles. The two typed adapters report only scheduling summaries, while
    this module enforces the important liveness rule: a retained completion is
    retried after a bounded wait instead of terminating the worker or spinning
    on a permanently ready event. *)

type progress =
  | Progress
  | Not_ready
  | Retry_pending

(** Result-bind notation keeps bounded wait failures on the typed path. *)
let ( let* ) = Result.bind

(** Returns the next lane to use when the worker has to wait for ordinary
    readiness. Alternation prevents a workload with only one kind of task from
    being delayed by repeatedly waiting on the other lane. *)
let next_wait_lane workflow_lane = not workflow_lane

(** Runs both serialized lanes until shutdown or a fatal error. The activity
    lane is polled after the workflow lane, preserving the existing ordering;
    retry-pending results are handled before ordinary progress so a retained
    completion always receives its bounded backoff. *)
let run ~closed ~poll_workflow ~poll_activity ~wait_for_lane =
  let rec loop wait_workflow_lane =
    if closed () then Ok ()
    else
      let workflow_result = poll_workflow () in
      let activity_result =
        match workflow_result with
        | Error _ as error -> error
        | Ok _ -> poll_activity ()
      in
      match (workflow_result, activity_result) with
      | Error _, _ when closed () -> Ok ()
      | _, Error _ when closed () -> Ok ()
      | Error error, _ -> Error error
      | _, Error error -> Error error
      | Ok Retry_pending, _ ->
          let* () = wait_for_lane ~workflow_lane:true in
          loop (next_wait_lane wait_workflow_lane)
      | _, Ok Retry_pending ->
          let* () = wait_for_lane ~workflow_lane:false in
          loop (next_wait_lane wait_workflow_lane)
      | Ok Progress, _ | _, Ok Progress ->
          loop (next_wait_lane wait_workflow_lane)
      | Ok Not_ready, Ok Not_ready ->
          let* () = wait_for_lane ~workflow_lane:wait_workflow_lane in
          loop (next_wait_lane wait_workflow_lane)
  in
  loop true
