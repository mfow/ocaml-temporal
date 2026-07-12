(** Private scheduler for the two native worker lanes.

    The workflow and activity adapters own task leases and typed execution.
    This small module owns only the decision made after one lane has been
    polled: continue immediately after progress, wait for readiness when both
    lanes are empty, or apply a bounded wait before retrying a retained native
    completion. Keeping that policy separate makes it testable with fake lane
    sources without constructing Rust handles or a Temporal Server. *)

(** Scheduling summary returned by one lane poll. *)
type progress =
  | Progress
      (** A task was processed or a task-level failure was acknowledged. *)
  | Not_ready
      (** The lane had no work at this instant. *)
  | Retry_pending
      (** A completion or other explicitly retryable native operation remains
          pending; the worker must wait before trying the same operation. *)

(** Runs the serialized workflow/activity polling policy until [closed] is
    observed or a non-retryable lane/wait error is returned.

    [wait_for_lane] receives [true] for the workflow readiness lane and [false]
    for the activity lane. The callback must perform a bounded wait and must
    not hold an OCaml adapter mutex while it blocks. [retry_pending] receives
    the same lane flag but must apply a real bounded backoff (normally through
    the dedicated native supervisor Domain) before the next poll. Retry-pending
    work always selects its own lane, so a transient completion rejection
    cannot become a tight loop or be mistaken for ordinary idle state. *)
val run :
  closed:(unit -> bool) ->
  poll_workflow:(unit -> (progress, 'error) result) ->
  poll_activity:(unit -> (progress, 'error) result) ->
  wait_for_lane:(workflow_lane:bool -> (unit, 'error) result) ->
  retry_pending:(workflow_lane:bool -> (unit, 'error) result) ->
  (unit, 'error) result
