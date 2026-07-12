(** One item of work delivered to a workflow execution. For example, a job can
    start the workflow, provide an activity result, or report that a timer has
    fired. [seq] identifies the earlier command that created that activity or
    timer. *)
type job =
  | Start_workflow
  | Resolve_activity of {
      seq : int64;
      result : (Temporal_base.Codec.payload, Temporal_base.Error.t) result;
    }
  (** Acknowledges that a child start was accepted. [Ok run_id] records the
      concrete execution while keeping the child result future pending. *)
  | Resolve_child_workflow_start of {
      seq : int64;
      result : (string, Temporal_base.Error.t) result;
    }
  | Resolve_child_workflow of {
      seq : int64;
      result : (Temporal_base.Codec.payload, Temporal_base.Error.t) result;
    }
  | Fire_timer of { seq : int64 }
  | Cancel_workflow
  | Remove_from_cache

(** How an activity reacts after workflow cancellation reaches its task. *)
type activity_cancellation_type =
  | Try_cancel
  | Wait_cancellation_completed
  | Abandon

(** An instruction produced by workflow code for Temporal Core. Commands are
    returned in the order they were created because Temporal records that order
    in workflow history and expects replay to reproduce it. Activity timeout
    values are exact non-negative milliseconds; [None] means that policy is not
    supplied. *)
type command =
  | Schedule_activity of {
      seq : int64;
      activity_id : string;
      activity_type : string;
      task_queue : string;
      arguments : Temporal_base.Codec.payload list;
      schedule_to_close_timeout : int64 option;
      schedule_to_start_timeout : int64 option;
      start_to_close_timeout : int64 option;
      heartbeat_timeout : int64 option;
      cancellation_type : activity_cancellation_type;
      do_not_eagerly_execute : bool;
    }
  | Start_child_workflow of {
      seq : int64;
      id : string;
      name : string;
      input : Temporal_base.Codec.payload;
    }
  | Request_cancel_activity of { seq : int64 }
  | Start_timer of { seq : int64; milliseconds : int64 }
  | Cancel_timer of { seq : int64 }
  | Complete_workflow of Temporal_base.Codec.payload
  | Fail_workflow of Temporal_base.Error.t
  | Cancel_workflow_execution
