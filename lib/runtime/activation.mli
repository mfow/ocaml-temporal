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
  | Resolve_child_workflow of {
      seq : int64;
      result : (Temporal_base.Codec.payload, Temporal_base.Error.t) result;
    }
  | Fire_timer of { seq : int64 }
  | Cancel_workflow
  | Remove_from_cache

(** An instruction produced by workflow code for Temporal Core. Commands are
    returned in the order they were created because Temporal records that order
    in workflow history and expects replay to reproduce it. *)
type command =
  | Schedule_activity of {
      seq : int64;
      name : string;
      input : Temporal_base.Codec.payload;
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
