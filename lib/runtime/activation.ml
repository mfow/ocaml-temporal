(** Small OCaml representation of the jobs received from Core and the commands
    returned to it. Tests use these values without constructing Protobuf
    messages; a separate future module will translate to and from Core's actual
    Protobuf format. *)
type job =
  | Start_workflow
  | Resolve_activity of {
      seq : int64;
      result : (Temporal_base.Codec.payload, Temporal_base.Error.t) result;
    }
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

(** How an activity reacts after workflow cancellation reaches its task.  The
    values deliberately mirror Temporal Core's three cancellation policies but
    remain a runtime type so the public API does not expose protocol records. *)
type activity_cancellation_type =
  | Try_cancel
  | Wait_cancellation_completed
  | Abandon

(** The validated retry policy attached to a scheduled remote activity.  The
    runtime stores the backoff coefficient as its canonical unsigned IEEE-754
    bit string so replay and the JSON bridge never depend on a decimal float
    printer.  Durations remain exact millisecond counts until the semantic
    protocol converts them to protobuf seconds and nanoseconds. *)
type retry_policy = {
  initial_interval : int64;
  backoff_coefficient_bits : string;
  maximum_interval : int64;
  maximum_attempts : int;
  non_retryable_error_types : string list;
}

(** Commands contain only immutable data needed by the semantic JSON
    translator; they never expose the runtime's mutable execution state.  The
    activity record carries every field Core needs to schedule a task, rather
    than making the translation layer invent an identifier, queue, or timeout. *)
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
      retry_policy : retry_policy option;
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
