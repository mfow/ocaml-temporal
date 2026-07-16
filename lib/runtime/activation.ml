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
  (** A synchronous query request. Query jobs carry no sequence because they
      answer a read-only request directly and never resolve a workflow future. *)
  | Query_workflow of {
      query_id : string;
      query_type : string;
      arguments : Temporal_base.Codec.payload list;
      headers : (string * Temporal_base.Codec.payload) list;
    }
  (** An update request retained in activation order. Core uses the protocol
      instance for responses; the workflow-visible ID is canonicalized from
      the top-level update identifier by the native bridge even when Core
      stripped its duplicate nested metadata field. *)
  | Do_update of {
      id : string;
      protocol_instance_id : string;
      name : string;
      input : Temporal_base.Codec.payload list;
      headers : (string * Temporal_base.Codec.payload) list;
      identity : string;
      update_id : string;
      run_validator : bool;
    }
  (** A validated incoming signal. The runtime keeps the complete event while
      the execution resolves its name against the private handler registry;
      missing or invalid public handlers fail closed rather than dropping the
      history-derived event. *)
  | Signal_workflow of {
      signal_name : string;
      input : Temporal_base.Codec.payload list;
      identity : string;
      headers : (string * Temporal_base.Codec.payload) list;
    }
  (** Records that replay history already contains the named patch marker.
      The execution applies this metadata before running workflow fibers so
      [Temporal.Workflow.patched] can reproduce the historical branch. *)
  | Notify_has_patch of { patch_id : string }
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

(** Controls when Core resolves a parent future after a child cancellation
    request.  [Abandon] reports cancellation without asking the child worker;
    [Try_cancel] requests cancellation and resolves immediately; the two wait
    policies retain the child state until Core reports the requested outcome. *)
type child_workflow_cancellation_type =
  | Child_try_cancel
  | Child_wait_cancellation_completed
  | Child_abandon
  | Child_wait_cancellation_requested

(** The validated retry policy attached to an activity or child-workflow
    command. The runtime stores the backoff coefficient as its canonical
    unsigned IEEE-754 bit string so replay and the JSON bridge never depend on
    a decimal float printer. Durations remain exact millisecond counts until
    the semantic protocol converts them to protobuf seconds and nanoseconds. *)
type retry_policy = {
  initial_interval : int64;
  backoff_coefficient_bits : string;
  maximum_interval : int64;
  maximum_attempts : int;
  non_retryable_error_types : string list;
}

(** Validated workflow matching priority retained by a buffered command. The
    fairness weight is kept as exact IEEE-754 bits so JSON serialization and
    replay do not depend on a decimal float representation. *)
type workflow_priority = {
  priority_key : int;
  fairness_key : string;
  fairness_weight_bits : int64;
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
      priority : workflow_priority option;
      cancellation_type : activity_cancellation_type;
      do_not_eagerly_execute : bool;
    }
  | Start_child_workflow of {
      seq : int64;
      id : string;
      name : string;
      input : Temporal_base.Codec.payload;
      retry_policy : retry_policy option;
      priority : workflow_priority option;
      cancellation_type : child_workflow_cancellation_type;
    }
  (** Requests cancellation of the child identified by the start command's
      sequence.  Core owns the race between this command and the start
      acknowledgment; the OCaml runtime only emits it once per pending child. *)
  | Cancel_child_workflow of { seq : int64; reason : string }
  | Request_cancel_activity of { seq : int64 }
  | Start_timer of { seq : int64; milliseconds : int64 }
  | Cancel_timer of { seq : int64 }
  (** Answers a synchronous query without entering the workflow scheduler. *)
  | Query_result of {
      query_id : string;
      result : (Temporal_base.Codec.payload, Temporal_base.Error.t) result;
    }
  (** Acknowledges one update protocol phase. The polymorphic variant keeps
      the runtime command private while mirroring the semantic protocol's
      accepted, rejected, and completed cases. *)
  | Update_response of {
      protocol_instance_id : string;
      response :
        [ `Accepted
        | `Rejected of Temporal_base.Error.t
        | `Completed of Temporal_base.Codec.payload ];
    }
  (** Tells Core that workflow code invoked one active or deprecated patch
      lifecycle operation. Core owns same-mode marker deduplication, so the
      OCaml runtime emits this command on every public patch call. *)
  | Set_patch_marker of { patch_id : string; deprecated : bool }
  | Complete_workflow of Temporal_base.Codec.payload
  | Fail_workflow of Temporal_base.Error.t
  (** Ends this run and asks Temporal to start the same workflow type with a
      fresh history and the supplied encoded input. *)
  | Continue_as_new of { workflow_type : string; input : Temporal_base.Codec.payload }
  | Cancel_workflow_execution
