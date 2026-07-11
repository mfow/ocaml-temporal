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
  | Resolve_child_workflow of {
      seq : int64;
      result : (Temporal_base.Codec.payload, Temporal_base.Error.t) result;
    }
  | Fire_timer of { seq : int64 }
  | Cancel_workflow
  | Remove_from_cache

(** Commands contain only immutable data needed by the future Protobuf
    translator; they never expose the runtime's mutable execution state. *)
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
