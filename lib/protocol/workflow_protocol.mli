(** Typed private JSON semantics for the first workflow activation and
    completion slice. Protobuf remains confined to the Rust bridge. *)

(** Binary Temporal payload with binary metadata preserved losslessly. *)
type payload = { metadata : (string * bytes) list; data : bytes }

(** Protobuf timestamp represented without floating-point conversion. *)
type timestamp = { seconds : int64; nanoseconds : int }

(** Nonnegative normalized protobuf duration. *)
type duration = { seconds : int64; nanoseconds : int }

(** Workflow/run identity for the root of a child-workflow tree. *)
type workflow_execution = { workflow_id : string; run_id : string }

(** Parent execution identity including its Temporal namespace. *)
type namespaced_workflow_execution = {
  namespace : string;
  workflow_id : string;
  run_id : string;
}

(** Exact workflow priority. [fairness_weight_bits] is the unsigned IEEE-754
    bit pattern, avoiding a lossy or non-integral JSON float representation. *)
type workflow_priority = {
  priority_key : int;
  fairness_key : string;
  fairness_weight_bits : int64;
}

(** Normal initialization fields present on an ordinary first workflow task.
    Grouping them keeps legacy fixtures readable while preserving every value
    needed to execute a basic root workflow. *)
type initialize_context = {
  headers : (string * payload) list;
  identity : string;
  parent_workflow : namespaced_workflow_execution option;
  workflow_execution_timeout : duration option;
  workflow_run_timeout : duration option;
  workflow_task_timeout : duration option;
  first_execution_run_id : string;
  start_time : timestamp option;
  root_workflow : workflow_execution option;
  priority : workflow_priority option;
}

(** Worker deployment identity attached to the current workflow task. *)
type worker_deployment_version = { deployment_name : string; build_id : string }

(** Service reason for suggesting continue-as-new. *)
type suggest_continue_as_new_reason =
  | Suggest_unspecified
  | History_size_too_large
  | Too_many_history_events
  | Too_many_updates

(** Activation metadata used by SDK language layers and normal Core traffic. *)
type activation_metadata = {
  available_internal_flags : int64 list;
  history_size_bytes : string;
  continue_as_new_suggested : bool;
  deployment_version_for_current_task : worker_deployment_version option;
  last_sdk_version : string;
  suggest_continue_as_new_reasons : suggest_continue_as_new_reason list;
  target_worker_deployment_version_changed : bool;
}

(** Retry state carried by an activity failure. *)
type retry_state =
  | Unspecified
  | In_progress
  | Non_retryable_failure
  | Timeout
  | Maximum_attempts_reached
  | Retry_policy_not_set
  | Internal_server_error
  | Cancel_requested

(** Supported structured Temporal failure-info variants. *)
type failure_info =
  | Application of {
      type_name : string;
      non_retryable : bool;
      details : payload list;
    }
  | Canceled of { details : payload list; identity : string }
  | Activity of {
      scheduled_event_id : int64;
      started_event_id : int64;
      identity : string;
      activity_type : string;
      activity_id : string;
      retry_state : retry_state;
    }

(** Recursive Temporal failure shared by activity resolution and workflow
    failure commands. *)
type failure = {
  message : string;
  source : string;
  stack_trace : string;
  encoded_attributes : payload option;
  cause : failure option;
  info : failure_info;
}

(** Result variants Core may deliver for a remote activity. *)
type activity_resolution =
  | Completed of payload option
  | Failed of failure
  | Cancelled of failure

(** Exact eviction reasons in the pinned Temporal Core revision. *)
type eviction_reason =
  | Eviction_unspecified
  | Cache_full
  | Cache_miss
  | Nondeterminism
  | Lang_fail
  | Lang_requested
  | Task_not_found
  | Unhandled_command
  | Fatal
  | Pagination_or_history_fetch
  | Workflow_execution_ending

(** Supported activation jobs in their Core-provided order. *)
type activation_job =
  | Initialize_workflow of {
      workflow_id : string;
      workflow_type : string;
      arguments : payload list;
      randomness_seed : string;
      attempt : int;
      context : initialize_context option;
    }
  | Resolve_activity of { seq : int64; result : activity_resolution }
  | Fire_timer of { seq : int64 }
  | Cancel_workflow of { reason : string }
  | Remove_from_cache of { message : string; reason : eviction_reason }

(** One workflow activation. [jobs] ordering is deterministic and retained
    exactly. *)
type activation = {
  run_id : string;
  timestamp : timestamp option;
  is_replaying : bool;
  history_length : int64;
  jobs : activation_job list;
  metadata : activation_metadata option;
}
(** A complete activation. [timestamp] is absent only on Core's synthetic
    cache-eviction activation; ordinary activations require it. *)

(** How a scheduled activity responds to cancellation. *)
type activity_cancellation_type =
  | Try_cancel
  | Wait_cancellation_completed
  | Abandon

(** Supported workflow commands in scheduler emission order. *)
type completion_command =
  | Schedule_activity of {
      seq : int64;
      activity_id : string;
      activity_type : string;
      task_queue : string;
      arguments : payload list;
      schedule_to_close_timeout : duration option;
      schedule_to_start_timeout : duration option;
      start_to_close_timeout : duration option;
      heartbeat_timeout : duration option;
      cancellation_type : activity_cancellation_type;
      do_not_eagerly_execute : bool;
    }
  | Request_cancel_activity of { seq : int64 }
  | Start_timer of { seq : int64; start_to_fire_timeout : duration }
  | Cancel_timer of { seq : int64 }
  | Complete_workflow of { result : payload option }
  | Fail_workflow of { failure : failure }
  | Cancel_workflow_execution

(** Successful completion of one activation. At most one terminal workflow
    command may appear, and it must be last. *)
type completion = { run_id : string; commands : completion_command list }

type error
(** Opaque semantic or strict-JSON validation failure. *)

type error_view = { code : string; path : string; message : string }
(** Privacy-safe error details that never contain source payload bytes. *)

val error_view : error -> error_view
(** Copies the stable error classification, path, and diagnostic. *)

val decode_activation : string -> (activation, error) result
(** Strictly decodes and semantically validates one activation document. *)

val encode_activation : activation -> (string, error) result
(** Validates, normalizes, and semantically reparses an outgoing activation. *)

val decode_completion : string -> (completion, error) result
(** Strictly decodes and semantically validates one completion document. *)

val encode_completion : completion -> (string, error) result
(** Validates, normalizes, and semantically reparses an outgoing completion. *)
