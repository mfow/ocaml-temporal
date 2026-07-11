(** The private deterministic backend seam used by the public client and
    worker modules' unit tests.

    This interface deliberately contains only owned OCaml values and is not a
    promise that the native adapter will implement these mock task shapes. The
    Rust/Core integration will use separate activation/completion semantic
    types plus an explicit admission/begin-shutdown/finalize lifecycle. None
    of those native details are exposed by the public package. *)

(** A validated connection configuration shared by client and worker graphs. *)
type config = {
  target_url : string;
  namespace : string;
  identity : string;
  task_queue : string option;
}

(** The request sent when a typed client starts one workflow execution. *)
type start_request = {
  workflow_name : string;
  workflow_id : string;
  task_queue : string;
  input : Payload.t;
}

(** The server-issued identity returned by a successful start. *)
type start_response = {
  workflow_id : string;
  run_id : string;
}

(** The exact execution selected by a client wait. *)
type wait_request = {
  workflow_id : string;
  run_id : string;
}

(** Terminal outcomes are kept separate from bridge transport errors so a
    completed Temporal failure remains an ordinary typed value. *)
type terminal_result =
  | Completed of Payload.t
  | Failed of Error.t
  | Cancelled of Error.t
  | Terminated of Error.t
  | Timed_out of Error.t
  | Continued_as_new of {
      workflow_id : string;
      run_id : string;
    }

(** A synthetic workflow task used only by the deterministic unit-test seam.
    Native Core activations carry replay metadata, jobs, and history context;
    the future adapter will translate those through separate private semantic
    types rather than widening this mock record. *)
type workflow_task = {
  task_token : string;
  workflow_name : string;
  input : Payload.t;
}

(** A synthetic activity task used only by the deterministic unit-test seam.
    Native activity tasks have cancellation and asynchronous-completion
    variants that are intentionally absent from this mock record. *)
type activity_task = {
  task_token : string;
  activity_name : string;
  input : Payload.t;
}

(** A single poll result. [Shutdown] is terminal for that poll stream and
    [Idle] permits adapters with a non-blocking readiness API. *)
type 'task poll_result =
  | Task of 'task
  | Idle
  | Shutdown

(** Synthetic workflow completion used only by the unit-test seam. Native Core
    completion is a semantic command set, not an output/failure payload. *)
type workflow_completion =
  | Workflow_completed of {
      task_token : string;
      output : Payload.t;
    }
  | Workflow_failed of {
      task_token : string;
      error : Error.t;
    }

(** Synthetic activity completion used only by the unit-test seam. Native Core
    completion also models cancellation and asynchronous completion. *)
type activity_completion =
  | Activity_completed of {
      task_token : string;
      output : Payload.t;
    }
  | Activity_failed of {
      task_token : string;
      error : Error.t;
    }

(** An opaque client backend instance owned by one public [Client.t]. *)
type client

(** An opaque worker backend instance owned by one public [Worker.t]. *)
type worker

(** Creates a deterministic client test seam. The in-memory transport is used
    only by unit tests; production URLs are reserved for the future Rust/Core
    adapter, whose semantic types and lifecycle are separate from this seam. *)
val client_create : config -> (client, Error.t) result

(** Starts one workflow after validating the request in the backend boundary. *)
val client_start : client -> start_request -> (start_response, Error.t) result

(** Waits for the exact workflow/run pair and returns its terminal outcome. *)
val client_wait : client -> wait_request -> (terminal_result, Error.t) result

(** Closes a client backend. Repeated calls are harmless and return [Ok ()]. *)
val client_shutdown : client -> (unit, Error.t) result

(** Creates a deterministic worker test seam and records the task queue and
    names registered by the OCaml registry. The queue and names are retained
    here so tests exercise the same admission inputs that the native adapter
    will validate, even though its activation protocol is different. *)
val worker_create :
  config ->
  workflow_names:string list ->
  activity_names:string list ->
  (worker, Error.t) result

(** Polls one workflow activation. At most one call may be in flight per
    worker; the supervisor implementation will enforce that invariant. *)
val worker_poll_workflow :
  worker -> (workflow_task poll_result, Error.t) result

(** Polls one activity task. At most one call may be in flight per worker. *)
val worker_poll_activity :
  worker -> (activity_task poll_result, Error.t) result

(** Completes exactly one previously polled workflow activation. *)
val worker_complete_workflow :
  worker -> workflow_completion -> (unit, Error.t) result

(** Completes exactly one previously polled activity task. *)
val worker_complete_activity :
  worker -> activity_completion -> (unit, Error.t) result

(** Closes a worker backend after pollers have drained. Repeated calls are
    idempotent so application shutdown paths can be safely retried. *)
val worker_shutdown : worker -> (unit, Error.t) result
