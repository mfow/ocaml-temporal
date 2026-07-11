(** Strict JSON values for the native client start and exact-run wait bridge.

    The Rust side owns Temporal's protobuf and network client.  This module
    owns the corresponding OCaml representation of the small JSON documents
    that cross that private boundary.  Every decoder rejects unknown or missing
    members and every encoder validates its own output before returning it. *)

type payload = Workflow_protocol.payload
(** Binary-safe Temporal payload shared with the workflow semantic protocol. *)

type failure = Workflow_protocol.failure
(** Structured Temporal failure shared with workflow activations. *)

type execution = { namespace : string; workflow_id : string; run_id : string }
(** The exact Temporal execution identified by namespace, workflow ID, and run. *)

type start_request = {
  namespace : string;
  workflow_id : string;
  workflow_type : string;
  task_queue : string;
  input : payload list;
}
(** Dynamic workflow-start request sent to the Rust client adapter. *)

type start_response = { execution : execution }
(** Server-assigned execution returned by a successful start. *)

type wait_request = execution
(** Exact run selected by a wait; continued-as-new successors are not followed. *)

type outcome =
  | Completed of { result : payload list; successor : execution option }
  | Failed of { failure : failure; successor : execution option }
  | Cancelled of { details : payload list }
  | Terminated of { details : payload list }
  | Timed_out of { successor : execution option }
  | Continued_as_new of { successor : execution }
(** Terminal outcome returned by Temporal for one exact run. *)

type wait_response = { execution : execution; outcome : outcome }
(** Exact execution and its terminal result returned by the native adapter. *)

type client_error =
  | Already_started of { workflow_id : string; existing_run_id : string option }
  | Rpc of { code : string }
  | Protocol of { code : string }
(** Closed error body returned by a native client operation. *)

type error
(** Privacy-safe client-protocol validation failure. *)

type error_view = { code : string; path : string; message : string }
(** Stable diagnostic view that never contains payload bytes. *)

val error_view : error -> error_view
(** Copies the safe fields of a protocol error. *)

val encode_start_request : start_request -> (string, error) result
(** Validates and serializes one start request. *)

val decode_start_response : request:start_request -> string -> (start_response, error) result
(** Strictly decodes one successful start response and verifies that the
    returned namespace and workflow ID belong to the requested start. *)

val encode_wait_request : wait_request -> (string, error) result
(** Validates and serializes one exact-run wait request. *)

val decode_wait_response : request:wait_request -> string -> (wait_response, error) result
(** Strictly decodes one terminal exact-run response and verifies that the
    returned execution is exactly the requested run. *)

val decode_client_error : string -> (client_error, error) result
(** Strictly decodes the structured error body returned by the native ABI. *)

val decode_start_error :
  request:start_request -> string -> (client_error, error) result
(** Decodes a start error and correlates an [already_started] body with the
    workflow ID supplied by the start request. *)

val decode_wait_error :
  request:wait_request -> string -> (client_error, error) result
(** Decodes an exact-run wait error and rejects the start-only
    [already_started] category. *)
