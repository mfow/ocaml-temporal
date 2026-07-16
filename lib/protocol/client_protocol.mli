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
  request_id : string;
  namespace : string;
  workflow_id : string;
  workflow_type : string;
  task_queue : string;
  input : payload list;
}
(** Dynamic workflow-start request sent to the Rust client adapter. [request_id]
    is stable across retries and is passed unchanged to Temporal, so a caller
    can reconcile an uncertain asynchronous start without issuing a second
    logical operation. *)

type start_response = { execution : execution }
(** Server-assigned execution returned by a successful start. *)

type start_ticket
(** Opaque capability for one admitted asynchronous start. The ticket retains
    the originating request privately so terminal outcomes can be correlated
    with the caller's requested workflow identity before they are exposed. *)

type wait_request = execution
(** Exact run selected by a wait; continued-as-new successors are not followed. *)

type cancel_request = {
  execution : execution;
  request_id : string;
  reason : string;
}
(** Exact run and idempotency metadata for a client cancellation request. *)

type cancel_response = { acknowledged : bool }
(** Positive acknowledgement returned after Temporal accepts the cancellation RPC. *)

type terminate_request = {
  execution : execution;
  reason : string;
}
(** Exact workflow/run identity and operator reason for a termination request. *)

type terminate_response = { acknowledged : bool }
(** Positive acknowledgement returned after Temporal accepts termination. *)

type signal_request = {
  execution : execution;
  signal_name : string;
  request_id : string;
  input : payload list;
}
(** Exact workflow/run identity and typed payloads for one signal delivery.

    [request_id] is the Temporal idempotency key for this logical control
    operation. The signal name and payload list are encoded in the same closed
    JSON document on both sides of the native bridge. *)

type signal_response = { acknowledged : bool }
(** Positive acknowledgement returned after Temporal accepts a signal RPC. *)

type query_request = {
  execution : execution;
  query_type : string;
  input : payload list;
}
(** Exact execution identity and output-only query name sent to Temporal. The
    input list is currently required to be empty by the public client API but
    remains explicit in the closed protocol for future typed query arguments. *)

type query_response = { result : payload list }
(** Ordered payloads returned by a successful workflow query. *)

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

type start_outcome =
  | Accepted of start_response
  | Rejected of client_error
  | Unknown of { request_id : string; workflow_id : string }
(** Terminal result of an asynchronous start. [Unknown] means the bridge
    cannot prove whether Temporal accepted the request; callers must reconcile
    using [request_id] rather than automatically retrying. *)

type error
(** Privacy-safe client-protocol validation failure. *)

type error_view = { code : string; path : string; message : string }
(** Stable diagnostic view that never contains payload bytes. *)

val error_view : error -> error_view
(** Copies the safe fields of a protocol error. *)

val encode_start_request : start_request -> (string, error) result
(** Validates and serializes one start request. *)

val encode_start_ticket : start_ticket -> (string, error) result
(** Serializes one opaque ticket for the private poll/wait bridge calls. *)

val decode_start_ticket :
  request:start_request -> string -> (start_ticket, error) result
(** Strictly decodes a native ticket and binds it to the request that admitted
    it. Binding the request here prevents a ticket result from being accepted
    for another workflow identity. *)

val start_ticket_request : start_ticket -> start_request
(** Returns the request retained by an opaque ticket for supervisor-side
    correlation. The native ticket string itself remains inaccessible. *)

val decode_start_outcome :
  request:start_request -> string -> (start_outcome, error) result
(** Strictly decodes a terminal asynchronous-start outcome and correlates every
    execution, rejection identity, and unknown request identity with [request]. *)

val encode_start_outcome : start_outcome -> (string, error) result
(** Validates and serializes one terminal asynchronous-start outcome. *)

val decode_start_response : request:start_request -> string -> (start_response, error) result
(** Strictly decodes one successful start response and verifies that the
    returned namespace and workflow ID belong to the requested start. *)

val encode_wait_request : wait_request -> (string, error) result
(** Validates and serializes one exact-run wait request. *)

val encode_cancel_request : cancel_request -> (string, error) result
(** Validates and serializes one exact-run cancellation request. *)

val decode_cancel_response : string -> (cancel_response, error) result
(** Strictly decodes the positive native cancellation acknowledgement. *)

val encode_terminate_request : terminate_request -> (string, error) result
(** Validates and serializes one exact-run termination request. *)

val decode_terminate_response : string -> (terminate_response, error) result
(** Strictly decodes the positive native termination acknowledgement. *)

val encode_signal_request : signal_request -> (string, error) result
(** Validates and serializes one exact-run signal request. *)

val decode_signal_response : string -> (signal_response, error) result
(** Strictly decodes the positive native signal acknowledgement. *)

val encode_query_request : query_request -> (string, error) result
(** Validates and serializes one exact-run, output-only query request. *)

val decode_query_response : string -> (query_response, error) result
(** Strictly decodes one successful query result payload list. *)

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

val decode_cancel_error : string -> (client_error, error) result
(** Decodes a cancellation error and rejects the start-only
    [already_started] category. *)

val decode_terminate_error : string -> (client_error, error) result
(** Decodes a termination error and rejects the start-only
    [already_started] category. *)

val decode_signal_error : string -> (client_error, error) result
(** Decodes a signal error and rejects the start-only [already_started]
    category. *)

val decode_query_error : string -> (client_error, error) result
(** Decodes a query error and rejects the start-only [already_started]
    category. *)
