(** State machine for an activity completion that was handed off to external
    code.

    This module is deliberately below the public API. It owns no native
    pointer and performs no I/O; the adapter supplies a callback that enters
    the single SDK supervisor. Keeping lifecycle state here makes handle
    methods safe when they are called from several OCaml Domains. *)

(** An encoded operation submitted to the private native adapter. Payloads are
    already copied and validated by the caller before they reach this module. *)
type operation =
  | Complete of Payload.t
  | Fail of Error.t
  | Cancel of Payload.t list
  | Heartbeat of Payload.t list

(** A callback result used by the state machine. The callback must return a
    typed error when acceptance is unknown; the pending operation is retained
    so the caller can retry the exact request. *)
type submit_result = (unit, Error.t) result

(** An opaque handle paired with the output type of its activity definition. *)
type 'output handle

(** An attempt-scoped context from which the callback can obtain its handle. *)
type 'output context

(** The outcome returned by an asynchronous activity implementation.

    [Completed] and [Failed] finish the activity while the worker callback is
    still running. [Will_complete_async] transfers the completion capability to
    the returned handle; the callback must not perform another completion after
    returning that value. *)
type 'output async_result =
  | Completed of 'output
  | Failed of Error.t
  | Will_complete_async of 'output handle

(** The callback type for an activity that may finish after its worker task
    has been acknowledged. The context is attempt-scoped and exists solely to
    obtain the opaque completion handle. *)
type ('input, 'output) implementation =
  'output context -> 'input -> 'output async_result

(** Creates a dormant handle. It cannot submit an operation until [activate]
    succeeds after the worker accepts [WillCompleteAsync]. [encode_output] is
    retained by the handle so callers can complete it with the activity's
    typed output rather than constructing a wire payload themselves. *)
val create :
  submit:(operation -> submit_result) ->
  encode_output:('output -> (Payload.t, Error.t) result) ->
  'output handle

(** Builds the callback context associated with a handle. *)
val context : 'output handle -> 'output context

(** Returns the handle retained by a callback context. *)
val handle : 'output context -> 'output handle

(** Linearizes the worker-to-client handoff. Calling this more than once is a
    typed lifecycle error. *)
val activate : 'output handle -> submit_result

(** Reserves the current attempt's dormant handle for the worker-side
    [WillCompleteAsync] acknowledgement. The [expected] identity is checked
    before changing lifecycle state, so a callback cannot return a handle
    retained from an earlier attempt whose submit callback still captures the
    earlier task token. Only the owning adapter calls this function. *)
val prepare_handoff : expected:'output handle -> 'output handle -> submit_result

(** Encodes and submits one complete operation. The state machine derives a
    canonical key from the encoded payload; if the transport fails, only the
    same byte-identical request may retry. *)
val complete : 'output handle -> 'output -> submit_result

(** Submits one failed operation attempt. *)
val fail : 'output handle -> Error.t -> submit_result

(** Submits one cancellation operation attempt with optional detail payloads. *)
val cancel : 'output handle -> Payload.t list -> submit_result

(** Sends one heartbeat without changing the terminal lifecycle. *)
val heartbeat : 'output handle -> Payload.t list -> submit_result

(** Closes a handle after the owning SDK has stopped. Closing while an
    operation is in flight returns an outstanding-operation error instead of
    silently invalidating that request. *)
val close : 'output handle -> submit_result
