(** Private worker-loop state for one OCaml-owned Temporal workflow worker.

    The functor below deliberately accepts already-decoded semantic protocol
    values. A native supervisor is responsible for decoding JSON, rejecting a
    malformed leased activation, and retiring that lease before returning its
    typed error. Keeping that responsibility below this module means this
    layer never guesses at a child/activity wire shape or silently drops a
    leased task. *)

module type SUPERVISOR = sig
  (** The opaque supervisor instance. Its implementation owns the native
      runtime/client/worker graph and serializes every operation on one owner
      Domain. *)
  type t

  (** Expected native-supervisor failure. The adapter copies only the stable
      code and message supplied by [error_code] and [error_message]; it never
      retains an exception or native pointer. *)
  type error

  (** The typed workflow poll operation. [None] means that the nonblocking
      poll observed no ready task; it is ordinary scheduler state. A returned
      [Error] must already have retired any leased activation that failed
      semantic decoding. *)
  val try_poll_workflow :
    t ->
    (Temporal_protocol.Workflow_protocol.activation option, error) result

  (** Submits one complete semantic activation result. The supervisor performs
      canonical validation and retires the exact native lease identified by
      [completion.run_id]. *)
  val complete_workflow :
    t ->
    Temporal_protocol.Workflow_protocol.completion ->
    (unit, error) result

  (** Stable, bounded classification for a supervisor error. *)
  val error_code : error -> string

  (** Stable diagnostic for a supervisor error. Implementations must omit
      payload bytes, credentials, task tokens, and unbounded remote text. *)
  val error_message : error -> string
end

(** Stable diagnostic exposed by this private worker loop. It is safe to log
    and intentionally excludes payload bytes and native handles. *)
type error_view = { code : string; path : string; message : string }

(** One workflow definition registered with the worker. The existential
    wrapper preserves the input/output codec relationship while allowing one
    registry to contain heterogeneous workflow functions. *)
type registered_workflow

(** One worker-loop outcome. [Rejected] means a valid lease was completed with
    a non-retryable bridge failure, so the caller can log the typed rejection
    and continue polling. A supervisor error remains a [result] error because
    the lease could not be proven retired. *)
type outcome =
  | Not_ready
  | Completed of {
      run_id : string;
      command_count : int;
      terminal : bool;
    }
  | Rejected of {
      run_id : string option;
      error : error_view;
      lease_retired : bool;
    }

module Make (Supervisor : SUPERVISOR) : sig
  (** A private owner-confined registry of running workflow executions. Calls
      to [poll] are serialized by an internal mutex so callers may safely
      invoke it from multiple ordinary Domains; workflow fibers must not call
      it directly because native supervisor operations are blocking
      producer-Domain calls. *)
  type t

  (** Creates a registry after validating every executable definition and
      rejecting duplicate Temporal workflow type names. [task_queue] is checked
      before the registry is published; empty, NUL-containing, oversized, or
      non-UTF-8 values return a typed configuration error instead of failing
      the first workflow activation. A valid queue is copied into every
      execution context so an activity without an explicit queue is sent back
      to the same queue as its workflow worker. No native operation is
      performed and no workflow function is called during creation. *)
  val create :
    ?task_queue:string ->
    supervisor:Supervisor.t ->
    workflows:registered_workflow list ->
    unit ->
    (t, error_view) result

  (** Polls at most one activation, applies it to the deterministic execution
      selected by its run ID, and submits exactly one completion. Empty native
      lanes return [Ok Not_ready]. Unsupported child-workflow commands, unknown
      run IDs, and invalid initialization inputs are converted to typed
      non-retryable workflow failures and reported as [Ok (Rejected _)] after
      their lease is retired. *)
  val poll : t -> (outcome, error_view) result

end

(** Wraps a public workflow definition in the private existential registration
    used by [Make]. Remote definitions are accepted by this constructor so the
    registry can report a typed configuration error at [create] rather than
    silently pretending they are executable. *)
val register :
  ('input, 'output,
   'input -> ('output, Temporal_base.Error.t) result)
  Temporal_base.Definition.t ->
  registered_workflow
