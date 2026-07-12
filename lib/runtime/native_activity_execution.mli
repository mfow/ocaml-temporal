(** Private typed execution of one native Temporal activity-task lease.

    The native supervisor supplies already decoded semantic tasks and accepts
    semantic completions. This adapter owns the OCaml activity registry, decodes
    one activity input with the definition's codec, invokes the local
    implementation, and keeps an unacknowledged completion until the supervisor
    confirms the exact opaque task token. It is intentionally hidden from the
    public worker API: [Temporal.Worker] supplies the supervisor operations and
    owns lifecycle while this module remains responsible only for typed
    dispatch and lease-safe completion. *)

(** Native operations needed by the adapter. A supervisor implementation owns
    the Rust/Core handle graph and must serialize these calls on its owner
    domain. The adapter never retains the supervisor's error value or native
    handle. *)
module type SUPERVISOR = sig
  type t
  (** Opaque owner-confined supervisor state. *)

  type error
  (** Expected native failure. Accessors below must return bounded, privacy-safe
      diagnostics and must not expose a task token. *)

  val try_poll_activity :
    t -> (Temporal_protocol.Activity_protocol.task option, error) result
  (** Polls at most one activity task. [None] is ordinary nonblocking scheduler
      state; [Some task] transfers one opaque task-token lease to this adapter.
  *)

  val complete_activity :
    t -> Temporal_protocol.Activity_protocol.completion -> (unit, error) result
  (** Submits a semantic completion. [Ok ()] is the only proof that the native
      side accepted and retired the task-token lease. *)

  val error_code : error -> string
  (** Stable classification for a native failure. *)

  val error_message : error -> string
  (** Stable diagnostic for a native failure. Implementations must not include
      payload bytes, credentials, opaque task tokens, or unbounded text. *)
end

type error_view = { code : string; path : string; message : string }
(** Privacy-safe diagnostic returned by the private adapter and suitable for
    logging. Opaque task-token bytes never appear in this record. *)

type registered_activity
(** One heterogeneous, executable activity registration. The existential
    representation keeps the input/output codec relationship intact while
    allowing one registry to contain activities with different OCaml types. *)

(** Kind of a completion accepted by the native supervisor. *)
type completion_kind = Succeeded | Failed | Cancelled

(** Result of one serialized poll/execute/complete transaction.

    [Completed] deliberately reports only non-sensitive summary information;
    callers that need the opaque token for diagnostics should use the native
    supervisor's own correlation logging rather than copying it into OCaml
    state. [Rejected] means the task was acknowledged with a failure completion.
    A supervisor rejection remains a [result] error because lease retirement is
    unproven. *)
type outcome =
  | Not_ready
  | Completed of { activity_type : string option; kind : completion_kind }
  | Rejected of {
      activity_type : string option;
      error : error_view;
      lease_retired : bool;
    }

module Make (Supervisor : SUPERVISOR) : sig
  type t
  (** One owner-confined activity registry. Calls to [poll] are serialized by an
      internal mutex, so independent Domains may ask the adapter to poll; user
      activity implementations still run synchronously in that caller and must
      not call back into this adapter recursively. *)

  val create :
    supervisor:Supervisor.t ->
    activities:registered_activity list ->
    (t, error_view) result
  (** Builds a registry after checking that every definition has a local
      implementation and that Temporal activity names are unique. No native call
      or user implementation runs during creation. *)

  val poll : t -> (outcome, error_view) result
  (** Polls at most one task. A pending completion from an earlier native
      rejection is retried before polling a new task, so an activity is never
      executed twice merely because its completion transport was unavailable.
      [Ok Not_ready] means the native poll had no ready task. *)
end

val register :
  ( 'input,
    'output,
    'input -> ('output, Temporal_base.Error.t) result )
  Temporal_base.Definition.t ->
  registered_activity
(** Wraps a public or internal activity definition without exposing the
    existential constructor used by [Make]. Definitions made with
    [Temporal.Activity.remote] are accepted here only so [create] can return a
    precise configuration error instead of silently dropping them. *)
