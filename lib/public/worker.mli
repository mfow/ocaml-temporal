(** Worker registration and execution for OCaml workflow and activity code.

    Definitions are packed existentially only at the registration boundary;
    workflow bodies and activity bodies remain ordinary typed OCaml functions. *)

(** A heterogeneous workflow registration item. The existential package keeps
    each definition's input and output codecs paired with its implementation. *)
type registered_workflow

(** Packs a typed workflow definition for a worker registration list. [signals]
    attach scheduler handlers for matching native signal activations; [queries]
    attach synchronous read-only handlers for matching query requests; [updates]
    attach typed non-suspending update handlers for matching update activations. *)
val workflow :
  ?signals:Signal.Handler.t list ->
  ?queries:Query.Handler.t list ->
  ?updates:Update.Handler.t list ->
  ('input, 'output) Workflow.t -> registered_workflow

(** A heterogeneous activity registration item. *)
type registered_activity

(** Packs a typed activity definition for a worker registration list. *)
val activity : ('input, 'output) Activity.t -> registered_activity

(** Immutable resource settings for one native Temporal worker.

    These settings bound worker-side Core resources; they are not workflow
    configuration and do not change the deterministic meaning of workflow
    code or its replayed history. Construct a value with [make] before passing
    it to [create]. The private OCaml/Rust bridge validates the same values
    again immediately before native worker construction. *)
module Options : sig
  (** A validated, immutable collection of worker resource limits. The record
      representation is private so callers cannot bypass the relationships
      required by Temporal Core. *)
  type t

  (** The production resource settings used when [Worker.create] receives no
      explicit [~options]. They preserve the SDK's established defaults: a
      1,000-entry sticky cache, 1,000 admitted workflow tasks, two workflow
      task pollers, and a 30-second graceful shutdown period. *)
  val default : t

  (** Validates and creates worker resource settings.

      [max_cached_workflows] permits zero to disable the sticky workflow
      cache. A non-zero cache requires at least two
      [max_concurrent_workflow_task_polls], matching Temporal Core's safety
      requirement. [max_outstanding_workflow_tasks] and the poller count must
      both be positive. The graceful shutdown duration is inclusive from zero
      to 24 hours. Invalid settings return a typed [Error.t] before any native
      runtime, client, or worker resource is allocated. *)
  val make :
    ?max_cached_workflows:int ->
    ?max_outstanding_workflow_tasks:int ->
    ?max_concurrent_workflow_task_polls:int ->
    ?graceful_shutdown_timeout:Duration.t ->
    unit ->
    (t, Error.t) result

  (** Returns the maximum number of sticky workflow executions retained by
      this worker. Zero means that Core evicts each workflow after its task. *)
  val max_cached_workflows : t -> int

  (** Returns the maximum number of workflow tasks admitted by this worker at
      one time. This is a worker throughput bound, not a workflow limit. *)
  val max_outstanding_workflow_tasks : t -> int

  (** Returns the number of concurrent workflow-task pollers owned by Core.
      A worker with a non-zero sticky cache always has at least two. *)
  val max_concurrent_workflow_task_polls : t -> int

  (** Returns the period during which [Worker.shutdown] asks Core to drain
      work before final teardown. *)
  val graceful_shutdown_timeout : t -> Duration.t
end

(** An opaque worker instance owning one supervisor/backend graph and two
    deterministic registration maps. *)
type t

(** Creates and validates a worker. Duplicate names and remote-only definitions
    return typed defects before any backend graph is allocated. [options]
    defaults to [Options.default] and is validated before allocation. A
    [mock://] target selects the deterministic test backend; an [http://] or
    [https://] target creates the OCaml-owned native Core worker and its
    private Rust bridge. *)
val create :
  ?identity:string ->
  ?options:Options.t ->
  target_url:string ->
  namespace:string ->
  task_queue:string ->
  workflows:registered_workflow list ->
  activities:registered_activity list ->
  unit ->
  (t, Error.t) result

(** Runs the workflow and activity poll loops until [shutdown] is requested.
    Each accepted task is decoded, dispatched to its registered OCaml function,
    encoded, and completed before the next task is admitted. This is a blocking
    call: invoke it from an ordinary dedicated Domain or system thread, not
    directly on a cooperative Eio/Lwt scheduler fiber. Native readiness waits
    release the OCaml runtime lock and return periodically so shutdown cannot
    be stranded, but releasing that lock does not make [run] non-blocking. *)
val run : t -> (unit, Error.t) result

(** Initiates graceful worker shutdown. Repeated calls are safe and return the
    same cached terminal result. A permanent native teardown error is retained
    so later callers observe [Error] rather than a spurious [Ok]. Retryable
    failures leave the worker open for another attempt. *)
val shutdown : t -> (unit, Error.t) result
