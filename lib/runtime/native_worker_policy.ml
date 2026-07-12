(** Pure lifecycle predicates for the private native worker.

    The pinned Temporal Core revision currently consumes activity completions
    and logs network failures internally. Consequently the only status that
    can authorize a future completion retry is the dedicated bilateral
    [Retryable] category; this module deliberately fails closed for every
    existing generic bridge status. *)

type drain_failure =
  | Workflow_drain
  | Activity_drain of bool

(** Returns whether a native activity-completion error proves that the lease
    remains available for another submission. The current Core bridge can only
    make that claim through the dedicated bilateral status. *)
let activity_completion_retryable = function
  | Temporal_core_bridge.Native_bridge.Retryable -> true
  | _ -> false

(** Returns whether a failed adapter drain can safely reopen worker admission.
    A same-Domain shutdown admission defect is handled before a drain and is
    intentionally separate from this predicate. *)
let shutdown_retryable = function
  | Activity_drain true -> true
  | Workflow_drain | Activity_drain false -> false

(** Permanent drain failures cannot be retried safely, so the native graph must
    be disposed even though the adapter error remains the public result. *)
let needs_native_cleanup failure = not (shutdown_retryable failure)

(** Keeps a terminal adapter failure authoritative while making native cleanup
    observable to the caller through callbacks. The cleanup callback is
    deliberately injected so the policy can be tested without constructing a
    live Temporal worker or depending on a server. *)
let retain_original_error ~cleanup ~on_cleanup_error ~on_cleanup_exception
    original =
  match cleanup () with
  | Ok _ -> (true, original)
  | Error error ->
      on_cleanup_error error;
      (true, original)
  | exception exception_ ->
      on_cleanup_exception exception_;
      (false, original)
