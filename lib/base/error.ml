(** Lists the broad error categories exposed by the public SDK. Keep it in sync
    with the public interface and with conversions from Temporal failures. *)
type category =
  [ `Activity
  | `Bridge
  | `Cancelled
  | `Child_workflow
  | `Codec
  | `Defect
  | `Nexus
  | `Terminated
  | `Timeout
  | `Update
  | `Workflow ]

type view = {
  category : category;
  message : string;
  non_retryable : bool;
  details : Payload.t list;
}

(** The internal representation currently matches the fields visible through
    [view]. Public callers still use accessors, so this may change later. *)
type t = view

(** Creates an error with the common defaults: retryable and without details. *)
let make ?(non_retryable = false) ?(details = []) ~category ~message () =
  { category; message; non_retryable; details }

let view error = error

(** Converts a category to the lowercase name used in logs and metrics. *)
let kind error =
  match error.category with
  | `Activity -> "activity"
  | `Bridge -> "bridge"
  | `Cancelled -> "cancelled"
  | `Child_workflow -> "child_workflow"
  | `Codec -> "codec"
  | `Defect -> "defect"
  | `Nexus -> "nexus"
  | `Terminated -> "terminated"
  | `Timeout -> "timeout"
  | `Update -> "update"
  | `Workflow -> "workflow"

(** Provides the common field access and constructors used throughout the SDK. *)
let message error = error.message
let codec ~message = make ~category:`Codec ~message ()
let defect ~message = make ~non_retryable:true ~category:`Defect ~message ()
