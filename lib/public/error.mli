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

type t = Temporal_base.Error.t

val view : t -> view
(** Return the stable, inspectable representation of an SDK error. *)

val kind : t -> string
(** Lowercase stable category name, suitable for diagnostics and metrics. *)

val message : t -> string
val codec : message:string -> t
val defect : message:string -> t
