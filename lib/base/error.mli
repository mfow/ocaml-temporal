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

type t

val make :
  ?non_retryable:bool ->
  ?details:Payload.t list ->
  category:category ->
  message:string ->
  unit ->
  t

val view : t -> view
val kind : t -> string
val message : t -> string
val codec : message:string -> t
val defect : message:string -> t
