(** Public aliases expose inspection without allowing record construction to
    bypass the base error constructors. *)
type category = Temporal_base.Error.category

type view = Temporal_base.Error.view = {
  category : category;
  message : string;
  non_retryable : bool;
  details : Payload.t list;
}

type t = Temporal_base.Error.t

let view = Temporal_base.Error.view
let kind = Temporal_base.Error.kind
let message = Temporal_base.Error.message
let codec = Temporal_base.Error.codec
let defect = Temporal_base.Error.defect
