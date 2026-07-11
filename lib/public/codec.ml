(** Re-exports the internal codec implementation without adding another layer
    of allocation or changing its behavior. *)
type payload = Payload.t = { metadata : (string * string) list; data : bytes }
type 'a t = 'a Temporal_base.Codec.t

let make = Temporal_base.Codec.make
let encode = Temporal_base.Codec.encode
let decode = Temporal_base.Codec.decode
let string = Temporal_base.Codec.string
let bytes = Temporal_base.Codec.bytes
let unit = Temporal_base.Codec.unit
let option = Temporal_base.Codec.option
