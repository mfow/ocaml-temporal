type t = Temporal_base.Payload.t = {
  metadata : (string * string) list;
      (** Codec metadata, including the mandatory [encoding] entry. *)
  data : bytes;
      (** Opaque payload bytes. *)
}
