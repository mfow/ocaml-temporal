(** Re-exports the internal payload record directly. This avoids copying the
    metadata and bytes merely to cross between public and internal modules. *)
type t = Temporal_base.Payload.t = {
  metadata : (string * string) list;
  data : bytes;
}
