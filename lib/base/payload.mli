(** One value in the serialized form stored by Temporal. [metadata] describes
    how [data] was encoded. The list keeps entries the SDK does not recognize,
    allowing newer senders to add metadata without losing it. *)
type t = { metadata : (string * string) list; data : bytes }
