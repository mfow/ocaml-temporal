(** Internal representation of a Temporal payload. The public [Payload] module
    exposes the same two fields so callers can pass raw payloads without seeing
    a codec's private implementation. *)
type t = { metadata : (string * string) list; data : bytes }
