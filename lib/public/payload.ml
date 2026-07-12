(** Owns the public payload record. Keeping this record independent from the
    private protocol representation means an installed consumer never needs a
    private CMI merely to construct or inspect a payload. Boundary adapters
    make an explicit copy when converting it to native values. *)
type t = {
  metadata : (string * string) list;
  data : bytes;
}
