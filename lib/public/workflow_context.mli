(** Returns [true] only while the current Domain is running workflow code under
    an activation. It is intended for diagnostics and internal guard checks. *)
val is_active : unit -> bool
