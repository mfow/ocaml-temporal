(** These operators are direct aliases to the standard [result] semantics, so
    helper functions compose without any hidden workflow control effect. *)
let ( let* ) = Result.bind
let ( let+ ) value map = Result.map map value
