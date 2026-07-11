(** Stores everything needed to register or call a workflow or activity. The
    codecs live beside the name so registration and invocation cannot choose
    different payload formats. *)
type ('input, 'output, 'implementation) t = {
  name : string;
  input : 'input Codec.t;
  output : 'output Codec.t;
  implementation : 'implementation option;
}

(** Rejects an empty name or a name containing a NUL byte before any worker is
    started or command is sent. *)
let validate_name name =
  if String.length name = 0 then invalid_arg "Temporal definition name is empty";
  if String.contains name '\000' then
    invalid_arg "Temporal definition name contains a NUL byte"

(** Validates the name and then stores the definition fields. *)
let make ~name ~input ~output ~implementation =
  validate_name name;
  { name; input; output; implementation }

(** Returns individual fields while allowing public modules to keep the record
    itself private. *)
let name definition = definition.name
let input definition = definition.input
let output definition = definition.output
let implementation definition = definition.implementation
