(** Stores everything needed to register or call a workflow or activity. The
    codecs live beside the name so registration and invocation cannot choose
    different payload formats. *)
type ('input, 'output, 'implementation) t = {
  name : string;
  input : 'input Codec.t;
  output : 'output Codec.t;
  implementation : 'implementation option;
}

(** Maximum byte length accepted by the closed JSON/native identifier contract. *)
let max_name_bytes = 65_536

(** Rejects a name that cannot cross the protocol before any worker is started
    or command is sent. Base definitions are also constructed by private
    adapters, so they repeat the public validation instead of trusting only
    the public constructors. *)
let validate_name name =
  if String.length name = 0 then invalid_arg "Temporal definition name is empty";
  if String.contains name '\000' then
    invalid_arg "Temporal definition name contains a NUL byte"
  else if String.length name > max_name_bytes then
    invalid_arg "Temporal definition name exceeds 65536 bytes"
  else if not (Codec.valid_utf_8 name) then
    invalid_arg "Temporal definition name must be valid UTF-8"

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
