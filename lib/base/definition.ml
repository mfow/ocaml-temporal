type ('input, 'output, 'implementation) t = {
  name : string;
  input : 'input Codec.t;
  output : 'output Codec.t;
  implementation : 'implementation option;
}

let validate_name name =
  if String.length name = 0 then invalid_arg "Temporal definition name is empty";
  if String.contains name '\000' then
    invalid_arg "Temporal definition name contains a NUL byte"

let make ~name ~input ~output ~implementation =
  validate_name name;
  { name; input; output; implementation }

let name definition = definition.name
let input definition = definition.input
let output definition = definition.output
let implementation definition = definition.implementation
