type ('input, 'output) implementation =
  'input -> ('output, Error.t) result

type ('input, 'output) t =
  ( 'input,
    'output,
    ('input, 'output) implementation )
  Temporal_base.Definition.t

let define ~name ~input ~output implementation =
  Temporal_base.Definition.make ~name ~input ~output
    ~implementation:(Some implementation)

let remote ~name ~input ~output =
  Temporal_base.Definition.make ~name ~input ~output ~implementation:None

let name = Temporal_base.Definition.name
