type ('input, 'output) t

val start :
  ( 'input,
    'output,
    'input -> ('output, Temporal_base.Error.t) result )
  Temporal_base.Definition.t ->
  'input ->
  ('input, 'output) t

val activate : ('input, 'output) t -> Activation.job list -> Activation.command list
