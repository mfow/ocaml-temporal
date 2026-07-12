(** Rebuilds a public workflow description as the private base definition
    consumed by Core and the native worker. The adapter performs every codec
    and error conversion at registration time, before any native handle exists. *)

(* Copies codecs and wraps the public implementation so Core receives only
   private base values and errors at registration time. *)
(** Rebuilds a public workflow as the base definition expected by private
    adapters while retaining its typed implementation callback. *)
let to_base (definition : ('input, 'output) Workflow.t) =
  let implementation =
    Option.map
      (fun implementation input ->
        Result.map_error Error_private.to_base (implementation input))
      (Workflow.implementation definition)
  in
  Temporal_base.Definition.make
    ~name:(Workflow.name definition)
    ~input:(Codec_private.to_base (Workflow.input definition))
    ~output:(Codec_private.to_base (Workflow.output definition))
    ~implementation
