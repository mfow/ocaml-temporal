(** Rebuilds a public activity description as the private base definition
    consumed by the native registration adapter. Implementations are wrapped so
    public structured errors become base errors only at this boundary. *)

(* Copies codecs and wraps the public implementation so the native adapter
   receives only package-private base values and errors. *)
(** Rebuilds the public activity as the base definition expected by private
    adapters while retaining its typed implementation callback. *)
let to_base (definition : ('input, 'output) Activity.t) =
  let implementation =
    match Activity.implementation_with_context definition with
    | Some implementation ->
        Some (fun context input ->
            Result.map_error Error_private.to_base
              (implementation context input))
    | None ->
        Option.map
          (fun implementation _context input ->
            Result.map_error Error_private.to_base (implementation input))
          (Activity.implementation definition)
  in
  Temporal_base.Definition.make
    ~name:(Activity.name definition)
    ~input:(Codec_private.to_base (Activity.input definition))
    ~output:(Codec_private.to_base (Activity.output definition))
    ~implementation
