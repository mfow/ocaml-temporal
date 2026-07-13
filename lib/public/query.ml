(** Typed query definitions and their read-only handler boundary. *)

type 'output definition = {
  (* The validated query name used by the registry and future native bridge. *)
  name : string;
  (* The result codec retained by the query definition. *)
  output : 'output Codec.t;
}

(* The public [t] alias keeps the definition opaque while avoiding the nested
   [Handler.t] name inside the existential package. *)
type 'output t = 'output definition

(* Keep interaction names aligned with the strict native identifier contract. *)
let max_name_bytes = 65_536

(** Rejects an unsafe query name before a handler or registry can retain it. *)
let validate_name name =
  if String.equal name "" then invalid_arg "Temporal query name is empty";
  if String.contains name '\000' then
    invalid_arg "Temporal query name contains a NUL byte";
  if String.length name > max_name_bytes then
    invalid_arg "Temporal query name exceeds 65536 bytes";
  if not (Temporal_base.Codec.valid_utf_8 name) then
    invalid_arg "Temporal query name must be valid UTF-8"

(** Creates a validated query definition. *)
let define ~name ~output =
  validate_name name;
  { name; output }

(** Returns the stable query name. *)
let name query = query.name

(** Returns the query result codec. *)
let output query = query.output

module Handler = struct
  (** A handler retains its query definition and callback as one existential
      package, preventing output codec mismatches in a registry. *)
  type t = Handler : {
    definition : 'output definition;
    implementation : unit -> ('output, Error.t) result;
  } -> t

  (** Associates a read-only callback with [query]. *)
  let make query implementation = Handler { definition = query; implementation }

  (** Registration-friendly alias for [make]. *)
  let handle = make

  (** Returns the name used by the dispatcher. *)
  let name (Handler { definition; _ }) = definition.name

  (** Runs the query callback and encodes its output. Callback exceptions are
      converted to defects so they cannot tear down the dispatcher. *)
  let dispatch (Handler { definition; implementation }) =
    let result =
      try implementation () with
      | exception_ ->
          Error
            (Error.defect
               ~message:
                 (Printf.sprintf "query handler raised: %s"
                    (Printexc.to_string exception_)))
    in
    match result with
    | Error _ as error -> error
    | Ok value -> Codec.encode definition.output value
end
