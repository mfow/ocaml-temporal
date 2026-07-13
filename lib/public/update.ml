(** Typed update definitions and deterministic validator/handler dispatch. *)

type ('input, 'output) definition = {
  (* The validated update name used in registration and routing. *)
  name : string;
  (* Codec for the request payload. *)
  input : 'input Codec.t;
  (* Codec for the successful update result. *)
  output : 'output Codec.t;
}

(* The public [t] alias keeps the definition opaque while avoiding the nested
   [Handler.t] name inside the existential package. *)
type ('input, 'output) t = ('input, 'output) definition

(* This bound matches the bridge's identifier safety contract. *)
let max_name_bytes = 65_536

(** Validates one update name before any handler can retain it. *)
let validate_name name =
  if String.equal name "" then invalid_arg "Temporal update name is empty";
  if String.contains name '\000' then
    invalid_arg "Temporal update name contains a NUL byte";
  if String.length name > max_name_bytes then
    invalid_arg "Temporal update name exceeds 65536 bytes";
  if not (Temporal_base.Codec.valid_utf_8 name) then
    invalid_arg "Temporal update name must be valid UTF-8"

(** Creates a validated update definition. *)
let define ~name ~input ~output =
  validate_name name;
  { name; input; output }

(** Returns the stable update name. *)
let name update = update.name

(** Returns the update input codec. *)
let input update = update.input

(** Returns the update output codec. *)
let output update = update.output

module Handler = struct
  (** An update handler keeps validator and implementation types paired with
      the same definition inside one existential package. *)
  type t = Handler : {
    definition : ('input, 'output) definition;
    validator : ('input -> (unit, Error.t) result) option;
    implementation : 'input -> ('output, Error.t) result;
  } -> t

  (** Builds a handler with an optional precondition validator. *)
  let make ?validator definition implementation =
    Handler { definition; validator; implementation }

  (** Registration-friendly alias for [make]. *)
  let handle = make

  (** Returns the name used by the dispatcher. *)
  let name (Handler { definition; _ }) = definition.name

  (** Performs the strict input-decode, validator, implementation, and output
      encode sequence. Each callback boundary contains exceptions as defects;
      importantly, the implementation is not called when validation fails.
      Both codec calls are contained the same way as the validator and
      implementation callbacks, so a raising codec cannot escape dispatch
      half-applied. *)
  let dispatch ?(run_validator = true)
      (Handler { definition; validator; implementation }) payload =
    match Codec.decode definition.input payload with
    | exception exception_ ->
        Error
          (Error.defect
             ~message:
               (Printf.sprintf "update input codec raised: %s"
                  (Printexc.to_string exception_)))
    | Error error -> Error error
    | Ok input -> (
        let validation_result =
          if not run_validator then Ok ()
          else
            match validator with
            | None -> Ok ()
            | Some validate -> (
                try validate input with
                | exception_ ->
                    Error
                      (Error.defect
                         ~message:
                           (Printf.sprintf "update validator raised: %s"
                              (Printexc.to_string exception_))))
        in
        match validation_result with
        | Error _ as error -> error
        | Ok () -> (
            let result =
              try implementation input with
              | exception_ ->
                  Error
                    (Error.defect
                       ~message:
                         (Printf.sprintf "update handler raised: %s"
                            (Printexc.to_string exception_)))
            in
            match result with
            | Error _ as error -> error
            | Ok output -> (
                match Codec.encode definition.output output with
                | result -> result
                | exception exception_ ->
                    Error
                      (Error.defect
                         ~message:
                           (Printf.sprintf "update output codec raised: %s"
                              (Printexc.to_string exception_))))))
end
