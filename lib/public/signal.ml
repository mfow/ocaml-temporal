(** Typed signal definitions and their deterministic handler boundary. *)

type 'input definition = {
  (* The validated Temporal signal name retained for registration and lookup. *)
  name : string;
  (* The codec that owns the signal's payload representation. *)
  input : 'input Codec.t;
}

(* The public [t] alias keeps the definition opaque while allowing the nested
   handler to name the definition without colliding with [Handler.t]. *)
type 'input t = 'input definition

(* Temporal's closed semantic identifier contract uses this same byte ceiling
   for workflow, activity, and interaction names. *)
let max_name_bytes = 65_536

(** Validates one interaction name before it can enter a registry or payload
    boundary. Keeping this check local means definitions remain safe even when
    they are used without a worker or dispatcher. *)
let validate_name name =
  if String.equal name "" then invalid_arg "Temporal signal name is empty";
  if String.contains name '\000' then
    invalid_arg "Temporal signal name contains a NUL byte";
  if String.length name > max_name_bytes then
    invalid_arg "Temporal signal name exceeds 65536 bytes";
  if not (Temporal_base.Codec.valid_utf_8 name) then
    invalid_arg "Temporal signal name must be valid UTF-8"

(** Creates a validated signal definition without allocating any runtime state. *)
let define ~name ~input =
  validate_name name;
  { name; input }

(** Returns the stable signal name. *)
let name signal = signal.name

(** Returns the signal input codec. *)
let input signal = signal.input

module Handler = struct
  (** A handler keeps the definition and callback existentially paired so a
      registry cannot accidentally decode an input with another codec. *)
  type t = Handler : {
    definition : 'input definition;
    implementation : 'input -> (unit, Error.t) result;
  } -> t

  (** Builds a handler whose callback is associated with [signal]'s codec. *)
  let make signal implementation = Handler { definition = signal; implementation }

  (** Registration-friendly alias for [make]. *)
  let handle = make

  (** Returns the name used by the interaction dispatcher. *)
  let name (Handler { definition; _ }) = definition.name

  (** Decodes and invokes one signal payload, converting callback exceptions to
      non-retryable defects at the same boundary as workflow dispatch. *)
  let dispatch (Handler { definition; implementation }) payload =
    match Codec.decode definition.input payload with
    | Error error -> Error error
    | Ok input -> (
        try implementation input with
        | exception_ ->
            Error
              (Error.defect
                 ~message:
                   (Printf.sprintf "signal handler raised: %s"
                      (Printexc.to_string exception_))))
end
