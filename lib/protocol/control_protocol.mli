(** Strict private JSON transport between OCaml and the Rust Core bridge.
    Operation-specific modules must validate each body before changing state. *)

val compatibility_version : int32
(** Shared once-per-runtime compatibility number. *)

val max_document_bytes : int
(** Maximum bytes accepted in one JSON control document. The safety ceiling
    includes base64 expansion of pinned Core's default 128 MiB inbound gRPC
    limit plus bounded structural overhead. *)

val max_payload_bytes : int
(** Maximum decoded bytes in one opaque Temporal payload byte field. This is a
    bridge safety limit, not Temporal Server's namespace blob-size policy. *)

(** Stable machine-readable bridge error classifications. *)
type bridge_error_code =
  | Invalid_message
  | Unsupported_message
  | Internal_bridge

type bridge_error = {
  code : bridge_error_code;
  message : string;
  retryable : bool;
}
(** Closed safe error returned to the peer. [message] must never contain raw
    payload bytes. *)

type request = {
  correlation_id : string;
  operation : string;
  body : Yojson.Safe.t;
}
(** Structurally checked request awaiting operation-specific body validation. *)

type response = {
  correlation_id : string;
  operation : string;
  body : Yojson.Safe.t;
}
(** Structurally checked successful response. *)

type error_response = {
  correlation_id : string;
  operation : string;
  error : bridge_error;
}
(** Structurally checked failed response. *)

(** Complete control envelope. *)
type t = Request of request | Response of response | Failed of error_response

type error
(** Opaque validation failure. *)

type error_view = { code : string; path : string; message : string }
(** Public-safe view of a validation failure. *)

val error_view : error -> error_view
(** Returns a copyable view without including source JSON or payload bytes. *)

val check_compatibility : int32 -> (unit, error) result
(** Checks the compatibility number once before native runtime creation. *)

val decode : string -> (t, error) result
(** Strictly decodes one complete envelope. *)

val encode : t -> (string, error) result
(** Validates, normalizes, and independently reparses an outgoing envelope. *)

val decode_object : string -> (Yojson.Safe.t, error) result
(** Strictly decodes one operation-specific JSON object while preserving the
    same duplicate-key, integer, nesting, and resource guarantees as an
    envelope. *)

val encode_object : Yojson.Safe.t -> (string, error) result
(** Validates, normalizes, and independently reparses one outgoing
    operation-specific object. Non-object roots are rejected. *)

(** Decodes a semantic operation object while temporarily allowing the larger
    encoded representation of a maximum-size payload. The semantic caller must
    immediately apply exact shape checks and retain the normal string limit for
    every field except canonical payload data. *)
val decode_payload_object : string -> (Yojson.Safe.t, error) result

(** Encodes and reparses an already semantically validated operation object
    that may contain maximum-size base64 payload strings. *)
val encode_payload_object : Yojson.Safe.t -> (string, error) result
(** Validates, normalizes, and independently reparses one outgoing semantic
    object that may contain maximum-size canonical payload data. *)

val decode_payload : string -> (bytes, error) result
(** Decodes a closed canonical-base64 payload wrapper. *)

val encode_payload : bytes -> (string, error) result
(** Encodes opaque bytes with canonical padded base64 and self-validation. *)
