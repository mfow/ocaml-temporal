(** Implements the public codec contract without exposing the private base
    codec record. A codec owns complete payload-level callbacks, which lets
    value-dependent encodings such as [option] preserve their metadata exactly
    while private adapters later convert the callbacks to base codecs. *)
type payload = Payload.t = { metadata : (string * string) list; data : bytes }

type 'a t = {
  encode_payload : 'a -> (payload, Error.t) result;
  decode_payload : payload -> ('a, Error.t) result;
}

(** Finds the optional encoding marker while enforcing the object semantics of
    payload metadata. The public record uses a list so callers can preserve
    metadata they do not recognize, but the wire representation is a JSON
    object and therefore cannot contain duplicate names. *)
let encoding_metadata metadata =
  let seen = Hashtbl.create 16 in
  let rec loop encoding = function
    | [] -> Ok encoding
    | (key, value) :: rest ->
        if Hashtbl.mem seen key then
          Error (Error.codec ~message:"payload metadata contains a duplicate key")
        else (
          Hashtbl.add seen key ();
          loop
            (if String.equal key "encoding" then Some value else encoding)
            rest)
  in
  loop None metadata

(** Builds a codec that writes one exact encoding name and validates that name
    before invoking the decoder. User conversion failures remain typed errors,
    so malformed data never escapes as an exception. Duplicate metadata names
    are rejected before the callback runs, matching the strict bridge protocol. *)
let make ~encoding ~encode ~decode =
  let encode_payload value =
    Result.map
      (fun data -> { metadata = [ ("encoding", encoding) ]; data })
      (encode value)
  in
  let decode_payload payload =
    match encoding_metadata payload.metadata with
    | Error error -> Error error
    | Ok (Some actual) when String.equal actual encoding -> decode payload.data
    | Ok (Some actual) ->
        Error
          (Error.codec
             ~message:
               (Printf.sprintf "expected payload encoding %S, received %S"
                  encoding actual))
    | Ok None -> Error (Error.codec ~message:"payload has no encoding metadata")
  in
  { encode_payload; decode_payload }

(** Serializes a value while preserving the codec's ownership boundary. *)
let encode codec value = codec.encode_payload value

(** Validates metadata and deserializes one payload. *)
let decode codec payload = codec.decode_payload payload

(** Checks every byte of a string as a complete UTF-8 sequence. *)
let valid_utf_8 value =
  let rec loop offset =
    if offset = String.length value then true
    else
      let decoded = String.get_utf_8_uchar value offset in
      Uchar.utf_decode_is_valid decoded
      && loop (offset + Uchar.utf_decode_length decoded)
  in
  loop 0

(** Encodes a string as Temporal's interoperable [json/plain] payload. *)
let encode_json_string value =
  if not (valid_utf_8 value) then
    Error (Error.codec ~message:"OCaml string is not valid UTF-8")
  else Ok (Bytes.of_string (Yojson.Safe.to_string (`String value)))

(** Decodes exactly one JSON string and reports malformed or non-string JSON as
    a typed codec error. Yojson performs the JSON grammar validation. *)
let decode_json_string data =
  try
    match Yojson.Safe.from_string (Bytes.to_string data) with
    | `String value when valid_utf_8 value -> Ok value
    | `String _ -> Error (Error.codec ~message:"JSON string contains invalid UTF-8")
    | _ -> Error (Error.codec ~message:"payload is not a JSON string")
  with Yojson.Json_error message ->
    Error (Error.codec ~message:("invalid JSON string: " ^ message))

let string =
  make ~encoding:"json/plain" ~encode:encode_json_string ~decode:decode_json_string

(** Copies mutable byte buffers on both sides of the codec boundary. *)
let bytes =
  make ~encoding:"binary/plain"
    ~encode:(fun value -> Ok (Bytes.copy value))
    ~decode:(fun value -> Ok (Bytes.copy value))

(** Represents [()] as an empty [binary/null] payload and rejects unexpected
    bytes when decoding. *)
let unit =
  make ~encoding:"binary/null" ~encode:(fun () -> Ok Bytes.empty)
    ~decode:(fun data ->
      if Bytes.length data = 0 then Ok ()
      else Error (Error.codec ~message:"unit payload must be empty"))

(** Uses a null payload for [None] and delegates [Some] values to the supplied
    codec. The complete callback representation is retained instead of
    assuming that one option codec has one fixed encoding name. *)
let option codec =
  {
    encode_payload =
      (function
      | None ->
          Ok { metadata = [ ("encoding", "binary/null") ]; data = Bytes.empty }
      | Some value -> encode codec value);
    decode_payload =
      (fun payload ->
        match encoding_metadata payload.metadata with
        | Error error -> Error error
        | Ok (Some "binary/null") when Bytes.length payload.data = 0 -> Ok None
        | Ok (Some "binary/null") ->
            Error (Error.codec ~message:"null payload must be empty")
        | Ok _ -> Result.map Option.some (decode codec payload));
  }
