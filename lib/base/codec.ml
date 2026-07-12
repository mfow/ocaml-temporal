type payload = Payload.t = { metadata : (string * string) list; data : bytes }

(** Stores the two operations that every codec must provide. Keeping these
    functions private ensures every decode checks the payload's encoding name. *)
type 'a t = {
  encode_payload : 'a -> (payload, Error.t) result;
  decode_payload : payload -> ('a, Error.t) result;
}

(** Finds the optional encoding marker while enforcing the object semantics of
    payload metadata. The base representation uses a list so unknown entries
    survive translation, but the JSON wire representation still requires every
    metadata name to be unique. *)
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

(** Builds a Temporal payload codec from two functions supplied by the caller:
    one that converts an OCaml value to bytes and one that converts bytes back
    to an OCaml value. On encode, it adds [("encoding", encoding)] to the
    payload metadata. On decode, it calls the byte decoder only when the
    payload contains that exact encoding value; other unique metadata entries
    do not affect decoding. Duplicate metadata names are rejected before the
    callback runs, matching the strict bridge protocol. *)
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

(** Runs a codec's encoding callback without exposing its private record
    representation to callers, retaining payload ownership at the codec
    boundary. *)
let encode codec value = codec.encode_payload value

(* Applies the codec's decoding callback to one payload. *)
let decode codec payload = codec.decode_payload payload

(** Installs already validated payload-level callbacks without adding an
    encoding metadata entry. The public API uses this narrow bridge to turn an
    opaque public codec into the representation consumed by worker adapters;
    keeping it here avoids exposing the record representation. *)
let of_payload ~encode ~decode =
  { encode_payload = encode; decode_payload = decode }

(** Checks every byte of a string with OCaml's UTF-8 decoder. Advancing by the
    length of each decoded character ensures invalid continuation bytes are not
    accidentally skipped. *)
let valid_utf_8 value =
  let rec loop offset =
    if offset = String.length value then true
    else
      let decoded = String.get_utf_8_uchar value offset in
      Uchar.utf_decode_is_valid decoded
      && loop (offset + Uchar.utf_decode_length decoded)
  in
  loop 0

(** Converts an OCaml string to the JSON representation used in a Temporal
    [json/plain] payload. Yojson handles quoting and escaping. The explicit
    UTF-8 check prevents malformed strings from being stored by Temporal. *)
let encode_json_string value =
  if not (valid_utf_8 value) then
    Error (Error.codec ~message:"OCaml string is not valid UTF-8")
  else Ok (Bytes.of_string (Yojson.Safe.to_string (`String value)))

(** Reads one complete JSON value with Yojson and accepts it only when it is a
    string. Objects, arrays, numbers, malformed JSON, and trailing input are
    reported as codec errors instead of raising exceptions to workflow code. *)
let decode_json_string data =
  try
    match Yojson.Safe.from_string (Bytes.to_string data) with
    | `String value when valid_utf_8 value -> Ok value
    | `String _ -> Error (Error.codec ~message:"JSON string contains invalid UTF-8")
    | _ -> Error (Error.codec ~message:"payload is not a JSON string")
  with Yojson.Json_error message ->
    Error (Error.codec ~message:("invalid JSON string: " ^ message))

(** Uses the encoding name understood by the standard converters in other
    Temporal SDKs. *)
let string = make ~encoding:"json/plain" ~encode:encode_json_string ~decode:decode_json_string

let bytes =
  make ~encoding:"binary/plain"
    ~encode:(fun value -> Ok (Bytes.copy value))
    ~decode:(fun value -> Ok (Bytes.copy value))

let unit =
  make ~encoding:"binary/null" ~encode:(fun () -> Ok Bytes.empty)
    ~decode:(fun data ->
      if Bytes.length data = 0 then Ok ()
      else Error (Error.codec ~message:"unit payload must be empty"))

(** Handles [None] itself, and uses the supplied codec only for [Some value]. A
    null payload containing data is rejected because the representation of
    [None] must be empty. *)
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
