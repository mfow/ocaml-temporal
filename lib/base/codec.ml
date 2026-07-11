type payload = Payload.t = { metadata : (string * string) list; data : bytes }

type 'a t = {
  encode_payload : 'a -> (payload, Error.t) result;
  decode_payload : payload -> ('a, Error.t) result;
}

let make ~encoding ~encode ~decode =
  let encode_payload value =
    Result.map
      (fun data -> { metadata = [ ("encoding", encoding) ]; data })
      (encode value)
  in
  let decode_payload payload =
    match List.assoc_opt "encoding" payload.metadata with
    | Some actual when String.equal actual encoding -> decode payload.data
    | Some actual ->
        Error
          (Error.codec
             ~message:
               (Printf.sprintf "expected payload encoding %S, received %S"
                  encoding actual))
    | None -> Error (Error.codec ~message:"payload has no encoding metadata")
  in
  { encode_payload; decode_payload }

let encode codec value = codec.encode_payload value
let decode codec payload = codec.decode_payload payload

let valid_utf_8 value =
  let rec loop offset =
    if offset = String.length value then true
    else
      let decoded = String.get_utf_8_uchar value offset in
      Uchar.utf_decode_is_valid decoded
      && loop (offset + Uchar.utf_decode_length decoded)
  in
  loop 0

let hex = "0123456789abcdef"

let encode_json_string value =
  if not (valid_utf_8 value) then
    Error (Error.codec ~message:"OCaml string is not valid UTF-8")
  else
    let buffer = Buffer.create (String.length value + 2) in
    Buffer.add_char buffer '"';
    String.iter
      (function
        | '"' -> Buffer.add_string buffer "\\\""
        | '\\' -> Buffer.add_string buffer "\\\\"
        | '\b' -> Buffer.add_string buffer "\\b"
        | '\012' -> Buffer.add_string buffer "\\f"
        | '\n' -> Buffer.add_string buffer "\\n"
        | '\r' -> Buffer.add_string buffer "\\r"
        | '\t' -> Buffer.add_string buffer "\\t"
        | character when Char.code character < 0x20 ->
            let code = Char.code character in
            Buffer.add_string buffer "\\u00";
            Buffer.add_char buffer hex.[code lsr 4];
            Buffer.add_char buffer hex.[code land 0x0f]
        | character -> Buffer.add_char buffer character)
      value;
    Buffer.add_char buffer '"';
    Ok (Bytes.of_string (Buffer.contents buffer))

let hex_value = function
  | '0' .. '9' as character -> Some (Char.code character - Char.code '0')
  | 'a' .. 'f' as character -> Some (Char.code character - Char.code 'a' + 10)
  | 'A' .. 'F' as character -> Some (Char.code character - Char.code 'A' + 10)
  | _ -> None

let decode_json_string data =
  let source = Bytes.to_string data in
  let length = String.length source in
  let malformed message = Error (Error.codec ~message) in
  let read_hex offset =
    if offset + 4 > length then None
    else
      let rec loop index value =
        if index = offset + 4 then Some value
        else
          match hex_value source.[index] with
          | None -> None
          | Some digit -> loop (index + 1) ((value lsl 4) lor digit)
      in
      loop offset 0
  in
  if length < 2 || source.[0] <> '"' || source.[length - 1] <> '"' then
    malformed "payload is not a JSON string"
  else
    let buffer = Buffer.create (length - 2) in
    let add_scalar scalar =
      if Uchar.is_valid scalar then Buffer.add_utf_8_uchar buffer (Uchar.of_int scalar)
      else invalid_arg "invalid Unicode scalar"
    in
    let rec loop offset =
      if offset = length - 1 then
        let value = Buffer.contents buffer in
        if valid_utf_8 value then Ok value
        else malformed "JSON string contains invalid UTF-8"
      else
        match source.[offset] with
        | character when Char.code character < 0x20 ->
            malformed "JSON string contains an unescaped control character"
        | '"' -> malformed "JSON string contains an unescaped quote"
        | '\\' -> decode_escape (offset + 1)
        | character ->
            Buffer.add_char buffer character;
            loop (offset + 1)
    and decode_escape offset =
      if offset >= length - 1 then malformed "unterminated JSON escape"
      else
        match source.[offset] with
        | '"' | '\\' | '/' as character ->
            Buffer.add_char buffer character;
            loop (offset + 1)
        | 'b' ->
            Buffer.add_char buffer '\b';
            loop (offset + 1)
        | 'f' ->
            Buffer.add_char buffer '\012';
            loop (offset + 1)
        | 'n' ->
            Buffer.add_char buffer '\n';
            loop (offset + 1)
        | 'r' ->
            Buffer.add_char buffer '\r';
            loop (offset + 1)
        | 't' ->
            Buffer.add_char buffer '\t';
            loop (offset + 1)
        | 'u' -> decode_unicode (offset + 1)
        | _ -> malformed "invalid JSON escape"
    and decode_unicode offset =
      match read_hex offset with
      | None -> malformed "invalid JSON Unicode escape"
      | Some high when high >= 0xd800 && high <= 0xdbff ->
          let second = offset + 4 in
          if second + 6 > length || source.[second] <> '\\'
             || source.[second + 1] <> 'u'
          then malformed "high surrogate has no low surrogate"
          else (
            match read_hex (second + 2) with
            | Some low when low >= 0xdc00 && low <= 0xdfff ->
                add_scalar
                  (0x10000 + ((high - 0xd800) lsl 10) + (low - 0xdc00));
                loop (second + 6)
            | _ -> malformed "high surrogate has an invalid low surrogate")
      | Some low when low >= 0xdc00 && low <= 0xdfff ->
          malformed "unexpected low surrogate"
      | Some scalar ->
          add_scalar scalar;
          loop (offset + 4)
    in
    loop 1

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

let option codec =
  {
    encode_payload =
      (function
      | None ->
          Ok { metadata = [ ("encoding", "binary/null") ]; data = Bytes.empty }
      | Some value -> encode codec value);
    decode_payload =
      (fun payload ->
        match List.assoc_opt "encoding" payload.metadata with
        | Some "binary/null" when Bytes.length payload.data = 0 -> Ok None
        | Some "binary/null" ->
            Error (Error.codec ~message:"null payload must be empty")
        | _ -> Result.map Option.some (decode codec payload));
  }
