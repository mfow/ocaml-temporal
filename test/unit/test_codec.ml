(** Turns an unexpected structured codec error into a readable test failure. *)
let fail_error error = failwith (Temporal.Error.message error)

(** Extracts a successful test result or fails with its SDK diagnostic. *)
let unwrap = function Ok value -> value | Error error -> fail_error error

let () =
  let value = "agent \"one\"\n\t\001" in
  let payload = unwrap (Temporal.Codec.encode Temporal.Codec.string value) in
  assert (List.assoc "encoding" payload.metadata = "json/plain");
  assert (Temporal.Codec.decode Temporal.Codec.string payload = Ok value);
  let wrong : Temporal.Payload.t =
    {
      metadata = [ ("encoding", "binary/plain") ];
      data = Bytes.of_string "agent";
    }
  in
  (match Temporal.Codec.decode Temporal.Codec.string wrong with
  | Error error -> assert (Temporal.Error.kind error = "codec")
  | Ok _ -> failwith "wrong encoding accepted");
  let escaped : Temporal.Payload.t =
    {
      metadata = [ ("encoding", "json/plain") ];
      data = Bytes.of_string "\"quote: \\\" slash: \\\\ newline: \\n\"";
    }
  in
  assert
    (Temporal.Codec.decode Temporal.Codec.string escaped
    = Ok "quote: \" slash: \\ newline: \n");
  let unicode : Temporal.Payload.t =
    {
      metadata = [ ("encoding", "json/plain") ];
      data = Bytes.of_string "\"\\uD83D\\uDE00\"";
    }
  in
  assert (Temporal.Codec.decode Temporal.Codec.string unicode = Ok "😀");
  let object_payload : Temporal.Payload.t =
    {
      metadata = [ ("encoding", "json/plain") ];
      data = Bytes.of_string "{\"value\":\"not a string\"}";
    }
  in
  assert
    (Result.is_error
       (Temporal.Codec.decode Temporal.Codec.string object_payload));
  let trailing_json : Temporal.Payload.t =
    {
      metadata = [ ("encoding", "json/plain") ];
      data = Bytes.of_string "\"value\" false";
    }
  in
  assert
    (Result.is_error
       (Temporal.Codec.decode Temporal.Codec.string trailing_json));
  let invalid_utf_8 = String.make 1 (Char.chr 0xff) in
  assert
    (Result.is_error
       (Temporal.Codec.encode Temporal.Codec.string invalid_utf_8));
  let original = Bytes.of_string "payload" in
  let binary = unwrap (Temporal.Codec.encode Temporal.Codec.bytes original) in
  Bytes.set original 0 'X';
  assert (Bytes.to_string binary.data = "payload");
  let optional = Temporal.Codec.option Temporal.Codec.string in
  let none_payload = unwrap (Temporal.Codec.encode optional None) in
  assert (List.assoc "encoding" none_payload.metadata = "binary/null");
  assert (Temporal.Codec.decode optional none_payload = Ok None);
  let some_payload = unwrap (Temporal.Codec.encode optional (Some "value")) in
  assert (Temporal.Codec.decode optional some_payload = Ok (Some "value"))
