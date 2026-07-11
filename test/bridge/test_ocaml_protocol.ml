module Protocol = Temporal_protocol.Control_protocol

(** Reads a complete fixture as binary-safe text and closes the descriptor on
    both successful and exceptional paths. *)
let read_file path =
  let channel = open_in_bin path in
  Fun.protect
    ~finally:(fun () -> close_in_noerr channel)
    (fun () -> really_input_string channel (in_channel_length channel))

(** Resolves a fixture beneath Dune's copied source tree. *)
let fixture parts =
  List.fold_left Filename.concat "fixtures/protocol" parts |> read_file

(** Fails the test with a stable rendering of a structured protocol error. *)
let unwrap = function
  | Ok value -> value
  | Error error ->
      let view = Protocol.error_view error in
      Alcotest.failf "%s at %s: %s" view.code view.path view.message

(** Requires a result to fail without inspecting potentially sensitive input. *)
let require_error = function
  | Error _ -> ()
  | Ok _ -> Alcotest.fail "expected protocol validation to fail"

(** Proves valid shared envelopes normalize and survive a typed round trip. *)
let test_valid_envelopes () =
  List.iter
    (fun name ->
      let input = fixture [ "valid"; name ^ ".input.json" ] in
      let expected =
        String.trim (fixture [ "valid"; name ^ ".normalized.json" ])
      in
      let decoded = unwrap (Protocol.decode input) in
      Alcotest.(check string)
        (name ^ " normalization") expected
        (unwrap (Protocol.encode decoded));
      ignore (unwrap (Protocol.decode expected)))
    [ "request"; "response"; "error"; "unicode" ]

(** Proves every malformed shared envelope is rejected, including duplicate
    members that an ordinary association-map decoder could silently replace. *)
let test_invalid_envelopes () =
  List.iter
    (fun name ->
      require_error (Protocol.decode (fixture [ "invalid"; name ^ ".json" ])))
    [
      "duplicate-envelope";
      "duplicate-body";
      "missing-field";
      "unknown-field";
      "wrong-type";
      "invalid-correlation";
      "unknown-kind";
      "non-integral-number";
      "integer-out-of-range";
      "error-unknown-field";
    ]

(** Exercises the standalone canonical payload wrapper without rendering raw
    bytes in a failure message. *)
let test_payloads () =
  let input = fixture [ "valid"; "payload.input.json" ] in
  let expected = String.trim (fixture [ "valid"; "payload.normalized.json" ]) in
  let bytes = unwrap (Protocol.decode_payload input) in
  Alcotest.(check int) "decoded length" 5 (Bytes.length bytes);
  Alcotest.(check string)
    "normalized payload" expected
    (unwrap (Protocol.encode_payload bytes));
  let all_bytes = Bytes.init 256 Char.chr in
  Alcotest.(check int)
    "all byte values" 256
    (Bytes.length
       (unwrap
          (Protocol.decode_payload (unwrap (Protocol.encode_payload all_bytes)))));
  require_error
    (Protocol.decode_payload
       (fixture [ "invalid"; "payload-invalid-base64.json" ]));
  require_error
    (Protocol.decode_payload
       (fixture [ "invalid"; "payload-unknown-field.json" ]))

(** Generates resource-limit attacks locally so the repository does not carry
    megabyte-sized fixtures. *)
let test_resource_limits () =
  let prefix =
    {|{"kind":"request","correlation_id":"0123456789abcdef0123456789abcdef","operation":"worker.poll","body":|}
  in
  let deep = prefix ^ String.make 17 '[' ^ String.make 17 ']' ^ "}" in
  let long_string =
    prefix ^ "{\"value\":\"" ^ String.make 65_537 'a' ^ "\"}}"
  in
  let long_array =
    prefix ^ "{\"values\":["
    ^ String.concat "," (List.init 257 (Fun.const "null"))
    ^ "]}}"
  in
  require_error (Protocol.decode deep);
  require_error (Protocol.decode long_string);
  require_error (Protocol.decode long_array);
  require_error
    (Protocol.decode (String.make (Protocol.max_document_bytes + 1) ' '));
  require_error
    (Protocol.encode_payload
       (Bytes.make (Protocol.max_payload_bytes + 1) '\000'))

(** Verifies the single startup compatibility gate and outgoing self-validation
    of typed values constructed by internal callers. *)
let test_compatibility_and_outgoing_validation () =
  ignore (unwrap (Protocol.check_compatibility Protocol.compatibility_version));
  require_error (Protocol.check_compatibility Int32.max_int);
  let invalid =
    Protocol.Request
      {
        correlation_id = "not-a-correlation-id";
        operation = "worker.poll";
        body = `Assoc [];
      }
  in
  require_error (Protocol.encode invalid)

let () =
  Alcotest.run "private JSON control protocol"
    [
      ( "conformance",
        [
          Alcotest.test_case "valid envelopes" `Quick test_valid_envelopes;
          Alcotest.test_case "invalid envelopes" `Quick test_invalid_envelopes;
          Alcotest.test_case "payloads" `Quick test_payloads;
          Alcotest.test_case "resource limits" `Quick test_resource_limits;
          Alcotest.test_case "compatibility and outgoing validation" `Quick
            test_compatibility_and_outgoing_validation;
        ] );
    ]
