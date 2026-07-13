(** Exercises typed signal, query, and update registration and dispatch.

    These tests intentionally use the deterministic [Temporal.Interaction]
    dispatcher rather than a Temporal Server. They verify the codec boundary,
    handler ordering, validator short-circuiting, and exception containment
    that the future native activation path must preserve. *)

(** Fails with the SDK's diagnostic when an operation unexpectedly returns an
    error, keeping the assertions concise without hiding the message. *)
let unwrap = function
  | Ok value -> value
  | Error error ->
      failwith ("unexpected SDK error: " ^ Temporal.Error.message error)

(** Requires an error of the requested public category. *)
let expect_error_kind kind = function
  | Error error -> assert (Temporal.Error.kind error = kind)
  | Ok _ -> failwith ("expected " ^ kind ^ " error")

(** Tests substring membership without adding a regular-expression dependency
    to the small unit-test executable. *)
let contains_substring ~needle haystack =
  let needle_length = String.length needle in
  let haystack_length = String.length haystack in
  let rec loop offset =
    if offset + needle_length > haystack_length then false
    else if String.sub haystack offset needle_length = needle then true
    else loop (offset + 1)
  in
  needle_length = 0 || loop 0

(** Requires an error message to retain the useful operation context. *)
let expect_error_message fragment = function
  | Error error ->
      let message = Temporal.Error.message error in
      assert (contains_substring ~needle:fragment message)
  | Ok _ -> failwith ("expected error containing " ^ fragment)

(** Runs the complete typed interaction flow and checks that signals are
    delivered in submission order, queries observe state, and updates run a
    validator before their implementation. *)
let test_dispatch_and_ordering () =
  let values = ref [] in
  let update_calls = ref 0 in
  let signal =
    Temporal.Signal.define ~name:"append-value" ~input:Temporal.Codec.string
  in
  let query =
    Temporal.Query.define ~name:"current-values" ~output:Temporal.Codec.string
  in
  let update =
    Temporal.Update.define ~name:"uppercase" ~input:Temporal.Codec.string
      ~output:Temporal.Codec.string
  in
  let signal_handler =
    Temporal.Signal.Handler.make signal (fun value ->
        values := !values @ [ value ];
        Ok ())
  in
  let query_handler =
    Temporal.Query.Handler.make query (fun () ->
        Ok (String.concat "," !values))
  in
  let update_handler =
    Temporal.Update.Handler.make
      ~validator:(fun value ->
        if String.equal value "" then
          Error
            (Temporal.Error.make ~category:`Update ~non_retryable:true
               ~message:"update input is empty" ())
        else Ok ())
      update (fun value ->
        incr update_calls;
        Ok (String.uppercase_ascii value))
  in
  let dispatcher =
    unwrap
      (Temporal.Interaction.create ~signals:[ signal_handler ]
         ~queries:[ query_handler ] ~updates:[ update_handler ] ())
  in
  unwrap (Temporal.Interaction.signal dispatcher signal "one");
  unwrap (Temporal.Interaction.signal dispatcher signal "two");
  assert (!values = [ "one"; "two" ]);
  assert
    (unwrap (Temporal.Interaction.query dispatcher query) = "one,two");
  expect_error_kind "update"
    (Temporal.Interaction.update dispatcher update "");
  assert (!update_calls = 0);
  assert
    (unwrap (Temporal.Interaction.update dispatcher update "ready") = "READY");
  assert (!update_calls = 1)

(** Duplicate names are rejected while building a dispatcher, and unknown
    definitions fail before any callback can run. *)
let test_registration_and_missing_handlers () =
  let signal =
    Temporal.Signal.define ~name:"duplicate" ~input:Temporal.Codec.string
  in
  let handler = Temporal.Signal.Handler.make signal (fun _ -> Ok ()) in
  expect_error_kind "defect"
    (Temporal.Interaction.create ~signals:[ handler; handler ] ());
  let dispatcher = unwrap (Temporal.Interaction.create ()) in
  let missing =
    Temporal.Signal.define ~name:"not-registered" ~input:Temporal.Codec.string
  in
  expect_error_message "unregistered signal handler"
    (Temporal.Interaction.signal dispatcher missing "value")

(** A definition with the same name but a different encoding cannot silently
    cross the handler boundary; the handler's codec reports the mismatch. *)
let test_codec_mismatch () =
  let registered =
    Temporal.Signal.define ~name:"codec-boundary" ~input:Temporal.Codec.string
  in
  let handler = Temporal.Signal.Handler.make registered (fun _ -> Ok ()) in
  let dispatcher =
    unwrap (Temporal.Interaction.create ~signals:[ handler ] ())
  in
  let caller =
    Temporal.Signal.define ~name:"codec-boundary" ~input:Temporal.Codec.bytes
  in
  expect_error_kind "codec"
    (Temporal.Interaction.signal dispatcher caller (Bytes.of_string "raw"))

(** Handler exceptions become typed defects rather than escaping through the
    dispatcher, which protects a future worker poll loop from user mistakes. *)
let test_exception_containment () =
  let query =
    Temporal.Query.define ~name:"raises" ~output:Temporal.Codec.string
  in
  let handler =
    Temporal.Query.Handler.make query (fun () -> failwith "query bug")
  in
  let dispatcher = unwrap (Temporal.Interaction.create ~queries:[ handler ] ()) in
  expect_error_kind "defect" (Temporal.Interaction.query dispatcher query)

(** Runs all interaction assertions. *)
let () =
  test_dispatch_and_ordering ();
  test_registration_and_missing_handlers ();
  test_codec_mismatch ();
  test_exception_containment ()
