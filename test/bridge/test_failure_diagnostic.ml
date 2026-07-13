(** Regression tests for the shared structured-failure diagnostic.

    These tests construct the same nested shape that Temporal sends for an
    activity timeout: an outer activity wrapper, a timeout record, and the
    application failure beneath it. No server is needed because the property
    under test is the deterministic protocol-to-text traversal. *)

module Protocol = Temporal_protocol.Workflow_protocol
module Diagnostic = Temporal_protocol.Failure_diagnostic

(** Finds a stable fragment without depending on optional String extensions. *)
let contains source needle =
  let source_length = String.length source in
  let needle_length = String.length needle in
  let rec loop index =
    if index + needle_length > source_length then false
    else if String.sub source index needle_length = needle then true
    else loop (index + 1)
  in
  if needle_length = 0 then true else loop 0

(** Builds one failure layer with explicit metadata so the test remains
    independent of JSON decoding and exercises the closed protocol type. *)
let layer ~message ~source ~info ~cause : Protocol.failure =
  {
    message;
    source;
    stack_trace = "";
    encoded_attributes = None;
    cause;
    info;
  }

(** A timeout nested below an activity wrapper must remain visible in the
    public diagnostic instead of being replaced by the outer summary. *)
let test_nested_timeout_is_visible () =
  let detail : Protocol.payload =
    { metadata = []; data = Bytes.of_string "heartbeat" }
  in
  let application =
    layer ~message:"llm call failed" ~source:"activity-worker"
      ~info:(Protocol.Application
               {
                 type_name = "LlmUnavailable";
                 non_retryable = false;
                 details = [ detail ];
               })
      ~cause:None
  in
  let timeout =
    layer ~message:"activity timed out" ~source:"temporal-core"
      ~info:(Protocol.Timeout_failure
               {
                 timeout_type = Protocol.Timeout_start_to_close;
                 last_heartbeat_details = [ detail ];
               })
      ~cause:(Some application)
  in
  let outer =
    layer ~message:"workflow failed" ~source:"temporal-core"
      ~info:(Protocol.Activity
               {
                 scheduled_event_id = 12L;
                 started_event_id = 13L;
                 identity = "worker-1";
                 activity_type = "llm.call";
                 activity_id = "activity-1";
                 retry_state = Protocol.Timeout;
               })
      ~cause:(Some timeout)
  in
  let diagnostic = Diagnostic.failure_diagnostic outer in
  assert (contains diagnostic "workflow failed source=temporal-core");
  assert (contains diagnostic "timeout type=start_to_close last_heartbeat_details=1");
  assert (contains diagnostic "application type=LlmUnavailable non_retryable=false details=1");
  assert (contains diagnostic " | ")

(** A recursively constructed value cannot make diagnostics grow without a
    bound, even though normal JSON decoding already rejects excessive depth. *)
let test_depth_is_bounded () =
  let rec make depth : Protocol.failure =
    let cause = if depth = 0 then None else Some (make (depth - 1)) in
    layer ~message:"nested" ~source:"test"
      ~info:(Protocol.Application
               { type_name = "Nested"; non_retryable = false; details = [] })
      ~cause
  in
  let diagnostic = Diagnostic.failure_diagnostic (make 130) in
  assert (contains diagnostic "cause_depth_limit_reached")

(** Runs the pure diagnostic regression cases. *)
let () =
  test_nested_timeout_is_visible ();
  test_depth_is_bounded ()
