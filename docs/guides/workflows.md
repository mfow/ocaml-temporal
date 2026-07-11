# Writing Workflows in OCaml

The public API is under the `Temporal` module. Workflow bodies are ordinary
OCaml functions that return `result`. Expected failures—such as an activity
failure, cancellation, timeout, or invalid payload—are values rather than
exceptions.

This guide describes the API that compiles today. The current runtime uses a
synthetic activation interpreter for tests and is not yet a production worker
connected to Temporal Server. Child invocation and future aggregation compile
and run against that interpreter; cancellation scopes and live Core worker
wiring remain future work. The pure-OCaml command translator already preserves
the complete activity record and validates it before the native boundary.

## Typed payload codecs

Temporal stores inputs and results as payloads: bytes plus metadata naming the
encoding. Temporal does not require JSON and does not interpret the bytes. A
codec is the OCaml code that converts between a typed value and that payload.

Every workflow and activity definition chooses its codecs explicitly. The
built-in codecs cover UTF-8 strings, bytes, unit, and options:

```ocaml
let encode_prompt prompt =
  Temporal.Codec.encode Temporal.Codec.string prompt

let decode_prompt payload =
  Temporal.Codec.decode Temporal.Codec.string payload
```

`Codec.encode` and `Codec.decode` return `result`, because user codecs and
untrusted remote payloads can fail. A custom codec supplies an encoding name
and byte-level conversion functions:

```ocaml
let positive_integer =
  Temporal.Codec.make
    ~encoding:"example/positive-integer"
    ~encode:(fun value ->
      if value > 0 then Ok (Bytes.of_string (string_of_int value))
      else Error (Temporal.Error.codec ~message:"integer must be positive"))
    ~decode:(fun bytes ->
      match int_of_string_opt (Bytes.to_string bytes) with
      | Some value when value > 0 -> Ok value
      | _ -> Error (Temporal.Error.codec ~message:"invalid positive integer"))
```

Strings use the optional `json/plain` codec, implemented with Yojson, because
that encoding is understood by standard converters in other Temporal SDKs.
Bytes use `binary/plain`, and unit or `None` use `binary/null`. `Some value`
uses the supplied nested codec. Applications may define Protobuf or another
deterministic binary codec instead of JSON.

Changing a codec after workflows have started is a compatibility change:
workers must still be able to decode payloads already recorded in workflow
history.

## Explicit error composition

Open `Temporal.Result_syntax` to compose fallible helpers in direct OCaml
style:

```ocaml
let decode_then_validate payload =
  let open Temporal.Result_syntax in
  let* value = Temporal.Codec.decode positive_integer payload in
  let+ doubled = Ok (value * 2) in
  doubled
```

Use `Temporal.Error.view`, `kind`, or `message` to inspect an error. The type is
kept abstract so the SDK can add internal detail without forcing application
code to construct error records itself.

## Definitions and ordinary helpers

A definition gives Temporal a stable type name and codecs while leaving the
implementation as a normal OCaml function:

```ocaml
let normalize name = String.trim name
let greet name = "Hello, " ^ normalize name

let greeting_workflow input = Ok (greet input)

let greeting =
  Temporal.Workflow.define
    ~name:"greeting"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string
    greeting_workflow
```

`normalize` and `greet` need no registration, SDK type, or special syntax.
Calling a helper is an ordinary in-process function call and does not create a
Temporal history boundary. Only explicit activity or child-workflow operations
will create those boundaries.

Use `Activity.remote` or `Workflow.remote` to declare code implemented by
another worker while retaining typed inputs and outputs:

```ocaml
let call_llm =
  Temporal.Activity.remote
    ~name:"call_llm"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string
```

Definition names must be non-empty and cannot contain NUL bytes. Invalid names
raise `Invalid_argument` during worker configuration because they are
programmer defects, not workflow execution failures.

## Futures and direct-style waiting

Temporal operations return typed `('value, 'error) Temporal.Future.t` values.
`Future.await` returns a `result`. If the result is not ready, the SDK uses an
OCaml 5 algebraic effect to pause the current workflow fiber. Other runnable
workflow fibers and the worker process can continue. Application code never
handles the effect or the saved continuation directly.

Futures support ordinary typed composition. `both` and `all` wait for every
input and never cancel siblings implicitly. `all` preserves input order even
when completions arrive in another order. If several inputs fail, it waits for
all of them and returns the first error in input order:

```ocaml
let await_pair first second =
  Temporal.Future.both
    (Temporal.Future.map String.length first)
    second
  |> Temporal.Future.await

let await_all pending =
  Temporal.Future.all pending
  |> Temporal.Future.await
```

`Future.race left right` returns `Left value` or `Right value` for differently
typed inputs. `Future.first leading rest` selects from a non-empty homogeneous
collection. Both settle on the first completion, including an error, and leave
losers running. If inputs are already ready, the left `race` argument or
`first` list order wins; otherwise the deterministic scheduler's callback order
wins. Explicit structured-cancellation scopes will be added later.

Combining futures from different workflow executions returns a ready structured
defect. It does not raise an exception. `Future.all []` is immediately `Ok []`;
inside workflow code it belongs to that execution and can be combined with its
other futures.

## Activities, child workflows, timers, and concurrent scheduling

`Activity.start` emits a command immediately and returns before the remote
activity completes. Start independent work first, then await the combined
future:

```ocaml
let enrich document =
  let open Temporal.Result_syntax in
  let summary = Temporal.Activity.start summarize document in
  let entities = Temporal.Activity.start extract_entities document in
  let* summary, entities =
    Temporal.Future.await (Temporal.Future.both summary entities)
  in
  let* () = Temporal.Workflow.sleep (Temporal.Duration.of_ms 10L) in
  Ok (summary, entities)
```

`Activity.execute definition input` is the convenience form of `start`
followed by `Future.await`. Both return expected failures through `result`.
Calling an operation outside an active workflow returns a structured defect.
The internal suspension effect never escapes to application code.

Both activity functions also accept labelled scheduling options. Supply
`~activity_id` when the activity needs a stable, application-chosen identity;
otherwise the runtime derives a deterministic ID from the command sequence.
`~task_queue` overrides the execution's queue, while omitting it uses the
queue captured when the execution started (the synthetic interpreter defaults
to `"default"`). Timeout labels use `Temporal.Duration.t` and are encoded as
exact integer milliseconds. If neither schedule-to-close nor start-to-close
is supplied, the SDK uses a deterministic 60-second start-to-close default;
the command translator rejects a schedule that would leave both absent.
`~heartbeat_timeout`, cancellation policy, and eager-execution preference map
directly to Temporal's activity command fields. Empty, non-UTF-8, NUL-bearing,
or overlong identifiers fail as typed workflow errors before a command is
emitted, so a validation failure cannot consume a sequence number.

Child workflows follow the same start-now or execute-and-wait pattern. Their
ID is mandatory because it is durable Temporal identity, not a private local
counter:

```ocaml
let review =
  Temporal.Workflow.remote
    ~name:"review_document"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

let summarize_and_review document =
  let open Temporal.Result_syntax in
  let summary = Temporal.Activity.start summarize document in
  let review =
    Temporal.Child_workflow.start
      ~id:"document-review"
      review
      document
  in
  let* summary, review =
    Temporal.Future.await (Temporal.Future.both summary review)
  in
  Ok (summary, review)
```

`Child_workflow.start ~id definition input` returns immediately;
`Child_workflow.execute` starts and waits. An ID must be non-empty, valid UTF-8,
and no more than 65,536 UTF-8 bytes, which is the bridge's bounded-string safety
ceiling. An invalid ID or input codec failure returns a typed failed future
without emitting a history command or consuming a command sequence. The SDK
does not invent child IDs, because a process-local counter cannot safely
represent durable identity across replay and retries.

Use `Workflow.start_sleep duration` to create a durable timer without waiting,
or `Workflow.sleep duration` for the common start-and-wait form. A zero duration
returns a ready future and emits no timer command. Starting several activities,
children, or timers before awaiting creates deterministic concurrency: command
order follows the workflow's OCaml call order, while completion order comes
from recorded history.

## Higher-order workflow helpers

Starters are ordinary functions and can be accepted or returned by application
helpers. No registration, special syntax, or SDK base class is needed:

```ocaml
let fan_out starters input =
  List.map (fun start -> start input) starters
  |> Temporal.Future.all

let fastest left right input =
  Temporal.Future.race (left input) (right input)

let enrich document =
  let summary = Temporal.Activity.start summarize in
  let review = Temporal.Child_workflow.start ~id:"review-1" review in
  fan_out [ summary; review ] document
  |> Temporal.Future.await
```

These helpers compose futures only; they do not hide a Temporal command or
create a new replay boundary. Their callers still choose exactly when each
activity, child, or timer starts and when to wait.

## Client and worker lifecycle

The public package also has typed client and worker values. A client starts an
execution with an explicit workflow ID and task queue, then waits for the exact
workflow/run pair returned by the server:

```ocaml
let client_result =
  let open Temporal.Result_syntax in
  let* client =
    Temporal.Client.create ~target_url:"http://127.0.0.1:7233"
      ~namespace:"default" ()
  in
  let* handle =
    Temporal.Client.start client ~workflow:greeting_workflow
      ~task_queue:"greetings" ~id:"greeting-1" ~input:"Ada" ()
  in
  Temporal.Client.wait handle
```

For a start whose network outcome may need to be reconciled, pass a stable
Temporal idempotency key with `~request_id:"greeting-start-1"` and use the same
value if the application retries that logical start. If the argument is
omitted, the SDK creates a fresh key for the call. The key is kept unchanged
while the native supervisor polls the asynchronous start ticket.

The worker registration list packs heterogeneous typed definitions at the
registration boundary while keeping each implementation and its codecs
together. Workflow and activity bodies remain ordinary OCaml functions:

```ocaml
let worker_result =
  Temporal.Worker.create ~target_url:"http://127.0.0.1:7233"
    ~namespace:"default" ~task_queue:"greetings"
    ~workflows:[ Temporal.Worker.workflow greeting_workflow ]
    ~activities:[] ()
  |> Result.bind Temporal.Worker.run
```

The public lifecycle surface is intentionally independent of native handles or
Temporal protobufs. HTTP(S) clients now route start and exact-run waits through
the private Rust/Core supervisor, with bounded native waits and typed JSON
validation at the OCaml boundary. The `mock://` endpoint remains a private,
deterministic seam for unit tests. Native worker polling and completion are
being connected separately; they use distinct activation/task types and an
explicit admission, shutdown, and finalization lifecycle.

## Current integration boundary

The workflow interpreter is still tested against a synthetic activation
interpreter. It can deterministically emit activity, child-workflow, and timer
commands, apply explicitly ordered resolution jobs, suspend and resume OCaml
continuations, aggregate futures, tear down cache entries, and replay the same
input to the same command bytes. The public client path is connected to the
pinned Rust Temporal Core SDK, but the two-process worker acceptance path is
not yet enabled; it will replace synthetic jobs with serialized Core workflow
activations while preserving the same public workflow style.
