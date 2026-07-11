# Writing Workflows in OCaml

The public API is under the `Temporal` module. Workflow bodies are ordinary
OCaml functions that return `result`. Expected failures—such as an activity
failure, cancellation, timeout, or invalid payload—are values rather than
exceptions.

This guide describes the API that compiles today. The current runtime uses a
synthetic activation interpreter for tests and is not yet a production worker
connected to Temporal Server. Planned child-workflow and structured-concurrency
APIs are identified as future work.

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

Futures support ordinary typed composition:

```ocaml
let await_pair first second =
  Temporal.Future.both
    (Temporal.Future.map String.length first)
    second
  |> Temporal.Future.await
```

`Future.both` observes both inputs before it settles and does not implicitly
cancel a sibling when one side fails. Later child-workflow APIs will supply
explicit structured-cancellation scopes.

## Activities, timers, and concurrent scheduling

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

## Current integration boundary

The repository currently proves this API against a synthetic activation
interpreter. It can deterministically emit activity and timer commands, apply
explicitly ordered resolution jobs, suspend and resume OCaml continuations,
tear down cache entries, and replay the same input to the same command bytes.

It does **not yet connect to Temporal Server**. The next phase links the native
OCaml worker to the pinned Rust Temporal Core SDK and replaces synthetic jobs
with serialized Core workflow activations. The public workflow style is
designed to remain the same across that integration.
