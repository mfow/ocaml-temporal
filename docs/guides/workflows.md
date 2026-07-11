# Writing Workflows in OCaml

The public API is exposed beneath the `Temporal` module. Workflow bodies will
be ordinary OCaml functions that return `result`; expected activity, child,
timer, cancellation, and codec failures do not require exceptions.

## Typed payload codecs

Every workflow and activity boundary has an explicit codec. Built-in codecs
cover UTF-8 strings, bytes, unit, and options:

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

Strings use `json/plain`, bytes use `binary/plain`, and unit or `None` use
`binary/null`. `Some value` retains the nested codec's encoding so it remains
interoperable with other Temporal SDKs.

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

Errors are abstract but inspectable through `Temporal.Error.view`, `kind`, and
`message`. This preserves room for compatible internal changes while keeping
application logging and policy decisions stable.

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
`Future.await` returns a `result`: it returns immediately for a ready future or
uses a private OCaml 5 effect to suspend only the current workflow fiber. The
effect constructor and captured continuation are not public API.

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
Calling an operation outside an active workflow returns a structured defect;
the private suspension effect never escapes to application code.

## Current integration boundary

The repository currently proves this API against a synthetic activation
interpreter. It can deterministically emit activity and timer commands, apply
explicitly ordered resolution jobs, suspend and resume OCaml continuations,
tear down cache entries, and replay the same input to the same command bytes.

It does **not yet connect to Temporal Server**. The next phase links the native
OCaml worker to the pinned Rust Temporal Core SDK and replaces synthetic jobs
with serialized Core workflow activations. The public workflow style is
designed to remain the same across that integration.
