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

Workflow definitions, activities, futures, and direct-style suspension are
introduced by the next runtime milestones; their APIs will build on these same
codec and error contracts.
