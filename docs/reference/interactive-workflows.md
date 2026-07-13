# Signals, queries, and updates

This page explains the first public slice of workflow interactions in the
OCaml API. It is written for someone defining a workflow or a test for the
first time, so it separates three things that are easy to confuse:

| Piece | What it describes | What it does not do yet |
| --- | --- | --- |
| `Signal.t`, `Query.t`, or `Update.t` | A name and the codecs for its typed values | Register anything with a worker or contact Temporal |
| A `Handler.t` | The OCaml function that handles one definition | Make a callback safe to suspend or run concurrently |
| `Temporal.Interaction.t` | An immutable, in-memory dispatcher for local tests | Deliver a message from a live Temporal Server |

The definitions and handler types are available in `Temporal.Signal`,
`Temporal.Query`, and `Temporal.Update`. `Temporal.Interaction` provides the
deterministic local routing path that currently lets those handlers be tested.
The API is experimental while the native delivery path is being built.

## Current status: local handlers and a partial native boundary

`Temporal.Interaction` remains the public, deterministic path for exercising
signal, query, and update handlers. The native bridge now accepts a Core
`SignalWorkflow` activation, validates and copies its ordered payloads,
identity, and headers, and retains it as a runtime job. That transport result
does not yet invoke a public OCaml signal handler: the current execution logs
the absence of a handler and emits no signal-specific command.

Query and update activation jobs are still rejected because their handler
registration, suspension, and completion records have not been implemented.
No local dispatcher test or signal transport test is evidence that a live
Temporal Server can invoke an OCaml interaction handler. The [feature coverage
table](feature-coverage.md) and [implementation roadmap](../implementation-roadmap.md#delivery-order)
record the same experimental status alongside the other SDK capabilities.

## The three-step model

An interaction is assembled in three steps:

1. Define a stable name and the codec for each value that crosses the
   interaction boundary.
2. Pair that definition with a typed OCaml callback to make a handler.
3. Put handlers in an `Interaction` dispatcher, then call the typed operation
   from a test or other local driver.

The definition and handler preserve the relationship between a Temporal name,
its codec, and its OCaml type. A caller cannot accidentally pass a string to a
handler that was defined for bytes without receiving a typed codec error. The
native transport and the planned handler/response lifecycles are described in
the [native interaction design](../design/native-interactions.md); only the
signal activation transport portion is implemented by the worker today.

## Definitions

Each definition pairs a validated Temporal name with the codec for values that
cross the interaction boundary:

```ocaml
let approval =
  Temporal.Signal.define
    ~name:"approval"
    ~input:Temporal.Codec.string

let status =
  Temporal.Query.define
    ~name:"status"
    ~output:Temporal.Codec.string

let add_tool =
  Temporal.Update.define
    ~name:"add-tool"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string
```

The public shapes are:

```text
Signal.define : name:string -> input:'a Codec.t -> 'a Signal.t
Query.define  : name:string -> output:'b Codec.t -> 'b Query.t
Update.define : name:string -> input:'a Codec.t -> output:'b Codec.t
                -> ('a, 'b) Update.t
```

The name must be non-empty, NUL-free, valid UTF-8, and no longer than the
bridge's 65,536-byte identifier limit. Constructing a definition does not
register a worker or contact Temporal. It creates an immutable typed
description that can be reused by ordinary helper functions and handler
registration. An invalid name raises `Invalid_argument` because it is a
programmer or configuration defect detected while building the workflow,
not a routine runtime failure.

Signals carry one input and do not return a result. Queries have no input in
this initial API and return one typed value; a query that needs parameters can
use a record or tuple as its output value, or wait for a future query-input
extension. Updates carry one input, run an optional validator, and return one
typed result.

### Codecs and payloads

A codec is the explicit boundary between an OCaml value and the payload that
the interaction dispatcher routes. On the way into a handler, the dispatcher
encodes the caller's value and the handler decodes that payload. On the way
out, it encodes the handler's result and the caller decodes it with its
definition. Encoding metadata is checked before a decoder runs, so a caller
using a definition with a different encoding receives `Error` instead of
silently interpreting the bytes as another type.

`Temporal.Codec.string` uses Temporal's `json/plain` payload encoding. That is
one payload choice, not a requirement that Temporal or the private OCaml/Rust
bridge use JSON for every message. Applications can define another payload
codec with `Temporal.Codec.make` when the value needs a different encoding.

## Handlers and ordinary helper functions

Handlers are ordinary OCaml functions returning `result` values. They can be
wrapped by normal helpers without a registration-specific class or callback
interface:

```ocaml
let approvals = ref []

let approve state value =
  state := value :: !state;
  Ok ()

let approval_handler =
  Temporal.Signal.Handler.make approval (approve approvals)

let status_handler =
  Temporal.Query.Handler.make status (fun () ->
      Ok (String.concat "," !approvals))

let non_empty_tool_handler update run =
  Temporal.Update.Handler.make
    ~validator:(fun value ->
      if String.equal value "" then
        Error (Temporal.Error.defect ~message:"tool name is empty")
      else Ok ())
    update run

let add_tool_handler =
  non_empty_tool_handler add_tool (fun value ->
    Ok (String.uppercase_ascii value))
```

The `ref` above is deliberately small synthetic-test state. It demonstrates
that a handler can close over ordinary OCaml values; it is not a substitute
for replay-safe workflow state and it is not synchronized for concurrent
Domains. Keep a dispatcher and its callback-owned mutable state on one owning
Domain until native worker scheduling supplies the corresponding ownership
boundary.

The definition and callback are existentially paired inside each handler. A
registry can therefore store handlers for different OCaml types without an
unsafe cast, while each handler still decodes with the codec that belongs to
its definition.

## Dispatch order and results

The local dispatcher follows a fixed sequence. This is the behavior that the
future native activation path must preserve:

| Operation | Local sequence | Result |
| --- | --- | --- |
| Signal | Encode input, find the named handler, decode input, invoke the callback | `(unit, Error.t) result` |
| Query | Find the named handler, invoke its read-only callback, encode its output, decode it with the caller's definition | `('output, Error.t) result` |
| Update | Encode input, find the named handler, decode input, run the validator, invoke the implementation, encode output, decode it with the caller's definition | `('output, Error.t) result` |

An update validator runs after input decoding and before the implementation. A
validator that returns `Error` prevents the implementation from running, so
the rejected request cannot perform an update-side state change. Validators
are intended to be read-only and non-suspending. Query callbacks have the same
read-only, non-suspending contract. The dispatcher cannot make a callback safe
by blocking an OS thread or by silently moving it to another scheduler.

Expected operational failures are returned as `Error error` values. Callback
exceptions are caught at the handler boundary and converted to a non-retryable
`Defect`; they must not escape through the dispatch loop.

## Building and calling a dispatcher

`Interaction.create` copies each handler list into a persistent map. It either
returns a complete dispatcher or returns an error; it never returns a partial
registry:

```ocaml
let run_local_test () =
  match Temporal.Interaction.create
          ~signals:[ approval_handler ]
          ~queries:[ status_handler ]
          ~updates:[ add_tool_handler ]
          () with
  | Error error -> Error error
  | Ok interactions ->
      let accepted = Temporal.Interaction.signal interactions approval "yes" in
      let current = Temporal.Interaction.query interactions status in
      let changed = Temporal.Interaction.update interactions add_tool "search" in
      Ok (accepted, current, changed)
```

In a real test, match each result and assert the expected value or inspect
`Temporal.Error.view error`. The example returns results rather than using
`failwith`, because routine interaction failures belong in the typed error
channel. Exceptions are reserved for programmer defects and violated internal
invariants.

Registration and routing have these rules:

- Names must be unique within signals, within queries, and within updates.
  The same string may be used once in each kind because Temporal treats the
  three kinds as separate namespaces.
- A call made with a definition whose name has no local handler returns a
  non-retryable `Workflow` error for signals and queries, or an `Update` error
  for updates.
- A name match with incompatible encoding metadata returns a typed `Codec`
  error before the callback is invoked.
- A duplicate registration returns a non-retryable `Defect` from
  `Interaction.create`.
- A validator's own `Error` is returned unchanged and its implementation is
  not called. A callback exception is translated to a non-retryable `Defect`.

Calls on one owning Domain are synchronous and are observed in submission
order. `Interaction` does not add a hidden queue, background thread, fiber, or
lock. Its maps are immutable after construction, but callback-owned mutable
state is not synchronized; do not invoke one dispatcher concurrently from
multiple Domains. This local module is a deterministic routing primitive, not
the SDK's supervisor actor or a replacement for the future workflow scheduler.

## Native delivery boundary

Native delivery is a separate protocol milestone. The first signal transport
slice is implemented, but it intentionally stops before handler invocation. It
must still add:

- public signal-handler registration and scheduler delivery, followed by
  activation records for query requests and update requests;
- worker-side registration and completion records for suspended handlers and
  validators;
- scheduler ownership for callbacks, so a Rust thread never calls an OCaml
  closure directly; and
- the same decode, validation, ordering, and error classification rules at
  the OCaml/Rust boundary.

The supervisor remains the sole owner of the Rust handle graph, and native
readiness must be observed through its existing scheduler-safe boundary. Until
those pieces are implemented, a passing `Temporal.Interaction` test or signal
transport test proves only local/bridge behavior; it is not live Temporal
Server acceptance.
