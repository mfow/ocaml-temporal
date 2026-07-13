# Signals, queries, and updates

This page describes the first public slice of workflow interactions. The
definitions and handler types are available in `Temporal.Signal`,
`Temporal.Query`, and `Temporal.Update`. `Temporal.Interaction` is a
deterministic in-memory dispatcher used by unit tests and by future worker
integration.

The native activation protocol does not deliver these interactions yet. The
current Rust/Core adapter still rejects signal, query, and update activation
jobs. This page therefore documents a stable OCaml typing and ordering
contract, not a claim that a live Temporal Server can send an interaction to a
workflow. Native delivery will reuse these definitions after the bilateral
activation/completion protocol is extended.

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

The name must be non-empty, NUL-free, valid UTF-8, and no longer than the
bridge's 65,536-byte limit. Constructing a definition does not register a
worker or contact Temporal. It only creates an immutable typed description that
can be reused by helper functions and handler registration.

Signals carry one input and do not return a result. Queries have no input in
this initial API and return one typed value; callers that need parameters can
make the query output a record containing the relevant state or use a future
query-input extension. Updates carry one input, run an optional validator, and
return one typed result.

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

let add_tool_handler =
  Temporal.Update.Handler.make
    ~validator:(fun value ->
      if String.equal value "" then
        Error (Temporal.Error.defect ~message:"tool name is empty")
      else Ok ())
    add_tool (fun value -> Ok (String.uppercase_ascii value))
```

The definition and callback are existentially paired inside each handler. A
registry therefore cannot accidentally use one callback with another codec.
The callback's expected operational failures remain `Error error` values.
Unexpected exceptions are caught at the handler boundary and converted to a
non-retryable `Defect`; they must not tear down the worker dispatch loop.

Update validators run after input decoding and before the implementation. A
validator that returns `Error` prevents the implementation from running and
therefore cannot perform an update-side state change. Validators are intended
to be read-only and non-suspending. The future native scheduler will reject
command-producing operations from query and validator modes rather than
blocking an OS thread.

## Deterministic dispatcher

The initial local dispatcher is immutable after construction:

```ocaml
let interactions =
  match Temporal.Interaction.create
          ~signals:[ approval_handler ]
          ~queries:[ status_handler ]
          ~updates:[ add_tool_handler ]
          () with
  | Ok dispatcher -> dispatcher
  | Error error -> failwith (Temporal.Error.message error)

let send value = Temporal.Interaction.signal interactions approval value
let read () = Temporal.Interaction.query interactions status
let change value = Temporal.Interaction.update interactions add_tool value
```

Construction rejects duplicate names within each interaction kind. A signal,
query, and update may use the same name because they occupy separate Temporal
namespaces. Dispatch encodes input before routing, looks up by the definition's
name, invokes the paired handler synchronously in the current Domain, and
decodes the output with the caller's definition. Calls made by one caller are
therefore observed in submission order; the dispatcher does not add a hidden
queue or a background thread.

An unknown name returns a non-retryable `Workflow` error for signals and
queries, or an `Update` error for updates. A name match with incompatible
encoding metadata returns a typed `Codec` error. These failures are explicit
`result` values, so a caller can choose whether to fail a synthetic test or
translate the failure into a Temporal workflow outcome later.

## Native delivery boundary

The dispatcher is deliberately separate from the native Core bridge. Future
native work must add activation records for signal delivery, query requests,
and update requests, plus completion records for suspended handlers and update
validators. The supervisor remains the sole owner of the Rust handle graph;
Rust threads must never call an OCaml closure directly. Interaction callbacks
will run on the owning workflow scheduler, preserving replay determinism and
the same validator-before-handler ordering described here.

Until that protocol milestone is complete, use `Temporal.Interaction` for
deterministic handler tests and do not describe a local dispatcher run as live
Temporal Server coverage.
