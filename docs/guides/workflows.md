# Writing workflows in OCaml

The public API is the `Temporal` module. A workflow body is an ordinary OCaml
function from a typed input to `('output, Temporal.Error.t) result`. Activities
are ordinary OCaml functions with the same result-oriented shape. The SDK uses
private OCaml 5 algebraic effects to suspend a workflow fiber while a future is
pending; application code never handles the effect or a saved continuation.

This guide shows the API that compiles today and labels its execution boundary
honestly:

| Target | What it is useful for today |
| --- | --- |
| `mock://...` | Fast deterministic unit tests for client/worker registration and dispatch. The pure runtime tests also exercise timers, activities, child scheduling, replay, cancellation, and future combinators without a server. |
| `http://...` or `https://...` | The OCaml-owned native client/worker path backed by Rust Temporal Core. The current native command slice handles activity, timer, terminal, cancellation, cache, and two-stage child-resolution paths. It is covered by focused bridge and adapter tests. |
| Live Compose acceptance | Real PostgreSQL and Temporal Server lifecycle validation. It does not yet run the two-OCaml-binary workflow-result scaffold. |

Child-workflow code is valid in the synthetic runtime and the semantic command
translator. The native adapter also represents the complete two-stage
resolution lifecycle: a successful start acknowledgment records the child run
ID, and a later terminal resolution resumes the parent future. Focused tests
cover success, start failure, final-before-start, duplicate sequences, and
lease retirement. The live Compose acceptance is still pending, so this is not
yet a claim of end-to-end Temporal Server compatibility.

## 1. Write a deterministic OCaml function

Start with ordinary functions and return expected failures as values:

```ocaml
let normalize name = String.trim name

let greeting input =
  let name = normalize input in
  if String.equal name "" then
    Error (Temporal.Error.defect ~message:"name must not be empty")
  else
    Ok ("Hello, " ^ name)

let greeting_workflow =
  Temporal.Workflow.define
    ~name:"greeting"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string
    greeting
```

`Workflow.define` pairs the stable Temporal workflow type name with input and
output codecs and the local implementation. `normalize` is just a helper
function; it needs no registration or special syntax. `Workflow.remote` makes
a typed reference to workflow code owned by another worker and has no local
implementation, so it cannot be registered in `Temporal.Worker.create`.
`Temporal.Result_syntax` supplies `let*` for sequencing these ordinary
`result` values; it does not introduce a second effect system.

Workflow code must be deterministic during replay. Do not read the wall clock,
use random values, perform filesystem or network I/O, inspect process-global
mutable state, or rely on unordered iteration. Use SDK operations for durable
time and for work that must appear in Temporal history. Activities are the
place for nondeterministic or external work such as calling an LLM.

## 2. Use typed codecs

Temporal stores values as payloads: bytes plus metadata naming the encoding.
Temporal does not require JSON and does not inspect the payload body. The
built-in codecs are:

- `Temporal.Codec.string`, using the interoperable `json/plain` encoding;
- `Temporal.Codec.bytes`, using `binary/plain`;
- `Temporal.Codec.unit`, using `binary/null`; and
- `Temporal.Codec.option codec`, which uses the nested codec for `Some` and
  `binary/null` for `None`.

JSON here is a payload choice, not the private OCaml/Rust bridge protocol and
not the format sent to Temporal Server. The bridge's Rust side converts its
strict semantic JSON records to Temporal Core protobuf; see the [protocol
reference](../reference/core-protocol.md).

Codec operations return `result`, because a remote payload can be malformed or
encoded with the wrong name:

```ocaml
let encode_prompt prompt =
  Temporal.Codec.encode Temporal.Codec.string prompt

let decode_prompt payload =
  Temporal.Codec.decode Temporal.Codec.string payload
```

Define a custom deterministic codec when another encoding is more appropriate:

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

Changing a codec for an existing workflow is a compatibility change: old
history can contain payloads written by the previous codec.

## 3. Schedule activities and wait directly

Define a local activity for the worker that will execute it, or a remote
reference for a workflow that only schedules it:

```ocaml
let summarize =
  Temporal.Activity.remote
    ~name:"summarize"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

let summarize_document document =
  let open Temporal.Result_syntax in
  let summary = Temporal.Activity.start summarize document in
  let timer = Temporal.Workflow.start_sleep (Temporal.Duration.of_ms 10L) in
  let* summary, () =
    Temporal.Future.await (Temporal.Future.both summary timer)
  in
  Ok summary

let summarize_workflow =
  Temporal.Workflow.define
    ~name:"summarize_document"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string
    summarize_document
```

`Activity.start` emits a command and returns a future immediately. Starting
the timer before waiting demonstrates the important pattern: schedule
independent work first, then await a combined future. `Activity.execute` is the
short form for start followed by `Future.await`.

Activity scheduling accepts labelled options for a stable activity ID, task
queue, timeout values, cancellation policy, and eager-execution preference.
Invalid identifiers, payloads, or options produce a typed future error before
a history command is emitted. `Temporal.Workflow.start_sleep` creates a durable
timer without waiting; `Temporal.Workflow.sleep` is the start-and-wait form.

## 4. Combine futures

A `Temporal.Future.t` belongs to the workflow execution that created it. It is
not a general-purpose operating-system promise. The common combinators are:

```ocaml
let await_both first second =
  Temporal.Future.both first second |> Temporal.Future.await

let await_all pending =
  Temporal.Future.all pending |> Temporal.Future.await

let await_fastest left right =
  Temporal.Future.race left right |> Temporal.Future.await
```

`both` and `all` wait for every input and preserve deterministic input ordering;
they do not cancel siblings implicitly. `race` can combine different output
types and returns `Left value` or `Right value`. `first` is the homogeneous
non-empty-list form. An error is a completion, so it may win a race. Futures
from different workflow executions return a ready structured defect rather than
silently sharing scheduler state.

When a future is not ready, `Future.await` suspends only the current workflow
fiber. Other runnable workflow fibers and the worker process can continue. The
effect machinery is private, so workflow authors write direct-style OCaml.

## 5. Child workflows: authoring versus native support

Child-workflow references use the same typed shape as activities:

```ocaml
let review =
  Temporal.Workflow.remote
    ~name:"review_document"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

let start_review document =
  Temporal.Child_workflow.start
    ~id:"document-review"
    review
    document
```

The ID is durable Temporal identity. It must be non-empty, valid UTF-8, free of
NUL bytes, and within the bridge's bounded length. `Child_workflow.execute`
starts and waits in one call.

The definitions and calls above compile, and the synthetic runtime tests cover
child scheduling and deterministic future resolution. The native protocol and
worker adapter now also carry the child-start acknowledgment and terminal
resolution required to resume the parent. The adapter rejects final-before-
start, duplicate, and unknown sequences as typed bridge failures, preserving
the parent lease rather than acknowledging an unsafe completion. Focused tests
cover the complete lifecycle, but the live acceptance test still needs to
exercise it against Temporal Server.

## 6. Compose ordinary helpers

Workflow starters and futures are ordinary values. Helpers can accept or return
them without registration or a special SDK base class:

```ocaml
let fan_out starters input =
  List.map (fun start -> start input) starters
  |> Temporal.Future.all

let fastest left right input =
  Temporal.Future.race (left input) (right input)

let run_helpers input =
  let starts =
    [ (fun value -> Temporal.Activity.start summarize value);
      (fun value ->
        Temporal.Activity.start summarize (value ^ ":backup")) ]
  in
  fan_out starts input |> Temporal.Future.await
```

These helpers still make their callers' Temporal boundaries visible: the
caller chooses when each operation starts and when to await it. Calling a
normal OCaml helper does not create a history event.

## 7. Register a worker

The worker registration boundary packs heterogeneous typed definitions while
keeping each implementation and its codecs together:

```ocaml
let summarize_activity =
  Temporal.Activity.define
    ~name:"summarize"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string
    (fun input -> Ok input)

let worker_result =
  Temporal.Worker.create
    ~target_url:"http://127.0.0.1:7233"
    ~namespace:"default"
    ~task_queue:"summaries"
    ~workflows:[ Temporal.Worker.workflow summarize_workflow ]
    ~activities:[ Temporal.Worker.activity summarize_activity ]
    ()
  |> Result.bind Temporal.Worker.run
```

Use `http://` or `https://` for a real native worker. `mock://` is a private,
deterministic test backend and does not contact Temporal Server. Registration
rejects duplicate names and remote-only definitions before a native graph is
created. `Temporal.Worker.run` is a blocking lifecycle loop; call it from an
ordinary dedicated OCaml Domain or system thread rather than directly from a
cooperative Eio/Lwt scheduler fiber. `Temporal.Worker.shutdown` is idempotent
and drains retryable completions before releasing the native graph.

The native path keeps Rust/Core and its protobufs private. The OCaml worker
receives validated semantic activations, runs the typed function, and returns a
validated semantic completion through the supervisor.

## 8. Start and wait from a client

`Temporal.Client` is useful when an application needs to submit an execution
but does not itself run workflow code. It retains the exact workflow ID and
server-issued run ID:

```ocaml
let result =
  let open Temporal.Result_syntax in
  let* client =
    Temporal.Client.create
      ~target_url:"http://127.0.0.1:7233"
      ~namespace:"default"
      ()
  in
  let* handle =
    Temporal.Client.start client
      ~workflow:summarize_workflow
      ~task_queue:"summaries"
      ~id:"summary-1"
      ~input:"document"
      ()
  in
  Temporal.Client.wait handle
```

`Temporal.Client.wait` does not silently follow continue-as-new. It returns a
typed terminal outcome so the application can decide whether to follow the
successor. Pass a stable `~request_id` when retrying an uncertain start; reuse
that ID only for the same logical start. As with the worker, expected failures
are `result` values. Exceptions are reserved for programmer defects and are
contained at the worker boundary.

## 9. Validate locally

From the repository root, the focused Make targets are:

```sh
make test-unit
make test-runtime
make verify
make test-temporal-integration
```

The first two use the deterministic test seams and do not require a running
server. The integration target starts real PostgreSQL and Temporal Server,
checks the schemas and frontend, runs the OCaml-owned Core lifecycle
executable, and cleans its Compose volume. It is not yet the two-process
workflow-result test described in the [acceptance design](../reference/two-ocaml-binary-e2e-acceptance.md).

For the complete ownership and protocol rules, read the [runtime
invariants](../reference/runtime-invariants.md), [Core bridge reference](../reference/core-bridge.md),
and [native worker execution reference](../reference/native-worker-execution.md).
