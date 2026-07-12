# Writing workflows in OCaml

The public API is the `Temporal` module. You write a workflow as an ordinary
OCaml function from a typed input to `('output, Temporal.Error.t) result`, then
register that function with a `Temporal.Workflow.t`. Activities use the same
function shape. There is no workflow base class, callback interface, or special
syntax to learn.

The one unusual part is waiting. A call such as `Temporal.Future.await` looks
like a normal function call, but when its value is not ready the SDK suspends
only the current workflow fiber. It does not block an operating-system thread,
and the workflow author never handles an effect constructor or a saved
continuation. OCaml 5 algebraic effects implement that suspension privately.

This guide describes the API that compiles today and labels its execution
boundary honestly:

| Target | What it is useful for today |
| --- | --- |
| `mock://...` | Fast deterministic unit tests for client/worker registration and dispatch. The pure runtime tests also exercise timers, activities, child scheduling, replay, cancellation, and future combinators without a server. |
| `http://...` or `https://...` | The OCaml-owned native client/worker path backed by Rust Temporal Core. The current native command slice handles activity and timer work plus terminal, cancellation, and cache paths. It is covered by focused bridge and adapter tests. |
| Live Compose acceptance | Real PostgreSQL and Temporal Server lifecycle validation. It does not yet run the two-OCaml-binary workflow-result scaffold. |

The first two rows are different test boundaries, not different workflow
languages. The same typed definitions and direct-style functions are used in
both; `mock://` keeps tests local, while an HTTP(S) target uses the native
OCaml/Rust bridge. The live Compose target currently proves server and Core
lifecycle only, so it is not yet evidence that a workflow result has crossed a
real Temporal Server.

## The direct-style model

There are three values to keep distinct:

- A `result` is an ordinary OCaml value. `Ok value` means the operation
  succeeded and `Error error` means it produced an expected SDK or Temporal
  failure.
- A `Temporal.Future.t` is a workflow-owned value that may become a result in
  a later response from Temporal. Creating one schedules work; it does not
  wait.
- `Temporal.Future.await future` turns that future into a result. If the future
  is already ready, it returns immediately. Otherwise the private scheduler
  suspends the current workflow fiber and resumes it when Temporal supplies the
  matching completion.

This lets workflow code read from top to bottom while retaining Temporal's
durable execution model:

The `summarize` value below is the typed activity reference defined later in
this guide.

```ocaml
let summarize_document document =
  let open Temporal.Result_syntax in
  let* summary = Temporal.Activity.execute summarize document in
  let* () = Temporal.Workflow.sleep (Temporal.Duration.of_ms 10L) in
  Ok summary
```

`Activity.execute` is the convenient form of “start this activity, then await
it”. If the activity is still running, the function above is suspended at that
line and the worker can run other workflow fibers. It is not equivalent to
holding a mutex or sleeping a native thread. The same rule applies to
`Workflow.sleep` and `Child_workflow.execute`.

`Temporal.Result_syntax` provides only the standard `result` operators. Its
`let*` makes it easy to stop on the first expected error; it does not add a
second monad or expose the effect scheduler.

Child-workflow code is valid in the synthetic runtime and the semantic command
translator. It is **not** a supported native end-to-end feature yet: the native
worker rejects a parent completion containing a child start until Core child
resolution jobs are represented and replay-tested.

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

The function above returns an `Error.t` value instead of raising an exception.
That is the normal way to report an expected workflow failure. Pattern-match
when the caller needs to choose a recovery path, or use `let*` when the error
should finish the workflow:

```ocaml
let greet_or_explain input =
  match greeting input with
  | Ok value -> Ok value
  | Error error ->
      Ok ("The greeting could not be created: " ^ Temporal.Error.message error)
```

Exceptions are reserved for programmer defects and broken internal invariants.
The worker boundary catches an unexpected exception and reports a structured
failure; workflow code should not use exceptions as its ordinary branch or
retry mechanism.

Workflow code must be deterministic during replay. Temporal may run the same
function again from its recorded history, so a different decision on the
second run would produce a different workflow. In workflow code, prefer pure
OCaml values and SDK operations:

| Safe in a workflow | Put it in an activity (or wait for a replay-safe SDK API) |
| --- | --- |
| String/list calculations and immutable data | Network calls, filesystem access, or subprocesses |
| `Activity.start`, `Child_workflow.start`, timers, and future combinators | `Unix.gettimeofday`, `Random`, or another unrecorded clock/random source |
| IDs derived from workflow input and explicit constants | Reading mutable process-global state or environment to choose a command |
| Iteration over an ordered list | Iteration over a hash table or other unordered collection when order affects commands |

Activities are the place for external work such as calling an LLM. The
activity result is recorded by Temporal, so replay can use that recorded result
without calling the external service again.

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

## 3. Choose between an activity and a child workflow

Starting either operation returns a typed future, but the two operations
represent different Temporal resources. The `execute` forms are convenience
functions that start and immediately await that future:

| Use an activity when… | Use a child workflow when… |
| --- | --- |
| One task should perform external or nondeterministic work, such as an LLM call. | The work is itself a durable workflow with its own workflow type and history. |
| An activity worker, possibly written in another language, should execute the task. | You want to start another workflow execution explicitly and identify it with a durable child ID. |
| The parent needs one typed result from that task. | The parent wants a separate workflow boundary and may await that child result. |

Calling an ordinary helper function creates neither resource. A child exists
only when code explicitly calls `Child_workflow.start` or
`Child_workflow.execute`; an activity exists only when code explicitly calls
`Activity.start` or `Activity.execute`.

Define a local implementation when this worker should execute the operation:

```ocaml
let summarize_activity =
  Temporal.Activity.define
    ~name:"summarize"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string
    (fun document -> Ok (String.trim document))
```

Use `Activity.remote` or `Workflow.remote` when another worker owns the
implementation. A remote definition keeps the name and codecs needed to
encode and decode the call, but it cannot be registered as a local worker
implementation.

## 4. Schedule activities and wait directly

Use a local activity definition when this worker will execute the task, or a
remote activity reference when another worker owns it:

```ocaml
let summarize =
  Temporal.Activity.remote
    ~name:"summarize"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

let summarize_document document =
  let open Temporal.Result_syntax in
  let summary_future = Temporal.Activity.start summarize document in
  let timer_future =
    Temporal.Workflow.start_sleep (Temporal.Duration.of_ms 10L)
  in
  let* summary, () =
    Temporal.Future.await (Temporal.Future.both summary_future timer_future)
  in
  Ok summary

let summarize_workflow =
  Temporal.Workflow.define
    ~name:"summarize_document"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string
    summarize_document
```

For a valid call, `Activity.start` emits a command and returns a future
immediately. Starting the timer before waiting demonstrates the important
pattern: schedule independent work first, then await a combined future.
Temporal can process the activity and timer independently, while the workflow
still receives their results in a deterministic way. `Activity.execute` is the
short form for `start` followed by `Future.await` when no other work needs to
be started first.

Activity scheduling accepts labelled options for a stable activity ID, task
queue, timeout values, cancellation policy, and eager-execution preference.
Invalid identifiers, payloads, or options produce a typed future error before
a history command is emitted. `Temporal.Workflow.start_sleep` creates a durable
timer without waiting; `Temporal.Workflow.sleep` is the start-and-wait form.

## 5. Combine futures

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
they do not cancel siblings implicitly. If several inputs fail, `all` returns
the first error in the input list after all siblings have settled. `race` can
combine different output types and returns `Left value` or `Right value`;
`first` is the homogeneous non-empty-list form. An error is a completion, so it
may win a race. The combinator itself does not cancel losing operations; they
continue according to their normal Temporal lifecycle.

All inputs to a combinator must belong to the same workflow execution. A
future from another execution is not silently adopted: the result is a ready
structured defect. This ownership rule prevents one workflow's scheduler from
resuming another workflow's continuation.

When a future is not ready, `Future.await` suspends only the current workflow
fiber. Other runnable workflow fibers and the worker process can continue. The
effect machinery is private, so workflow authors write direct-style OCaml.
`Future.peek` and `Future.is_ready` are available when code needs to inspect a
future without waiting, but they do not make an incomplete operation complete.

## 6. Child workflows: authoring versus native support

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
child scheduling and deterministic future resolution. The current native
worker does not yet complete this path against Temporal Server. It can encode
the child-start command, but the activation protocol does not yet carry the
child-resolution job needed to resume the parent. To avoid acknowledging a
parent task that cannot be resumed, the native adapter returns an explicit
typed rejection. Treat child workflows as experimental/synthetic-only until
the live acceptance test and matching resolution tests are added.

## 7. Compose ordinary helpers

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

Helpers can also add ordinary application behavior around one operation. They
do not need a registration entry or a special return type:

```ocaml
let summarize_with_label document =
  let open Temporal.Result_syntax in
  let* summary = Temporal.Activity.execute summarize document in
  Ok ("summary: " ^ summary)

let run_two_summaries document =
  let first = Temporal.Activity.start summarize document in
  let second = Temporal.Activity.start summarize (document ^ ":backup") in
  Temporal.Future.all [ first; second ]
  |> Temporal.Future.await
```

`run_two_summaries` is still just an OCaml function. The two activity commands
are emitted when it is called, and the caller decides whether to await both,
race them, or map their result. This is the intended way to build a small
library of reusable orchestration helpers.

## 8. Register a worker

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

This restriction applies to the worker lifecycle call, not to
`Future.await` inside a workflow. The worker's native readiness wait releases
the OCaml runtime lock, but `Worker.run` still owns a blocking loop. Keep it on
the dedicated worker Domain and let workflow fibers use the private scheduler
for their durable waits.

The native path keeps Rust/Core and its protobufs private. The OCaml worker
receives checked descriptions of work, runs the typed function, and sends a
checked result back through the private supervisor.

## 9. Start and wait from a client

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

## 10. Validate locally

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
