# Private native activity execution adapter

`Temporal_runtime.Native_activity_execution` is the private OCaml layer that
turns a decoded Temporal activity task into a typed OCaml function call. It is
connected to the native `Temporal.Worker` path through the one owner-Domain
supervisor. The supervisor provides these typed operations after Rust/Core has
polled and validated the native JSON envelope:

```ocaml
try_poll_activity :
  supervisor -> (Activity_protocol.task option, native_error) result

complete_activity :
  supervisor -> Activity_protocol.completion -> (unit, native_error) result

record_activity_heartbeat :
  supervisor -> Activity_protocol.heartbeat -> (unit, native_error) result
```

For a `Start` task, the native bridge has already leased the opaque task token
and decoded the JSON envelope into the semantic `Activity_protocol.task` value.
This adapter then performs the language-side work: it looks up the activity
type, decodes its one input value, calls the registered OCaml function, and
submits exactly one terminal completion for that token. A `Cancel` task skips
the user function and submits a cancelled completion. Polling and dispatch are
synchronous in this layer; Rust worker threads do not call OCaml callbacks.

The adapter is deliberately independent of the concrete Rust supervisor.  A
deterministic fake supervisor can therefore test every lease and completion
path without a Temporal Server, while the production supervisor remains the
only owner of Rust handles and network state.

## Registration and dispatch

An executable activity is registered with its normal typed definition:

```ocaml
let summarize =
  Temporal.Activity.define
    ~name:"summarize"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string
    (fun text -> Ok (String.uppercase_ascii text))
```

The private adapter stores heterogeneous definitions behind an existential
wrapper, indexed by Temporal activity type name.  Each definition keeps its
input and output codecs next to its implementation, so a function cannot be
called with one codec and completed with another.  Duplicate names and
`Temporal.Activity.remote` definitions are rejected before polling begins.

For a `Start` task the adapter performs this sequence while holding its
adapter mutex:

1. Copy the opaque task token.
2. Find the activity type in the immutable registry.
3. Decode zero arguments as the canonical unit payload, one argument with the
   registered input codec, or reject more than one argument.
4. Build an attempt context from the server's heartbeat details and timeout,
   then invoke the implementation and convert its typed `result` into either an
   encoded payload or a structured application failure. Application failure
   retryability and each detail body supplied in `Error.t` are copied into the
   Temporal failure without text conversion; metadata still follows the
   runtime's strict UTF-8 key/value rules rather than becoming an unvalidated
   side channel.
5. Validate the completion through the strict activity-protocol encoder.
6. Submit the completion to the supervisor and remove the token only after the
   supervisor returns `Ok ()`.

The adapter mutex covers this whole transaction, including the user
implementation and the native completion call. The production worker's run
loop therefore executes one OCaml activity attempt at a time and cannot poll a
second activity until the first attempt has produced an acknowledged terminal
completion. This deliberate serialization keeps the token ledger and the
supervisor mailbox race-free; it is separate from Rust/Core's own network
concurrency.

The context-aware form is authored with `Temporal.Activity.define_with_context`:

```ocaml
let summarize =
  Temporal.Activity.define_with_context
    ~name:"summarize"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string
    (fun context text ->
      match Temporal.Activity.Context.heartbeat context Temporal.Codec.string text with
      | Error error -> Error error
      | Ok () -> Ok (String.uppercase_ascii text))
```

`Temporal.Activity.Context.details` returns the ordered payloads from the
previous attempt's heartbeat, and `heartbeat_timeout` returns the server's
configured interval when one was supplied. `Context.heartbeat` and
`Context.heartbeat_payloads` copy their payloads, validate them through the
same strict activity JSON codec as completions, and send them through the
supervisor mailbox. The mailbox serializes heartbeats with polling,
completion, and shutdown; an arbitrary Rust thread never calls an OCaml
callback.

Before constructing the context, the adapter validates the server timeout: it
rejects negative, sub-millisecond, or out-of-range values instead of rounding
or overflowing them. An accepted timeout is therefore exposed as an exact
whole-millisecond `Duration.t`.

The context is valid only while its activity attempt is executing. The adapter
invalidates it before returning from dispatch, including exceptional and
completion-error paths. A retained context therefore returns a typed error
instead of retaining a native pointer or accidentally heartbeating a later
task. A successful heartbeat does not acknowledge or retire the activity lease;
the terminal completion retry map remains the sole owner of completion debt.

`heartbeat_timeout` is copied server metadata, not a local deadline. The
adapter exposes it but does not start a timer or synthesize timeout/retry
behavior; Temporal Core owns timeout decisions and subsequent task delivery.
If Core has already timed out an attempt, this synchronous adapter has no
stale-completion recovery or asynchronous-lease operation, so timeout-triggered
retry remains explicitly unimplemented.

An implementation exception is caught at this boundary and becomes a typed
non-retryable failure.  Exceptions are therefore a last-resort defect guard,
not the normal way an activity reports an expected failure.

## Cancellation

A `Cancel` task has no activity type or input.  The adapter returns a
`Cancelled` completion with a standard Temporal `Canceled` failure and copies
the exact token unchanged.  The closed cancellation-reason variant is rendered
with stable labels (`not_found`, `cancelled`, `timed_out`, `worker_shutdown`,
`paused`, or `reset`).  Cancellation details remain task metadata owned by the
native protocol; they are not re-encoded into an unrelated application
payload.

## Completion retry and ownership

The adapter keeps a small token-keyed map of pending completions.  The map is
needed because a native completion call can fail after the activity function
has already run.  Before polling a new task, `poll` retries one pending
completion.  It never invokes the activity implementation again for that
token.  A typed supervisor error or an exception leaves the completion in the
map and returns an error to the caller because lease retirement is not proven.

The token is copied on receipt, copied again into the completion, and never
converted to a string.  This preserves arbitrary binary tokens, prevents
caller-owned mutable buffers from being retained, and keeps tokens out of
diagnostics and logs.  The map is protected by one mutex around poll,
dispatch, and completion so two OCaml Domains cannot execute or retire the
same lease concurrently.  The mutex is an OCaml state guard; it does not hold
the OCaml runtime lock while Rust waits.  The concrete supervisor is
responsible for releasing that runtime lock in its C boundary.

The private worker shutdown path calls the adapter's `drain` operation before
closing native Core. It retries every retained completion while holding the
same mutex and starts teardown only after the token map is empty. A failed
drain leaves the exact completion and the native graph usable, so callers can
retry rather than converting a transient transport error into an
`outstanding_tasks` shutdown failure.

`test/runtime/test_native_activity_lifecycle.ml` keeps this shutdown contract
in a separate focused test. It forces one completion rejection during polling
and another during the first drain, then verifies that the second drain retires
the original binary-token lease. The activity implementation is called once
and the completion is submitted once; retrying never repeats user work.

## Current boundary and deliberate limits

This slice implements typed local activity dispatch, failure/cancellation
completions, strict completion and heartbeat validation, transport retry, and
public worker wiring. The native heartbeat path is covered by focused tests in
[`test/runtime/test_native_activity_execution.ml`](../../test/runtime/test_native_activity_execution.ml),
[`test/runtime/test_native_activity_lifecycle.ml`](../../test/runtime/test_native_activity_lifecycle.ml),
and [`rust/core-bridge/tests/activity_protocol.rs`](../../rust/core-bridge/tests/activity_protocol.rs),
including binary detail preservation, prior-attempt detail delivery, lease
retention, copied context payloads, callback-exception classification, and
context invalidation. A live Temporal heartbeat scenario, asynchronous
activity completion, and timeout/retry behavior still require dedicated
acceptance scenarios.

Although the semantic protocol reserves `Will_complete_async`, this adapter
does not emit that completion variant and does not retain a public handle for a
later completion. Activities must return a terminal `result` synchronously;
asynchronous completion is an explicit future capability rather than an
implicit behavior of the current worker.

The semantic wire shape already carries the full decoded Temporal activity
context (headers, heartbeat details, timeouts, retry policy, priority, and
standalone run ID).  The adapter currently consumes only the fields required
for typed dispatch; retaining the complete protocol value keeps future
features additive without inventing a second payload format.
