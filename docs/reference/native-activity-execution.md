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
```

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
4. Invoke the implementation and convert its typed `result` into either an
   encoded payload or a structured application failure. Application failure
   retryability and every binary-safe detail payload supplied in `Error.t` are
   copied into the Temporal failure rather than reduced to a message only.
5. Validate the completion through the strict activity-protocol encoder.
6. Submit the completion to the supervisor and remove the token only after the
   supervisor returns `Ok ()`.

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

## Current boundary and deliberate limits

This slice implements typed local activity dispatch, failure/cancellation
completions, strict completion validation, transport retry, and public worker
wiring. It does not yet expose heartbeats, asynchronous activity completion, or
activity retry policy decisions. Those features require additional semantic
protocol fields and remain behind the live Docker Compose acceptance gate.

The semantic wire shape already carries the full decoded Temporal activity
context (headers, heartbeat details, timeouts, retry policy, priority, and
standalone run ID).  The adapter currently consumes only the fields required
for typed dispatch; retaining the complete protocol value keeps future
features additive without inventing a second payload format.
