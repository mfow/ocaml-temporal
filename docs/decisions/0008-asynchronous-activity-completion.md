# ADR 0008: Asynchronous activity completion boundary

- Status: accepted design; implementation pending
- Date: 2026-07-13
- Decision owners: OCaml Temporal maintainers

## Context

The current activity worker invokes one OCaml callback at a time, submits one
terminal completion through the Temporal Core worker operation, and invalidates
the attempt context before dispatch returns. If the native completion call is
temporarily unavailable, the adapter retains an owned copy in its retry ledger
and retries it without running the callback again. `Temporal.Activity.define`
and `Temporal.Activity.define_with_context` therefore support synchronous
activities and heartbeats, but they do not let an activity retain a safe
completion capability for work that finishes later or after the worker lease
has expired.

The private activity protocol already has a closed
`will_complete_async` result. It is a protocol/Core-conversion variant only:
the OCaml adapter does not produce it from a public activity definition, and
the Rust bridge currently exposes only the Core worker completion path. Every
completion produced by the current adapter therefore remains a worker-lease
completion; no public `AsyncActivityHandle` is retained for a later client
operation.

Temporal Core deliberately treats these two operations differently. Accepting
`WillCompleteAsync` through the worker tells Core that the worker has handed
the activity to external code and removes the worker's outstanding-task
record. A later successful, failed, cancelled, or heartbeat response must use
the namespace-bound Temporal client service, not the worker operation. The
pinned Core client exposes this as `AsyncActivityHandle`.

The implementation must also respect the current ownership boundary. The
activity adapter holds its mutex over dispatch, including the user callback,
and the one SDK supervisor Domain owns all native graph operations. A handle
which recursively acquires the adapter mutex from inside the callback could
deadlock or re-enter activity dispatch. A retained activity context is not a
valid workaround: it is intentionally invalidated when the callback returns.

## Decision

Keep the existing synchronous API unchanged. Until this decision is
implemented, `Temporal.Activity.define` and
`Temporal.Activity.define_with_context` remain the only executable activity
definitions, and their callbacks must return terminal results before dispatch
returns. Then add an explicit asynchronous activity definition with a typed
outcome. The exact names may evolve during implementation, but the public
shape must have these properties:

```ocaml
type ('output) async_result =
  | Completed of 'output
  | Failed of Error.t
  | Will_complete_async of ('output) Async_handle.t

type ('input, 'output) async_implementation =
  Async_context.t -> 'input -> ('output) async_result

val define_async :
  name:string ->
  input:'input Codec.t ->
  output:'output Codec.t ->
  ('input, 'output) async_implementation ->
  ('input, 'output) t
```

`Async_handle.t` is opaque. Its operations accept and encode the output type
paired with the activity definition and return `(unit, Error.t) result` (or a
typed heartbeat response), never an exception for an expected Temporal or
transport failure. A context helper may construct the handle, but the callback
must return the explicit `Will_complete_async` case. There must be no dummy
successful output, mutable context flag, or implicit defer caused by dropping
an output value. The existing `define` and `define_with_context` wrappers
remain ordinary result-returning functions.

The defer handoff is linearized only after the activity callback has returned.
At that point the adapter submits exactly one `WillCompleteAsync` completion
to Core. If Core accepts it, the adapter publishes the opaque handle and moves
the token into a separate asynchronous-lease registry. If Core rejects it,
the original worker lease remains in the ordinary retry map and the activity
is not silently abandoned. A handle is never usable before this handoff is
accepted, so a callback cannot synchronously call back through the mutex it is
already holding.

## Native boundary

Add a distinct supervisor operation and C ABI entry point for terminal
completion of an accepted asynchronous lease. Do not overload the existing
`Complete_activity` operation:

| Operation | Temporal owner | Rust action | OCaml state transition |
| --- | --- | --- | --- |
| `WillCompleteAsync` handoff | Core worker | `Worker::complete_activity_task` with the async status; no server RPC | ordinary worker lease -> async lease, only after acceptance |
| later `Completed`/`Failed`/`Cancelled` | namespace-bound client | `Client::get_async_activity_handle(...).complete/fail/report_cancelation` | async lease remains pending until the client result proves acceptance |
| later heartbeat | namespace-bound client | `AsyncActivityHandle::heartbeat` | async lease remains pending; cancellation flags are returned |

The new operation consumes the same strict activity-completion JSON shape but
rejects `will_complete_async`; that variant is valid only for the worker
handoff. Rust must validate the token and payloads again, construct a temporary
Core client handle, and keep all Tokio work on the existing runtime. It must
not consult or retire `Runtime.activity_tasks` for the later client RPC: Core
already removed that worker ledger entry when the handoff succeeded.

The runtime retains the validated worker namespace needed to construct
`ClientOptions` for the async handle. Callers do not supply a second namespace
that could disagree with the worker configuration. The initial implementation
uses the copied task token as the identifier; workflow/run/activity-ID based
handles can be added later only with the same strict identity validation.

## Ownership and concurrency

- The OCaml adapter owns an asynchronous lease record containing an immutable
  copy of the binary token, the activity's output codec, and its lifecycle
  state. Public handles expose neither the token nor a Rust pointer.
- The supervisor owns the native runtime, connection, and client construction.
  Every bridge call is serialized through its owner Domain. Rust/Tokio owns
  network concurrency and never calls an OCaml closure.
- Handle methods may be called from any application Domain after the handoff.
  They enqueue typed supervisor operations; they must not acquire the adapter
  dispatch mutex while an activity callback is running. The implementation
  must document and test its lock order (`handle state` -> `supervisor`, never
  `adapter dispatch mutex` -> callback -> `handle state`).
- The asynchronous handle is independent of `Activity_context`. Context
  invalidation after dispatch therefore cannot invalidate a deliberately
  retained handle, and retaining a context cannot extend a native token's
  lifetime.
- A handle has one terminal transition. Concurrent terminal calls are
  serialized by its lease state: one request is admitted, an already admitted
  equivalent request may be observed as the same result, and a conflicting or
  repeated terminal outcome returns a typed state error. No result is silently
  discarded when a response is uncertain.

## Retry and shutdown

The adapter keeps terminal client requests in the asynchronous-lease registry
until the supervisor reports acceptance. A typed transport failure or an
exception leaves the exact token and encoded request available for retry; the
activity implementation is never run again. The implementation must map
Temporal's already-completed/not-found responses deliberately rather than
assuming that a lost response means the request was not applied. This is the
only safe way to preserve idempotent behavior across a network boundary.

Worker shutdown first stops new polling, then drains ordinary worker leases as
it does today. It must also account for asynchronous leases: either drain
their admitted client requests within the configured shutdown period or return
a typed outstanding-async-leases error while retaining enough state for an
explicit retry. It must not force-complete, drop, or invalidate an accepted
async token merely to make the native worker graph appear closed. Finalizer
cleanup follows the same ownership rule and may use the existing dedicated
cleanup thread, but it cannot run user callbacks or issue a hidden completion.

## Verification plan

Implementation is staged so each ownership boundary is independently
reviewable:

1. **OCaml adapter tests.** Use a fake supervisor to cover the explicit async
   outcome, callback-return handoff, token copying, context invalidation,
   handle-before-handoff rejection, duplicate/conflicting terminal calls,
   concurrent callers, transport uncertainty, retry without rerunning user
   code, and shutdown with outstanding async leases.
2. **Rust client tests.** Use a mock `WorkflowService` to verify namespace and
   identity construction, task-token completion/failure/cancellation and
   heartbeat conversion, strict malformed JSON rejection, and the fact that
   client completion never retires the Core worker ledger. Test the pinned
   `AsyncActivityHandle` behavior for accepted and already-resolved responses.
3. **Bilateral protocol tests.** Add OCaml and Rust fixtures for terminal-only
   client requests, rejection of `will_complete_async` on that endpoint,
   duplicate/unknown fields, malformed tokens, payload limits, and canonical
   re-encoding.
4. **Live Compose acceptance.** Extend the two-OCaml-binary stack with an
   activity that returns the async outcome, a separate completion action, and a
   workflow assertion that waits for the later result. Include cancellation,
   heartbeat details, retry/timeout, duplicate completion, and graceful
   shutdown scenarios. These are live feature tests; focused fake-supervisor
   tests must remain separate.

## Current synchronous boundary and heartbeat limits

The asynchronous handoff described here is not implemented yet. The current
activity protocol keeps `will_complete_async` so the semantic wire shape is
closed, but the public adapter emits only synchronous terminal completions
through the worker path. There is no public async handle, retained activity
context, or client-side completion operation in the current OCaml/C/Rust ABI.
Likewise, `heartbeat_timeout` is copied context metadata; the adapter does not
run a local timeout timer or attempt to recover a completion after Core has
timed out the lease. Existing context-lifecycle, payload-copying, protocol,
and ABI tests prove the synchronous ownership boundary. The next milestone is
the separate namespace-bound async lease and client terminal-operation path,
followed by live timeout and retry acceptance.

## Consequences

- Synchronous activities retain their simple, idiomatic API and existing
  behavior.
- Asynchronous activities make the suspension and ownership transfer explicit
  in the OCaml type instead of hiding it in exceptions or mutable context.
- The bridge gains one additional, clearly separated client-RPC operation and
  a second lease state, but the Core worker ledger and the client's server
  protocol cannot be accidentally conflated.
- A handle may outlive the callback, but it cannot outlive the SDK instance
  without returning a typed closed/outstanding error; callers must retain it
  until completion is acknowledged.
- This is a correctness-first design. Throughput optimizations such as
  parallel JSON encoding or multiple activity dispatch Domains are deferred
  until the serialized handoff, retry, and shutdown tests pass.

## Evidence

The current synchronous boundary and lease rules are documented in
[`native-activity-execution.md`](../reference/native-activity-execution.md).
The existing closed protocol representation is specified in
[`activity-protocol.md`](../reference/activity-protocol.md) and implemented in
`lib/protocol/activity_protocol.ml` and
`rust/core-bridge/src/activity_protocol.rs`.

The adapter implementation is in
`lib/runtime/native_activity_execution.ml`; public activity registration is in
`lib/public/activity.ml`. Neither module exposes asynchronous completion yet.

The worker/client split and one-owner lifecycle are recorded in
[`ADR 0004`](0004-sdk-instance-supervisor.md). The pinned Temporal Core client
implementation is
[`async_activity_handle.rs`](https://github.com/temporalio/sdk-core/blob/95e97686a079dcfe6c42e3254b2f3f5e3d97408f/crates/client/src/async_activity_handle.rs),
which provides the namespace-bound `AsyncActivityHandle` operations used by
this decision. It is a design reference for the future bridge; no current
OCaml/C/Rust operation wraps it.
