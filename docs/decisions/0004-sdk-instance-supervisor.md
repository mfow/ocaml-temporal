# ADR 0004: One owner-Domain supervisor per SDK instance

## Status

Accepted and implemented on 2026-07-11. The native graph contains the real
Rust runtime, one official client connection, and one workflow-only worker.
Polling and completion will extend the same typed graph rather than create
additional supervisors.

## Context

Temporal Core state will eventually include one Tokio runtime, one connected
client, and one or more workers. Those values are Rust-owned opaque handles.
Allowing arbitrary OCaml Domains to use or release them directly would make
shutdown races, use-after-close defects, and inconsistent child-before-parent
destruction possible. Creating one actor per handle would move those ordering
problems between actors instead of removing them.

The private mailbox processor from ADR 0003 already provides a bounded typed
FIFO, a sole handler Domain, and deterministic close/failure propagation. The
remaining need is a resource-graph lifecycle layer which never leaks backend
state through a callback or result.

The bridge first established runtime ownership; it now also exposes real client
connection and validated workflow-worker lifecycle. Polling and completion are
still deliberately absent until their strict JSON protocol is implemented.

## Decision

Add a Dune-private `temporal_sdk_supervisor` library. `Sdk_supervisor.Make`
accepts a typed backend protocol:

- `create` constructs owner-confined graph state;
- a GADT operation selects each successful result type;
- `perform` uses that graph without returning it;
- `shutdown` consumes or invalidates the entire graph, even when it returns an
  expected error.

The supervisor starts its mailbox before backend creation and sends an
initialization request through it. Creation, every operation, and shutdown
therefore run on the same dedicated owner Domain. Producers receive only typed
operation results and structured supervisor errors. The backend `state` type
does not occur in any producer-facing function, so a raw runtime/client/worker
handle cannot be returned accidentally by a generic `with_handle` callback.

The production specialization owns one abstract
`Temporal_core_bridge.Native_bridge.runtime`, inside which Rust retains the
client and worker children. Its GADT operations check compatibility, connect a
client, start a validated workflow-only worker, stop that worker, and disconnect
the client. Polling and completion will be added to this same typed language.

## Lifecycle and failure rules

Owner-only graph state follows this state machine:

```text
Not_started --successful create--> Running --shutdown--> Closed_graph
Not_started --create error-------> no published supervisor
Running --unexpected defect------> cleanup once --> terminal mailbox failure
```

Expected `perform` errors do not close the graph. An unexpected backend
exception first records terminal state, attempts backend shutdown once, and is
then re-raised into the mailbox's containment boundary. The active caller,
queued callers, later callers, and shutdown all observe the same contained
exception. A cleanup error cannot replace the original defect.

Explicit shutdown first submits and caches the mailbox's atomic terminal call.
Under the same mailbox mutex, it takes the FIFO position after operations
already admitted and changes the mailbox to closing, so later operations
cannot overtake it even when the ordinary bounded queue is full. Submission
does not wait for earlier backend work; the private `initiate_shutdown` seam
therefore identifies the exact point after which all new operations must be
rejected. The public `shutdown` operation awaits that cached reply and joins
the mailbox. A separate mutex permits only one producer to submit, await, and
perform the blocking join. It caches the exact result, including an expected
backend shutdown error or an unexpected owner failure. Concurrent and repeated
shutdown calls never submit a second terminal request or invoke native
destruction again.

If an application abandons a live supervisor, an OCaml finalizer starts a
dedicated system thread which calls the same serialized shutdown function. The
finalizer itself never waits for mailbox capacity, an owner response, a Domain
join, or native destruction. Explicit shutdown marks the instance atomically,
so collecting an already closed instance does not start a redundant thread.
If thread creation is unavailable during process teardown, the fallback closes
the mailbox without blocking; native runtime custom-block finalization then
uses Rust's asynchronous cleanup thread.

The backend contract requires `shutdown` to consume or invalidate its graph
even when it returns `Error`. This matches the native runtime close operation,
which atomically detaches the Rust pointer before reporting status. A backend
which cannot honor that rule must represent a retryable close as a separate
non-terminal operation instead of returning from `shutdown` with live state.

## Scheduler boundary

`create`, `perform`, and `shutdown` may block the calling OS thread while
waiting for mailbox admission or a one-shot reply. They are for ordinary
producer Domains. A future Eio adapter must use a documented system-thread
offload and resolve a fiber-safe promise afterwards. Workflow effect handlers
must never call these functions directly while running their deterministic
scheduler.

The owner Domain may later block in a Rust polling C stub only when that stub
has released the OCaml runtime lock. Rust/Tokio threads signal native readiness
and never call arbitrary OCaml closures. The supervisor itself introduces no
timer polling or foreign callback.

## Verification

Focused tests prove:

- creation, typed operations, and shutdown all run on one non-caller Domain;
- concurrent producers never overlap backend calls and receive each result
  exactly once;
- expected operation errors leave the graph usable;
- creation errors publish no supervisor and close no nonexistent state;
- unexpected operation exceptions close once and release queued callers with
  the same failure;
- shutdown initiation rejects later use synchronously while admitted backend
  work remains blocked, then public shutdown waits behind that work and caches
  both success and expected shutdown errors;
- sixteen concurrent shutdown callers share one result and one backend close;
- unexpected create and shutdown exceptions are contained without stranding
  callers, and a shutdown exception is cached for concurrent/later callers;
- abandoning a live supervisor schedules non-blocking-finalizer cleanup and
  closes its backend exactly once;
- the production specialization creates, uses, rejects an invalid lifecycle
  transition, and repeatedly shuts down the real Rust graph safely; and
- repository/install checks keep the library out of the installed package.

## Consequences

- Native graph ownership has one reviewable serialization point.
- The design favors correctness over maximum cross-language call throughput;
  Rust/Tokio still owns network concurrency behind each serialized request.
- Future handle types remain private and cannot escape through the supervisor
  API.
- Cooperative runtime adapters are still required and must not weaken the
  blocking boundary.
- This milestone connects and validates a worker but does not yet poll or
  complete workflow tasks; that live execution loop remains Phase 2 work.
