# ADR 0003: Private bounded FIFO mailbox processor

## Status

Accepted and implemented on 2026-07-11.

## Context

The SDK's future supervisor must serialize every stateful OCaml/Rust handle
operation on one owner Domain. Producers may be ordinary code running on many
OCaml Domains. This needs a reusable typed mailbox boundary before live worker
handles exist, but the boundary must not publish backend, Temporal, or Rust
types to installed `temporal-sdk` consumers.

The synchronization substrate must have an explicit cross-Domain contract and
must work in the native Windows CI job. OCaml 5 documents `Mutex`, `Condition`,
and `Semaphore` as blocking synchronization available to Domains. Its memory
model makes mutex unlock and subsequent mutex operations part of happens-before
ordering, and data-race-free code receives sequentially consistent behavior.
The threads implementation uses POSIX threads on Unix-like systems and Win32
threads on Windows. These are primary runtime facilities, so they add no OPAM
dependency or license closure.

Eio 1.3 now documents `Eio.Stream` as thread-safe between Domains and documents
that promises may be resolved from another Domain. Using it internally would
still require an Eio scheduler on the owner Domain, enlarge the current locked
dependency graph, and couple this low-level ownership primitive to one fiber
runtime. The standard blocking design is smaller and remains usable by a later
Eio adapter.

Primary references:

- [OCaml 5.2 parallel programming](https://ocaml.org/manual/5.2/parallelism.html)
- [OCaml 5.2 condition variables](https://ocaml.org/manual/5.2/api/Stdlib.Condition.html)
- [OCaml 5.2 memory model](https://ocaml.org/manual/5.2/memorymodel.html)
- [OCaml 5.2 threads and Windows implementation](https://ocaml.org/manual/5.2/libthreads.html)
- [Eio 1.3 synchronization and Domain integration](https://ocaml.org/p/eio/latest/doc/README.html)

## Decision

Add `lib/mailbox_processor/` as a Dune-private library with no `public_name`.
It is a functor over a GADT request family:

```ocaml
module type Request = sig
  type _ t
end
```

The result type selected by each request is preserved through an existential
queue job and a typed one-shot reply cell. The implementation does not use
`Obj`, serialized values, or a heterogeneous result table. A rank-2 handler
runs only on the one Domain created for the processor. Mutable state captured
by that handler is therefore owner-confined when callers respect the contract.

`post` accepts only `unit Request.t` and returns when the request is admitted.
`call` accepts any `'result Request.t` and waits for that exact result. Expected
operational failure belongs in the request result type, for example
`('value, 'operation_error) result Request.t`; mailbox failures describe only
closure or an unexpected handler exception.

### FIFO and capacity

One mutex protects lifecycle state and the bounded `Queue.t`. A successful
enqueue mutation is the admission linearization point. The owner removes jobs
from the head, so it invokes handlers in that total enqueue order. Requests
made sequentially by one producer preserve program order. Requests from
concurrent producers have no specified order until their successful enqueue
mutations; mutex acquisition fairness is intentionally not promised.

Capacity is the maximum admitted work waiting in the FIFO. The request already
being handled is not in the queue and does not count against this bound. A
producer that finds an open full queue waits on a condition and rechecks both
capacity and lifecycle after every wake. Dequeue wakes capacity waiters. This
is real bounded-buffer backpressure, not an unbounded queue with advisory
limits.

### State transitions and closure

The protected state machine is:

```text
Open --close--> Closing --queue drained--> Stopped
Open or Closing --handler exception--> Failed
```

Normal close linearizes while holding the queue mutex. It rejects new work,
wakes blocked producers, and drains everything admitted before the transition.
Close and join are idempotent. Join serializes the single `Domain.join`, caches
the terminal result, and provides the Domain happens-before edge for owner
writes.

An unexpected handler exception first settles the active call, then atomically
changes the processor to `Failed`, removes queued jobs, and wakes blocked
producers. Queued posts are discarded because they have no waiter. Every
queued call is settled with the same contained exception after the queue mutex
is released. New admissions receive that same terminal failure. Thus no reply
cell can remain stranded after failure and no queued operation can execute
after the failure transition.

### Locking and scheduler boundary

All queue and lifecycle reads and writes occur under the processor mutex. Each
reply cell has a separate mutex and condition; it is resolved exactly once.
Condition waits always use predicate loops because OCaml permits spurious
wakes. Failure detaches queued jobs while holding the queue mutex, then settles
their replies without that mutex, avoiding nested queue/reply lock cycles.

`post`, `call`, and `join` may block an OS thread. They must not be called on a
future Eio or workflow-effect scheduler Domain. A future fiber-friendly adapter
will keep this `.mli` unchanged and offload blocking calls with a documented
bridge such as `Eio_unix.run_in_systhread`, then resolve a fiber promise from
that helper. The dedicated owner Domain remains the only handler executor.

The handler must not re-enter `post`, `call`, or `join` on its own processor.
Because there is exactly one owner Domain, `call` and `join` cannot complete
until the current handler returns. `post` can also wait forever if the bounded
FIFO is full. Calling `close` from the handler is safe: it changes lifecycle
state without waiting for the owner, and the owner drains previously admitted
requests after the current handler returns.

## Consequences

- The processor is portable without new dependencies and remains extractable
  into its own package later.
- Typed operations and expected `result` failures cross the queue without
  exposing implementation types.
- Throughput is deliberately secondary to simple ownership and reviewable
  failure paths.
- Selective receive, supervision trees, distribution, actor-per-handle designs,
  and scheduler adapters are outside this library.
