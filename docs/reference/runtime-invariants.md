# Runtime Invariants

These invariants define the correctness boundary of the OCaml workflow
runtime. Changes that weaken one require an architecture decision and replay
tests.

For introductory definitions of activation, command, replay, payload, future,
and bridge, read the [documentation guide](../README.md) first.

## Execution ownership

- One execution owns one scheduler, command sequence, pending-operation set,
  and continuation set.
- A workflow operation can access its context only while that execution's
  activation is running on the current domain.
- Futures from different schedulers cannot be combined.
- Terminal completion, failure, cancellation, eviction, and shutdown dispose
  all pending callbacks and captured continuations.
- Eviction emits no workflow command. A later replay creates a fresh execution
  rather than reusing native continuations.

## Deterministic scheduling

- Runnable fibers receive monotonic sequence numbers and execute FIFO.
- Spawn order is source execution order.
- Activation jobs are applied in their supplied list order.
- Resolving a future appends its waiters in waiter-registration order.
- No hash-table traversal determines runnable or command ordering.
- Command sequence numbers are monotonic per execution and begin at one.
- Commands are returned in emission order.

## Futures and continuations

- A promise resolves at most once; unknown or duplicate external resolutions
  are bridge defects.
- A captured one-shot continuation is continued or discontinued exactly once.
- Awaiting a ready future does not perform an effect.
- Awaiting a pending future outside its owning running scheduler returns a
  structured defect.
- `Future.both` observes both inputs before settling and selects the left error
  when both fail.
- User callback exceptions are contained and reported as scheduler defects.
- The implementation uses typed closures and GADTs, not `Obj.magic` or a
  heterogeneous untyped value store.

## Commands and terminal state

- Input payloads are encoded before scheduling a command.
- Activity outputs are decoded before resolving the typed public future.
- Zero-duration sleep emits no timer.
- Positive sleep emits one timer and resumes only for its exact sequence.
- A workflow emits at most one terminal command.
- Terminal command emission is retained while pending runtime state is torn
  down immediately.
- Malformed bridge jobs fail the execution with a non-retryable bridge error.

## Replay

- Native continuations are cache optimizations and are never serialized.
- Replay reconstructs state by running the workflow again from its start.
- Identical definitions, inputs, and ordered activation jobs must produce
  identical command bytes.
- Workflow code must not use unrecorded wall time, randomness, I/O, process
  state, or nondeterministic iteration to affect commands.
- Replay-safe time, randomness, side effects, patching, and logging APIs are
  required before production release.

## Core boundary assumptions

- Core and every Cargo dependency are pinned and license-audited.
- Activation/completion protobuf bytes cross the language boundary as owned
  buffers with one explicit free path.
- Rust panics, decode errors, and Core failures become explicit bridge errors.
- Foreign runtime threads never call arbitrary OCaml closures.
- Blocking FFI calls occur only while the OCaml runtime lock is released.
