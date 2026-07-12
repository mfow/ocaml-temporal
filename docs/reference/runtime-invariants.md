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
- `Future.both` and `Future.all` observe every input before settling and select
  the first error in input order; successful `all` values retain input order.
- `Future.race` and `Future.first` settle on the first completion, including an
  error, without cancelling losers. Already-ready inputs use registration order;
  pending inputs use deterministic callback order.
- Combining futures from different executions returns a ready typed defect
  owned by the leading input rather than raising an operational exception.
- User callback exceptions are contained and reported as scheduler defects.
- The implementation uses typed closures and GADTs, not `Obj.magic` or a
  heterogeneous untyped value store.

## Commands and terminal state

- Input payloads are encoded before scheduling a command.
- Activity outputs are decoded before resolving the typed public future.
- Child-workflow IDs are explicit, non-empty, valid UTF-8, and at most 65,536
  UTF-8 bytes. Invalid identity consumes neither a sequence nor a command. A
  child resolver is registered before its command is emitted. Core resolves a
  child in two stages: the start acknowledgment stores its non-empty run ID,
  while a later terminal resolution removes the resolver and decodes its
  payload. A start failure removes and resolves the future immediately. A
  terminal result before start, duplicate acknowledgment, or unknown sequence
  is a non-retryable bridge defect; no event is silently dropped.
- Activities, child workflows, and timers share one monotonic command sequence.
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
- Rust alone handles Temporal/Core protobuf. Strict JSON activation and
  completion documents cross the language boundary as owned buffers with one
  explicit free path; OCaml copies them into typed values before execution.
- The private supervisor validates native poll bytes before returning typed
  workflow or activity values to another Domain. It canonically encodes and
  reparses typed completions before entering C.
- If OCaml cannot decode a successful poll, it returns the exact untouched
  Rust document to the private rejection ABI. Rust requires full semantic
  equality with retained handoff state before retiring the lease; changed IDs,
  tokens, or content cannot consume real outstanding work. Rejection cleanup
  removes ledger and semantic ownership together even when Core reports an
  error, while the original OCaml protocol failure remains the primary result.
- Native `Not_ready` is represented as `Ok None`. ABI version 1 has no
  readiness wait, so no workflow fiber or effect scheduler blocks on a native
  lock, condition variable, or timer while waiting for a poll lane.
- Rust panics, decode errors, and Core failures become explicit bridge errors.
- Foreign runtime threads never call arbitrary OCaml closures.
- Blocking FFI calls occur only while the OCaml runtime lock is released.
- Worker readiness waits are bounded to 100 ms and return `Not_ready` on a
  quiet lane, so a supervisor handler cannot strand a queued shutdown request.
- Each Rust poll lane owns one mutex-protected pending count. Producers hold
  that mutex while publishing a queue message and its wake notification;
  the supervisor holds it while receiving and decrementing. A wake is never
  considered proof of readiness without rechecking the protected predicate.
- Shutdown closes both readiness signals before waking Core polls, but queued
  messages always take precedence over terminal state and are drained before a
  readiness wait reports shutdown or a fatal lane error.

## Native activation translation

- `Temporal_runtime.Native_execution` is a pure OCaml boundary below the
  supervisor. It owns no Rust handle, performs no I/O, and does not block a
  workflow scheduler.
- A typed activation is revalidated with the canonical protocol encoder before
  any execution state is touched. Sequence numbers, identifiers, payloads,
  timestamps, ordering, and closed-object invariants therefore have one
  validation path for JSON input and programmatic OCaml values.
- A rejected activation job is side-effect free: malformed child-resolution
  JSON cannot allocate a sequence, consume a resolver, or resolve a future.
  Lifecycle checks then reject terminal-before-start, duplicate start, duplicate
  terminal, and unknown-sequence messages as bridge defects without replacing
  the state established by a valid message.
- Activation jobs and emitted commands retain source ordering. Every payload
  is copied; binary protocol metadata that cannot be represented by the
  runtime's string metadata map is rejected rather than lossy-decoded.
- Initialization, cancellation, replay metadata, and cache-eviction details
  remain available in the translated activation even where the first runtime
  kernel uses only a marker job. Eviction is acknowledged with an empty
  completion and never emits workflow commands.
- A valid value with no lossless representation is an explicit typed
  `unsupported` error. Activity commands carry every exposed Core field, and
  child-start commands carry the workflow identity and input payload. Child
  resolution retains start run IDs, terminal payloads, typed failure info, and
  cancellation state. Options not yet exposed by the OCaml runtime remain
  explicit Core defaults; the adapter never fabricates a language-level option
  or silently drops a non-default value.
- Unknown or duplicate operation sequences are bridge defects. They fail the
  execution rather than being ignored, because ignoring them would make
  replay diverge from the history supplied by Core.

## Supervisor mailbox

- One mailbox owner Domain invokes every handler; producers never execute a
  handler while admitting or awaiting work.
- The bounded FIFO order is the total order of successful enqueue mutations
  under the mailbox mutex. One producer's program order is preserved;
  concurrent producers have no stronger order before those mutations.
- SDK shutdown is admitted through the mailbox's reserved terminal slot. Its
  FIFO append and `Open` to `Closing` transition happen under the same mutex;
  it may temporarily raise the waiting queue to `capacity + 1`, and no later
  normal request can be admitted ahead of it.
- Queue and lifecycle state are data-race free. Every condition wait rechecks
  its protected predicate after waking.
- Normal close rejects new work and drains all admitted work before the owner
  stops. An unexpected handler exception rejects new work, discards queued
  posts, and settles the active and queued calls with the same failure.
- A terminal reply remains owned by its admitted queue entry until the owner
  settles it. Dropping the caller's pending capability cannot strand the owner
  or change the terminal result observed by `join`.
- Blocking mailbox entry points run only on ordinary producer Domains. Future
  Eio or workflow-effect adapters must offload them rather than blocking a
  cooperative scheduler Domain.
- A handler never calls `post`, `call`, or `join` on its own processor.
  `call` and `join` cannot complete while the sole owner is executing that
  handler, and `post` can block if the bounded queue is full. A handler may
  call `close`, which does not wait for the owner and preserves drain semantics.

## SDK instance supervisor

- Exactly one dedicated owner Domain creates, uses, and closes the complete
  runtime/client/worker graph for an SDK instance. Individual native handles do
  not receive their own actors.
- Backend state never appears in a producer-facing operation or result. Typed
  GADT operations may expose ordinary copied values but cannot return a raw
  native handle.
- Expected operation errors leave a running graph usable. An unexpected
  backend exception marks the graph terminal, attempts cleanup exactly once,
  and becomes the common mailbox failure for active, queued, and later calls.
- Shutdown is admitted in FIFO order, invalidates the graph before later work
  can use it, closes and joins the owner, and caches the exact terminal result.
  Repeated or concurrent shutdown invokes backend destruction at most once.
- A shutdown which races a completed or abandoned asynchronous start drains
  every pending ticket, aborts and joins each Tokio task, and only then
  releases the client/Core graph. A result already queued for an abandoned
  ticket is discarded with its receiver; it cannot cause a second task join or
  a second native free.
- A backend shutdown result, including `Error`, means the graph has been
  consumed or invalidated. A retryable operation must not masquerade as
  terminal shutdown while it still owns live resources.
- Supervisor entry points may block and run only on ordinary producer Domains.
  Fiber runtimes must offload them; deterministic workflow schedulers must not
  invoke them directly.
- The abandoned-instance finalizer never calls a blocking supervisor entry
  point itself. It delegates normal shutdown to a dedicated system thread; an
  already completed explicit shutdown schedules no redundant cleanup.
- A handler that waits for native worker readiness uses the bounded bridge wait
  and returns to the mailbox between retries; it never performs an indefinite
  condition wait that could block the mailbox's reserved shutdown transition.
