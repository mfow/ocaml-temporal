# Native workflow execution adapter

This document describes the private OCaml loop that sits between typed native
supervisor operations and the deterministic workflow runtime. It is not a
public worker API. Public workflow definitions remain ordinary values; the
adapter hides the heterogeneous input/output types and the mutable execution
registry behind a private functor result.

## Boundary and ownership

The adapter consumes the typed operations below:

```text
try_poll_workflow : supervisor -> (activation option, error) result
complete_workflow : supervisor -> completion -> (unit, error) result
```

The concrete `Sdk_supervisor.Native` module instantiates this signature with
operations on its owner Domain. The public worker loop also uses two private,
bounded readiness operations (`Wait_workflow` and `Wait_activity`). They do not
consume a task; they only let the Rust/Core lane sleep until work is likely to
be available. The C boundary releases the OCaml runtime lock during that wait,
and the 100 ms bound lets the supervisor admit shutdown even when no task
arrives. Keeping waits outside this adapter means the registry remains usable
with deterministic fake sources and the execution translation does not depend
on a particular scheduling primitive.

The supervisor remains the sole owner of the Rust runtime, client, worker, and
native task ledger. It must decode and semantically validate the JSON returned
by Rust. If that decode fails, it must retire the exact leased item before
returning a typed error. The adapter then propagates that error; it never
pretends that a completion was submitted. This is why the functor takes typed
activations rather than raw JSON bytes.

The adapter owns only OCaml values:

- an immutable map from workflow type name to an existentially typed local
  definition;
- a mutable map from Temporal run ID to its matching typed `Execution.t`;
- a mutable map of workflow completions whose native acknowledgement has not
  yet been proven, with every payload buffer copied into adapter-owned storage;
- one mutex that serializes polling, execution, and completion submission.

No native pointer, Rust future, or continuation is stored in the maps. The
pending completion map owns copied protocol bytes until acknowledgement and
then releases them with the ordinary OCaml value lifetime. The mutex protects
OCaml scheduler state in addition to the supervisor's own owner-Domain
serialization, so two ordinary producer Domains cannot execute the same run
concurrently. Workflow fibers must not call the adapter directly because
supervisor operations may block their producer Domain.

## Worker configuration validation

`Make.create` validates the worker's implicit activity queue before it stores
any workflow definitions or accepts an activation. The queue must be non-empty,
contain no NUL byte, fit within 65,536 bytes, and be valid UTF-8. Invalid input
returns `Error { code = "invalid_configuration"; path = "$.task_queue"; ... }`;
it does not call the supervisor or wait until the first workflow starts. The
same predicate is used when an execution context is created, so a malformed
queue cannot turn into a late `Invalid_argument` after a Temporal lease has
already been accepted.

## One poll transaction

`Native_worker_execution.Make` processes at most one activation per `poll`:

1. A nonblocking empty lane returns `Ok Not_ready`.
2. A typed activation is validated again by
   `Native_execution.translate_activation`. This keeps fake supervisors and
   future alternate sources subject to the same canonical protocol checks as
   the native supervisor.
3. A first job must be exactly one `Initialize_workflow`. Its workflow type is
   looked up in the immutable registration map. Duplicate run IDs, unknown
   workflow types, remote-only definitions, and invalid input argument counts
   become typed bridge failures.
4. The input payload is copied into the runtime representation. Binary metadata
   is rejected because runtime codec metadata is text; no replacement encoding
   is attempted. Zero arguments are decoded as the canonical `binary/null`
   unit payload, one argument is decoded normally, and additional arguments
   are rejected rather than dropped.
5. A typed `Execution.t` is inserted under the run ID before activation jobs
   run. The existing deterministic scheduler applies jobs in order and emits
   commands in creation order.
6. `Native_execution` converts the command batch to a checked semantic
   completion. Activity commands retain their complete Core fields and child
   starts retain their workflow identity and input payload before submission.
   Core child options that the current OCaml runtime does not expose stay at
   explicit defaults; child result resolution is not yet represented by the
   activation protocol.
7. The completion is copied into an adapter-owned pending record before it is
   submitted through the same supervisor. The run entry is removed only after
   the supervisor confirms completion retirement. Terminal commands remove the
   run; a cache-removal activation also removes it after its required empty
   acknowledgement. Pending timer and activity work keeps the run entry. A
   completion containing a child-start command is rejected before submission
   and removes its run because child-resolution activations are not yet safe to
   consume.

Activations without initialization must identify a run already in the map.
Unknown run IDs are completed with a non-retryable bridge failure, which
retires the native lease instead of silently ignoring it.

## Rejection and failures

An activation that is valid JSON but cannot be represented by the current
runtime receives a typed `Fail_workflow` completion. `poll` returns
`Ok (Rejected ...)` only after that completion has been accepted, and marks
`lease_retired = true`. If the native completion operation fails, `poll` returns
an error and leaves the exact completion in the pending map so the caller can
retry or shut down without a false claim of retirement. A retry never reruns
the workflow implementation. `drain` retries every pending completion while
holding the adapter mutex and returns `Ok ()` only after the map is empty.

Malformed JSON or a malformed semantic activation is rejected below this
module by the supervisor's protocol adapter. That lower layer owns the raw
lease token and is the only layer able to retire it safely when no trustworthy
run ID can be decoded. The worker adapter exposes the typed source error with
its bounded code/path/message and performs no unsafe best-effort parsing.

Expected operational failures use `result` values. Unexpected exceptions from
workflow translation, codec execution, or completion are contained as typed
`ocaml_exception`/`completion_failed` diagnostics. The adapter makes one
failure-completion attempt when an ordinary completion raises; if that
acknowledgement also fails, it returns an error and does not claim retirement.
The mutex is still released by `Fun.protect`, so a producer Domain cannot lose
the registry lock or strand a second caller.

## Public worker wiring

`Temporal.Worker.create` validates all registration definitions before opening a
native resource. A `mock://` target selects the deterministic in-memory backend
used by unit tests; an `http://` or `https://` target creates one private
supervisor, connects the Core client, starts one workflow/remote-activity
worker, and installs the two typed adapters described above. The application
still owns the final executable: Rust remains a static implementation detail
behind the private supervisor and no native handle is exposed through the
public API.

`Temporal.Worker.run` first drains both nonblocking lanes. When both are empty,
it alternates the bounded workflow and activity waits so an activity-only load
cannot starve behind workflow readiness (or the reverse). A task-level
workflow/activity failure is completed through Core and then the loop
continues; a transport, protocol, or lifecycle error returns a typed
`Temporal.Error.t`. `shutdown` closes admission, waits for an active loop to
leave the adapters, drains both pending completion maps, and only then
releases worker, client, and Rust runtime state in reverse ownership order. If
either drain fails, native teardown is not started and shutdown remains
retryable. Repeated successful shutdown calls are idempotent.

The semantic translator accepts child-start commands with the workflow identity
and input fields represented by the protocol. The native worker deliberately
gates those completions before submission: Core child options not yet exposed
by the OCaml runtime remain explicit defaults, but child result-resolution jobs
are not yet represented by the activation schema. Sending a start without that
resolution path would strand the parent lease, so the worker returns a typed,
non-retryable failure and removes the local run. A later slice may lift this
gate only after matching child-resolution decoding and lease tests exist. The
live Compose acceptance remains the gate for proving complete
workflow/activity/child behavior end to end. Activity commands are accepted
only when their required identifiers, payloads, timeout policies, and
cancellation options are present; a missing field is rejected in the same
typed way.

## Verification

`test/runtime/test_native_worker_execution.ml` uses a fake semantic queue to
verify:

- first-activation initialization and terminal completion;
- durable timer suspension and resumption through a matching sequence;
- cancellation and cache-eviction removal of suspended runs;
- complete activity-command scheduling and lease retirement;
- child-start translation and the native worker's safe rejection gate while
  child result resolution remains explicitly pending;
- retention and retry of a rejected workflow completion without rerunning the
  workflow, including an explicit adapter drain;
- unknown run rejection and lease retirement;
- typed cleanup when completion raises, including the unacknowledged-lease
  path when the failure completion itself raises;
- typed propagation of lower-layer malformed-activation errors; and
- duplicate and remote-only registration rejection before worker publication;
- rejection of empty, NUL-containing, oversized, and non-UTF-8 worker queues
  before worker publication.

`test/runtime/test_native_worker_lifecycle.ml` is a separate focused regression
file for the shutdown-sensitive path. It rejects the same completion twice:
the initial poll fails, the first drain fails, and the second drain succeeds.
The workflow implementation runs once, the copied completion is submitted
once, and the fake native lease remains present until that final acknowledgement.
This is the contract that lets public worker shutdown retry a transient
completion transport failure safely.

The fake tests do not claim live Temporal compatibility. Focused supervisor and
bridge tests cover readiness and lifecycle ownership; the Compose acceptance
suite remains the gate for real Temporal Server behavior and for the remaining
activity/child-workflow command support.
