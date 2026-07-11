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

The concrete `Sdk_supervisor.Native` module will instantiate this signature
with its owner-Domain operations. The adapter intentionally does not mention a
readiness-wait symbol: the current bridge exposes a nonblocking poll, while a
future readiness mechanism can be added to the supervisor without changing
the run registry or deterministic translation.

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
- one mutex that serializes polling, execution, and completion submission.

No native pointer, Rust future, continuation, or payload buffer is stored in
the maps. The mutex protects OCaml scheduler state in addition to the
supervisor's own owner-Domain serialization, so two ordinary producer Domains
cannot execute the same run concurrently. Workflow fibers must not call the
adapter directly because supervisor operations may block their producer Domain.

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
   completion. Activity commands retain their complete Core fields and are
   validated before submission. Child-workflow commands remain explicit
   `unsupported` errors because the first semantic protocol has no child
   command variant; no replacement command is fabricated for that unsupported
   path.
7. The completion is submitted through the same supervisor. The run entry is
   removed only after the supervisor confirms completion retirement. Terminal
   commands remove the run; a cache-removal activation also removes it after
   its required empty acknowledgement. Pending timer/activity work keeps the
   run entry.

Activations without initialization must identify a run already in the map.
Unknown run IDs are completed with a non-retryable bridge failure, which
retires the native lease instead of silently ignoring it.

## Rejection and failures

An activation that is valid JSON but cannot be represented by the current
runtime (for example, a child-workflow command before its semantic protocol
record exists) receives a typed `Fail_workflow` completion. `poll` returns
`Ok (Rejected ...)` only after that completion has been accepted, and marks
`lease_retired = true`. If the native completion operation fails, `poll` returns
an error and leaves the run entry in place so the caller can retry or shut down
without a false claim of retirement.

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

## Verification

`test/runtime/test_native_worker_execution.ml` uses a fake semantic queue to
verify:

- first-activation initialization and terminal completion;
- durable timer suspension and resumption through a matching sequence;
- cancellation and cache-eviction removal of suspended runs;
- complete activity-command scheduling and lease retirement;
- unknown run rejection and lease retirement;
- typed cleanup when completion raises, including the unacknowledged-lease
  path when the failure completion itself raises;
- typed propagation of lower-layer malformed-activation errors; and
- duplicate and remote-only registration rejection before worker publication.

The fake tests do not claim live Temporal compatibility. The Compose
acceptance suite remains the gate for the concrete supervisor wiring and real
Temporal Server behavior.
