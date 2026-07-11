# Native Core Bridge ABI

This document is for contributors changing the OCaml/C/Rust boundary. Workflow
authors do not call this interface directly.

The product is an OCaml Temporal SDK, not only a Temporal service client. It
implements workflow workers and the deterministic workflow runtime as well as
client operations that start and observe workflows. The final worker process
is owned and launched by OCaml. The public OCaml library calls private C stubs,
which call the versioned Rust ABI documented here. Rust links the official
Temporal Core library and never invokes arbitrary OCaml functions from its
background threads.

## Version and symbols

ABI version 1 uses only symbols beginning with `ocaml_temporal_core_v1_`.
Before using the bridge, OCaml asks Rust which ABI version it implements and
checks that it matches `OCAML_TEMPORAL_CORE_ABI_VERSION`. The bridge represents
the Rust runtime, and will represent server connections and workers, with
opaque handles. “Opaque” means OCaml can pass a handle back to Rust but cannot
inspect the Rust object it refers to. A connection handle is only one internal
part of the SDK; the public package is not merely a service client.

The canonical header is
`rust/core-bridge/include/ocaml_temporal_core.h`. Both Rust and C compile-time
assertions protect the status width and field ordering of the documented
`repr(C)` structures.

## Semantic workflow adapter

`rust/core-bridge/src/workflow_protocol.rs` is the Rust-only protobuf boundary
for the first activation/completion slice. It converts pinned official Core
types to a closed semantic model, serializes that model as strict JSON, and
performs the inverse conversion for workflow commands. The private OCaml module
`Temporal_protocol.Workflow_protocol` implements the same model and validation
without importing protobuf definitions.

Both encoders reparse their own output before it can cross the native boundary.
Both decoders reject duplicate or unknown fields, unknown variants, numeric
range violations, non-canonical base64, invalid workflow invariants, and
oversized values. Core fields not represented by the current semantic slice are
accepted only at their documented default; a non-default value returns a typed
`Unsupported` conversion error rather than being lost. See the
[protocol reference](core-protocol.md), machine-readable schemas, and
[ADR 0006](../decisions/0006-first-workflow-semantic-protocol.md).

After protocol decoding, the private pure-OCaml
[`Native_execution`](native-execution-translation.md) adapter translates jobs
into the deterministic execution kernel and translates its commands back into
the checked completion model. It preserves activation metadata, initialization
records, sequence ordering, cancellation reasons, eviction details, and copied
payload bytes without exposing Rust state. A valid command is accepted only
when its fields have an exact protocol representation. The current runtime
activity command is missing Core's activity ID, task queue, argument list,
timeouts, and cancellation policy, and the first protocol has no child
workflow command; those commands return typed `unsupported` errors until both
schemas grow. No Temporal defaults are guessed and no command is silently
dropped. See the translation reference for the complete mapping table and
test coverage.

## Native client start and exact-run wait

The private Rust client adapter adds two JSON ABI operations:
`ocaml_temporal_core_v1_client_start_workflow_json` and
`ocaml_temporal_core_v1_client_wait_workflow_json`. They are deliberately
lower-level than the public OCaml `Client` module. The adapter uses Core's raw
`WorkflowService` trait because workflow type names and payloads are dynamic at
the OCaml boundary; it does not instantiate Rust's statically typed workflow
definitions.

The start request contains `namespace`, `workflow_id`, `workflow_type`,
`task_queue`, and an ordered `input` payload array. Optional start policies are
not silently invented in this first slice: Core receives its documented server
defaults. The successful response contains the namespace, workflow ID, and
run ID allocated by Temporal. Both request and response are strictly decoded,
re-encoded, and reparsed before crossing the boundary.

The wait request names `namespace`, `workflow_id`, and one concrete `run_id`.
There is no `follow_runs` escape hatch in the document: the operation always
uses a close-event history long poll for that exact run. Completed, failed, and
timed-out close events retain any successor run ID exposed by Core. A
continued-as-new close is returned as a terminal `continued_as_new` outcome
with a required successor execution reference; the bridge never follows that
run implicitly. This prevents an exact-run caller from accidentally observing a
different execution identity.

Temporal AlreadyStarted responses use status `12` and a closed JSON error body
(`kind`, `workflow_id`, `existing_run_id`) rather than copying gRPC server text.
Other RPC failures contain only a stable status code. Core payload and failure
conversion errors use a `protocol` error kind with a closed conversion code;
they never include payload bytes or server diagnostics. Machine-readable
schemas for these documents live under
`docs/schemas/bridge/client-*.schema.json`.

## Result and buffer ownership

Every fallible operation accepts a writable result pointer and returns the same
status stored in that result. Status zero is success. Nonzero statuses describe
invalid arguments, ABI mismatch, a contained Rust panic, an invalid lifecycle
transition, a worker failure, or a semantic protocol failure. Worker polling
also uses two expected statuses: `NOT_READY` means a lane currently has no
queued task, while `OUTSTANDING_TASKS` means shutdown cannot finalize until
the language side completes leased work.

A result has one success buffer and one error buffer. At most one owns memory:

- success may place arbitrary binary bytes in `value`;
- failure may place a UTF-8 diagnostic in `error`;
- an empty allocation is always represented as `{ NULL, 0 }`.

Rust owns both allocations. The caller may copy their bytes but must never
mutate or directly free their fields. It must call
`ocaml_temporal_core_v1_result_free` exactly once after consuming an initialized
result. That function clears the object, so accidentally calling it again on
the same object is safe. Copying a live result structure creates no new
ownership; freeing both copies is invalid.

An output object may be uninitialized, but it must not contain a live owned
result when passed to another operation. Free the previous result first.

### OCaml ownership guard

The private C stubs allocate an OCaml custom block before entering Rust. That
block is the sole owner of the ABI result and has a finalizer which calls
`ocaml_temporal_core_v1_result_free`. The OCaml wrapper also uses
`Fun.protect` to release the result deterministically after copying its bytes.
This gives every path two compatible safeguards: normal operation frees
immediately, while an OCaml allocation failure or other exception leaves a
rooted/finalizable owner rather than orphaning Rust memory. Disposal is
idempotent, so a later finalizer after deterministic disposal is harmless.

Returned bytes are copied once, directly from the live Rust buffer into the
OCaml string/bytes allocation. Inputs that must survive a blocking call are
copied to temporary C storage before the runtime lock is released, then freed
immediately after the lock is reacquired. Neither side directly frees an
allocation made by the other side.

### Private OCaml worker operations

`Temporal_core_bridge.Native_bridge` exposes eight private wrappers over the
poll, readiness, completion, and rejection symbols. They are used by the
native worker adapter and are not part of the public workflow-authoring API:

| OCaml operation | Native behavior | Successful value |
| --- | --- | --- |
| `worker_try_poll_workflow` | Drain one already-ready workflow activation without waiting | semantic workflow JSON bytes |
| `worker_wait_workflow` | Wait for workflow readiness without consuming a task | `unit` wake signal |
| `worker_complete_workflow_json` | Validate and complete one leased workflow activation | `unit` |
| `worker_reject_workflow_json` | Retire the lease when OCaml cannot decode the exact Rust-produced activation document | `unit` |
| `worker_try_poll_activity` | Drain one already-ready remote activity task without waiting | semantic activity JSON bytes |
| `worker_wait_activity` | Wait for remote-activity readiness without consuming a task | `unit` wake signal |
| `worker_complete_activity_json` | Validate and complete one leased activity task | `unit` |
| `worker_reject_activity_json` | Retire the lease when OCaml cannot decode the exact Rust-produced activity document | `unit` |

The two poll functions return `Error { status = Not_ready; _ }` when their
independent Rust ready queues are empty. This is normal scheduling state, not a
worker defect. A readiness wait returns immediately when its queue is already
populated, wakes when its lane publishes a task or fatal error, and returns an
invalid-state error after normal shutdown has drained queued messages. A quiet
lane returns `Not_ready` after a bounded 100 ms wait. That bound is intentional:
the supervisor mailbox must regain control periodically so a queued shutdown
operation cannot be stranded behind a blocking readiness handler. Completion
functions copy the
caller-provided OCaml `bytes` into temporary C storage, release the OCaml
runtime lock for the synchronous Rust submission, then free that copy before
returning. Rust validates the complete JSON document and checks the run ID or
opaque activity token against its ownership ledger, so a duplicate or stale
completion cannot silently reach Core.

If OCaml rejects poll bytes, the supervisor returns that same byte document to
the corresponding rejection operation. Rust bounds and strictly decodes it
again; callers never supply a guessed run ID or task token. A workflow document
must equal the complete semantic activation retained at handoff. An activity
document must equal one complete semantic task retained under its canonical
opaque token; cancellation updates using the same token are retained alongside
the earlier task rather than overwriting it. Those documents still represent
one ledger obligation per token, so successful completion or rejection retires
that obligation and clears every retained document for the token. Changed identity or content is a
protocol failure and cannot retire the real lease. The malformed-byte case is
defensive: successful Rust poll encoding cannot produce malformed JSON, but
both language decoders and both rejection entry points still validate it.

`Sdk_supervisor.Native` is the private OCaml adapter for these ABI version 1
operations. It exposes a typed GADT rather than raw JSON bytes:

| Supervisor operation | Result and boundary behavior |
| --- | --- |
| `Try_poll_workflow` | `Workflow_protocol.activation option`; `None` means the workflow lane was empty at that instant |
| `Complete_workflow completion` | canonical strict JSON is generated and reparsed before the native completion call |
| `Try_poll_activity` | `Activity_protocol.task option`; `None` means the activity lane was empty at that instant |
| `Complete_activity completion` | the opaque token and result are validated before the native completion call |

All four operations enter the same bounded mailbox as client and worker
lifecycle changes. A poll, completion, worker shutdown, and runtime shutdown
therefore cannot race native graph state. The pure protocol conversion module
is visible only from the private supervisor library so both serialization
directions can be tested without constructing a Core worker.

The two Rust readiness signals use one mutex-protected pending count per lane.
The poll task holds that mutex while it sends a message and increments the
count; the supervisor holds it while receiving and decrementing. This makes a
send and its wake notification one linearizable operation and prevents a
notification-before-wait or send/receive reordering race. Shutdown closes both
signals before asking Core to wake its polls, while in-flight poll results may
still be queued and are always drained before the terminal state is reported.

These wrappers deliberately return the same owned-response shape as lifecycle
operations. The OCaml `decode` helper copies success or diagnostic bytes and
always calls `response_free` under `Fun.protect`; the C custom-block finalizer
therefore remains a fallback for allocation failures or exceptions. No Rust
poll lane calls an OCaml closure, and no bridge result retains an OCaml heap
pointer after the C call returns.

## Pointer and panic contract

Null output/result pointers return `INVALID_ARGUMENT` without being
dereferenced. A null input pointer is valid only when its length is zero.
As with any C byte-span API, a non-null input pointer must identify a readable
allocation of the stated length and must not overlap the output object.

Every fallible exported operation contains Rust panics before they can unwind
through C. A contained panic becomes `STATUS_PANIC` and an owned diagnostic.
The Rust integration suite invokes the common wrapper with a deliberate panic;
the panic test hook is not exported in the C header and is not part of the
stable ABI.

## Stateful handle ownership

The reserved runtime, client, and worker handles are opaque references to
Rust-owned SDK state, not OS handles and not public OCaml values:

- a runtime owns Tokio and shared Core infrastructure;
- a client owns one cluster connection and its authentication/configuration;
- a worker owns polling and completion state for a task queue configuration.

A normal process is expected to have one runtime, usually one client, and one
or a small number of workers. The intended OCaml design is therefore one
supervisor actor per SDK instance, not one actor per handle. A dedicated OCaml
Domain owns the entire runtime/client/worker graph. Calls from other Domains
enter a synchronized MPSC mailbox and receive typed one-shot `result` replies.
The supervisor serializes lifecycle transitions and destroys workers before
clients and the runtime. Rust retains internal Tokio concurrency; workflow
executions retain their separate deterministic effect schedulers.

The implemented private supervisor owns the real runtime, one official client
connection, and one Core worker for workflows and remote activities. Its backend protocol exposes
typed GADT operations but never the owner-confined state, preventing a raw
handle from escaping through an otherwise convenient callback. See
[ADR 0004](../decisions/0004-sdk-instance-supervisor.md) for its lifecycle,
failure, and scheduler contracts.

### Lifecycle configuration JSON

The OCaml wrapper constructs two private JSON documents; applications never
assemble these strings themselves. The client document contains exactly
`target_url` and `identity`. The worker document contains exactly `namespace`,
`task_queue`, `build_id`, `max_cached_workflows`,
`max_outstanding_workflow_tasks`, `max_concurrent_workflow_task_polls`, and
`graceful_shutdown_timeout_ms`. Closed Draft 2020-12 schemas live under
`docs/schemas/bridge/`.

Both sides reject missing, unknown, wrongly typed, empty required, and
out-of-range values. The whole document and each individual string have a
65,536-byte private transport-safety ceiling. That ceiling is not a Temporal
identifier policy: Core and Server retain semantic authority. JSON Schema
measures characters rather than encoded bytes, so the bilateral runtime
validators enforce the byte ceiling.

Client connection and worker namespace validation are synchronous ABI calls.
The C stub copies input before releasing the OCaml runtime lock, Rust performs
the Tokio wait, and the stub reacquires the lock only to copy the result. A
failed connection publishes no client. A failed worker construction or
validation gracefully finalizes the temporary worker and leaves the client
available for a corrected retry.

The worker owns two Tokio poll lanes: exactly one calls Core's workflow poll and
exactly one calls its remote-activity poll. Local activities and Nexus remain
disabled. Each lane writes without waiting to its own ready queue. Core's
configured outstanding-task permits bound the number of queued tasks; using a
second bounded send would deadlock shutdown if the supervisor joined a lane
while that lane waited for the supervisor to drain its full queue. The OCaml
supervisor takes ready work through non-blocking ABI operations and uses the
bounded readiness waits only from its owner-domain mailbox handler; the C stubs
release the OCaml runtime lock while Rust waits. No Tokio thread enters OCaml
and no long Core poll occupies the supervisor Domain. Keeping the lanes
independent prevents an idle activity poll from delaying workflow completion,
or vice versa.

ABI version 1 includes private readiness-wait symbols for the two independent
poll lanes. The supervisor may invoke them only from the owner-domain mailbox
handler; the C boundary releases the OCaml runtime lock while Rust waits and
reacquires it before returning. Callers must not turn a readiness wait into a
blocking condition wait on a workflow scheduler fiber or allow a second owner
to access the native worker graph.

One mutex-protected ledger is the authority for every task Core expects the
language runtime to complete. A task enters the ledger before its ready message
is queued, changes from Rust-owned ready state to OCaml-leased state at the
non-blocking handoff, and leaves only after Core accepts the exact matching run
ID or opaque activity token. Activity cancellation reuses the original token
and therefore does not create a second completion debt.

There is one deliberate pre-handoff exception. If a leased Core value cannot
be converted to the closed semantic JSON protocol, OCaml never receives its
run ID or task token and therefore cannot complete it. Rust generates exactly
one workflow-task or activity failure for Core and retires the inaccessible
lease on every outcome. A rejected generated completion remains a fatal worker
error, but it cannot also leave a fabricated language-side debt that blocks
shutdown forever. Regression tests cover this rule independently for workflow
and activity conversion failures.

There is also a post-handoff decode-failure path for version or implementation
drift between the two strict decoders. OCaml preserves its original protocol
error, returns the exact Rust-produced bytes to the private rejection ABI, and
never reflects those bytes in diagnostics. Rust accepts rejection only after
full semantic equality with retained handoff state. It then generates the Core
failure and retires both the ledger debt and retained semantic state even if
Core reports that generated failure as unsuccessful. This prevents shutdown
from waiting forever while keeping the original decode failure primary.

Shutdown first closes ledger admission and both readiness signals, then asks
Core to wake both polls and joins the lane tasks. Existing ready and leased
work remains completable while the worker drains. Core finalization is refused
until the ledger is empty, and only then consumes the worker before client and
runtime destruction. The
garbage-collection fallback cannot obtain missing language completions; after
waking and joining the lanes it force-drops an undrained worker on the dedicated
cleanup thread. This preserves memory ownership and collector progress, while
explicit supervisor shutdown remains the required graceful path.

### Runtime destruction

Creating an SDK runtime starts its Tokio executor and a small Rust cleanup
thread dedicated to that owner. Explicit OCaml shutdown atomically detaches the
opaque pointer, releases the OCaml runtime lock, transfers Core to the cleanup
thread, and waits until Core's destructor has returned. This makes orderly
shutdown observable without preventing other OCaml Domains from running.

The OCaml custom-block finalizer is a fallback for abandoned runtime values. It
performs the same atomic detach and ownership transfer but does not wait. Core
is therefore never destroyed by the OCaml garbage collector thread, whose
progress must not depend on Tokio shutdown. Both paths clear the handle before
transfer and are idempotent; exactly one path can own the native graph. Cleanup
finalizes worker, drops client, then drops Core even when callers did not
explicitly close the children.

## Verification

Rust integration tests cover version negotiation, status propagation, binary
and zero-length buffers, invalid null pointers, bounded blocking, repeated
result disposal, panic containment, explicit runtime closure, and completion of
the asynchronous finalizer fallback. The latter runs in an isolated test
process and observes monotonic cleanup counters only after Core's destructor
returns, preventing a parallel test from producing a false positive. A C11
harness compiles against the public header, links the actual static archive,
exercises the ownership contract, and runs with Address Sanitizer and
UndefinedBehavior Sanitizer in the development container. An OCaml two-Domain
test calls the linked Rust archive and proves another Domain progresses during
a native wait. An install
smoke test builds a fresh OCaml executable from the staged package and invokes
the negotiated ABI through the public `Temporal.Runtime_info` module.

The Dune rule asks `rustc --print=native-static-libs` for the exact native
libraries required by the static archive and consumes the resulting ordered
flags from a generated S-expression file. This keeps platform linker knowledge
owned by the pinned Rust compiler instead of duplicating a fragile Linux,
macOS, and Windows library list in the OCaml build.

The C binding is a Dune `foreign_library`, so Dune first compiles it into a
plain static archive without applying Rust's system-library flags. The OCaml
library then references both that C archive and the Rust archive, and applies
the generated flags only when linking a consumer. The workspace also disables
dynamically linked foreign archives. The internal OCaml library uses
`no_dynlink`, because a native plugin (`.cmxs`) would be another dynamic bridge
artifact and is neither supported nor needed by the final executable.

The supported deployment artifact is an OCaml-owned native executable; the
project does not need a separately loadable bridge DLL. This distinction is
important on Windows: Rust correctly reports GNU linker tokens for the final
native link, but FlexDLL cannot reinterpret all of those tokens while
constructing an intermediate OCaml stub DLL. Keeping the C and Rust inputs as
static foreign archives removes that unnecessary link step without changing
the installed OCaml API or final executable.
