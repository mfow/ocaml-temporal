# Native Execution Translation

This document describes the private, pure-OCaml adapter between the checked
workflow protocol and the deterministic workflow runtime. Workflow authors do
not call it directly. Rust converts a Core activation into the checked JSON
document, and the SDK supervisor decodes that document before the private
native-worker adapter invokes this module. The adapter returns a checked
completion; the worker then submits that completion through the supervisor.
`Native_execution` itself owns no native handle and does not submit or retry
anything. The production supervisor owns the lifecycle and invokes the worker
through its typed operations.

## Where the adapter sits

Rust owns Temporal Core, protobuf, network I/O, and native handles. The
protocol module gives that native boundary a small JSON vocabulary with strict
validation. `Temporal_runtime.Native_execution` is the next boundary inward:
it converts that vocabulary into the runtime's typed jobs, runs an existing
`Execution.t`, and converts the commands emitted by that execution back into a
checked protocol completion.

The data flow is therefore:

```text
Temporal Core
    -> Rust Core/protobuf conversion and semantic validation
    -> checked activation JSON
    -> Sdk_supervisor.Native.Protocol_adapter.decode_workflow_activation
    -> Native_execution.translate_activation
    -> Execution.activate
    -> Native_execution.completion_of_commands
    -> Workflow_protocol.encode_completion
    -> Rust protobuf conversion
```

The Rust bridge canonical-encodes the converted activation before retaining its
native lease. The OCaml supervisor decodes that JSON, and
`translate_activation` canonical-encodes the typed value again before it
changes it into runtime jobs. The second pass is intentional: programmatically
constructed OCaml values receive the same closed-object, bounds, payload, and
ordering checks as values received from Rust. Outgoing commands follow the
same rule in reverse: `completion_of_commands` validates the complete command
batch before the worker submits it.

The adapter does not own a Rust handle, call the supervisor, wait on a lock, or
perform I/O. Keeping it pure and below the supervisor makes the conversion
testable without a Temporal server and prevents worker lifecycle concerns from
being mixed with replay logic.

## Activation mapping

`translate_activation` first runs the canonical protocol encoder over the
typed value. This means a value assembled by OCaml receives the same checks as
JSON received from Rust: closed objects, bounded identifiers, payload limits,
normalized times, and activation invariants. No runtime state is changed
until this validation succeeds.

Jobs are then converted in their original list order. Sequence numbers remain
`int64` in OCaml and are checked against Core's unsigned 32-bit range before a
job is created. A child sequence is intentionally allowed twice, once for its
start acknowledgment and once for its terminal result; duplicate events of
the same kind and collisions with another operation kind are rejected. An
unknown sequence is rejected later by `Execution`, which emits a non-retryable
bridge failure rather than silently ignoring a Core event.

| Protocol job | Runtime job | Information retained by the adapter |
| --- | --- | --- |
| `Initialize_workflow` | `Start_workflow` | Workflow ID, type, arguments, randomness seed, attempt, and initialization context are retained in `translated_activation.initialization`. |
| `Resolve_activity` with `Completed None` | `Resolve_activity` with the canonical null payload | The absence of a result remains distinguishable from an ordinary payload. |
| `Resolve_activity` with `Completed (Some payload)` | `Resolve_activity` with a copied runtime payload | Metadata and body bytes are copied before workflow code can observe them. |
| `Resolve_activity` with `Failed` or `Cancelled` | `Resolve_activity` with a typed `Temporal_base.Error.t` | Application/cancellation category, retryability, details, and a bounded diagnostic of structured failure information are retained. |
| `Resolve_child_workflow_start` with `Succeeded` | `Resolve_child_workflow_start` with `Ok run_id` | The translation preserves the run ID; `Workflow_context_store` records it and deliberately does not resolve the child's future yet. |
| `Resolve_child_workflow_start` with `Failed` or `Cancelled` | `Resolve_child_workflow_start` with `Error` | The translation produces a typed child-workflow or cancellation error; `Workflow_context_store` removes the pending child so a rejected start cannot remain suspended forever. |
| `Resolve_child_workflow` with `Completed` | `Resolve_child_workflow` with `Ok payload` | The translation preserves the terminal payload (including canonical null); `Workflow_context_store` resolves the child only after a successful start acknowledgment. |
| `Resolve_child_workflow` with `Failed` or `Cancelled` | `Resolve_child_workflow` with `Error` | Child failure identity, retry state, details, cancellation category, and the bounded recursive diagnostic are retained. |
| `Fire_timer` | `Fire_timer` | The exact sequence is retained. |
| `Cancel_workflow` | `Cancel_workflow` | The reason is retained in `translated_activation.cancellation_reason`. |
| `Remove_from_cache` | `Remove_from_cache` | The message and eviction reason are retained in `translated_activation.cache_removal`. |

The runtime currently uses marker jobs for initialization, cancellation, and
eviction. Retaining the protocol records alongside those markers is important:
the adapter must not throw away an identity, replay flag, or eviction reason
just because the first execution kernel does not need it yet.

`activation_jobs` is only a convenience projection. Code that needs replay
metadata or eviction details should use the complete `translated_activation`
record instead.

## Command mapping

`command_to_protocol` converts one runtime command only when the two types have
an exact, lossless representation. `completion_of_commands` preserves the
runtime's emission order and runs `Workflow_protocol.encode_completion` over
the complete result before returning it to the bridge.

| Runtime command | Protocol command | Notes |
| --- | --- | --- |
| `Request_cancel_activity` | `Request_cancel_activity` | The sequence is range-checked. |
| `Schedule_activity` | `Schedule_activity` | Activity ID, type, task queue, argument payloads, timeout policies, optional retry policy, cancellation policy, and eager-execution flag are validated and copied. Defaults are applied by the workflow context before this boundary; the translator never invents them. |
| `Start_child_workflow` | `Start_child_workflow` | The sequence, child workflow ID and type, one copied input payload, explicit child-cancellation policy, and optional retry policy are retained. Retry intervals and coefficient bits are validated before Core conversion. Namespace is worker-scoped rather than repeated in the OCaml command: Rust injects the already validated worker namespace before sending the command to Core. Task queue, workflow timeouts, close/reuse policies, cron, headers, memo, search attributes, versioning, and priority are not represented by the current OCaml command, so Rust sends their Core defaults and its reverse conversion rejects a non-default Core value instead of silently discarding it. |
| `Cancel_child_workflow` | `Cancel_child_workflow` | The sequence and validated reason are copied into Core's `CancelChildWorkflowExecution` command. The policy from the matching start command determines whether Core reports immediately or waits for cancellation progress. |
| `Start_timer` | `Start_timer` | Non-negative milliseconds are split into exact seconds and nanoseconds; no floating-point conversion is used. |
| `Cancel_timer` | `Cancel_timer` | The sequence is range-checked. |
| `Complete_workflow` with the canonical unit/null payload | `Complete_workflow { result = None }` | The nullable protocol result is preserved. |
| `Complete_workflow` with another payload | `Complete_workflow { result = Some payload }` | Metadata must be valid UTF-8 in the current runtime payload type and body bytes are copied. |
| `Fail_workflow` | `Fail_workflow` with an OCaml application or cancellation failure | Details are copied and the runtime category/retryability are retained. Recursive Core-only fields are represented in a bounded diagnostic until the runtime has a richer error type. |
| `Continue_as_new` | `Continue_as_new` with the successor workflow type and one copied input payload | This is terminal direct-style control flow. The scheduler stops the current workflow fiber after buffering the command, and the translator rejects invalid workflow identifiers or payload metadata before a completion is sent. |
| `Cancel_workflow_execution` | `Cancel_workflow_execution` | This is already an exact marker. |

Child starts and both Core child-resolution jobs now have closed semantic
records. The runtime keeps one child state per sequence: the start
acknowledgment stores the assigned run ID, a start failure removes the pending
future, and a terminal result removes it only after a successful start. A
terminal result received before its start acknowledgment is a bridge defect.
This two-stage lifecycle mirrors Core's event order and prevents a started
child from being mistaken for a completed child or a failed start from being
left suspended indefinitely.

### Activity command defaults and options

`Temporal.Activity.start` keeps its original two-argument form and also accepts
labelled options for the Core fields. The activity type comes from the
definition name, and the input is sent as the one-element argument list.
Omitting an activity ID creates `ocaml-activity-<sequence>` from the
workflow-local deterministic sequence. Omitting a task queue uses the queue
captured in the execution context; native workers populate that value from
their worker configuration, while synthetic executions use `default`.

Temporal requires either a schedule-to-close or start-to-close timeout. If no
timeout is supplied, the context emits a deterministic 60-second
start-to-close timeout so existing workflows remain valid. All supplied
timeouts are exact non-negative milliseconds and are converted to normalized
protocol durations without floating-point arithmetic. Cancellation defaults to `Try_cancel`,
and `do_not_eagerly_execute` defaults to `false`. An optional `~retry_policy` is immutable
and validated before the activity command is allocated. Its initial interval must
be positive, its maximum interval must not be smaller, its finite backoff
coefficient must be at least `1.0`, and its maximum-attempt count is a signed 32-bit
value where `0` means unlimited attempts. The policy's coefficient crosses JSON as
canonical unsigned decimal IEEE-754 bits rather than a JSON float; this keeps
OCaml, Rust, and Core lossless and makes replay independent of float formatting.
Omitting the option emits the required JSON member as `null`, preserving the
distinction between “no policy” and a concrete policy. Invalid IDs, queues,
payload metadata, negative durations in an internal command, malformed retry
policies, and missing required timeout policies return typed translation errors
before a completion is emitted. The public `Temporal.Duration.of_ms` constructor
rejects a negative value earlier with `Invalid_argument`, because a negative duration
is a programmer configuration defect rather than an operational workflow failure.

These errors are a planned compatibility boundary, not a hidden drop path.
Activity scheduling and child lifecycle translation are enabled because the
runtime, protocol, and translator carry every field currently exposed by the
OCaml API, including child retry policy. Future child options remain explicit
Core defaults until the public OCaml surface models them; a non-default Core
value is still rejected rather than silently discarded.

## Error and ownership rules

The public result type is used for ordinary translation failures. Its stable
view contains a code, a JSON-style path, and a short diagnostic; it never
contains payload bytes. `invalid_message` means the value violates a protocol
or runtime invariant. `unsupported` means the value is valid but cannot be
represented without losing meaning. Programmer defects remain exceptions in
the underlying execution kernel, while expected Temporal outcomes are typed
results.

Every payload crossing this boundary is copied. Binary protocol metadata cannot
be represented by the runtime's string metadata map and therefore returns
`unsupported` instead of being decoded with replacement characters. Runtime
metadata is validated as UTF-8 before it becomes protocol bytes. Failure causes
are traversed with a fixed depth limit so malformed recursive values cannot
consume unbounded stack or memory.

Retryability is derived by the shared `Workflow_protocol.failure_non_retryable`
rule used by both the worker and client terminal-result paths. An application
failure contributes its own `non_retryable` flag. An activity or child-workflow
wrapper uses `NonRetryableFailure` or `MaximumAttemptsReached` when Core gives
one of those explicit states; `Timeout`, `InProgress`, and the other explicit
retry states remain retryable. `RetryPolicyNotSet` and `Unspecified` do not
override the nested application flag, which is how Core reports a child that
failed non-retryably without having its own child retry policy. The bounded
walk preserves this distinction without making malformed recursive causes
unbounded.

The adapter itself has no mutable global state. A worker may keep the returned
`Execution.t` and translated records in its owner Domain, but this module does
not share those values between Domains and does not expose native pointers.

## Verification

`test/runtime/test_native_execution.ml` covers metadata and job ordering,
initialization retention, activity success/failure conversion, child
start/terminal success ordering and duplicate detection, timers, eviction,
terminal completion, duplicate/unknown sequences, payload validation, and
command-option validation. These tests exercise the pure translator without a
Temporal Server. Worker lifecycle behavior is covered separately by
`test/runtime/test_native_worker_execution.ml`, including lease retirement,
workflow cancellation/eviction, child-start failure, terminal child failure,
and retryability. Remote activity cancellation and heartbeat translation have
their own adapter coverage in `test/runtime/test_native_activity_execution.ml`.

The public native worker invokes this adapter through the owner-Domain
supervisor. The live Compose gate exercises timer and activity success paths
and includes one two-public-binary parent/child result path against Temporal
Server. Child failure, cancellation, retry, replay, and recovery remain
deferred live acceptance scenarios even though several of their local worker
paths are already tested.
