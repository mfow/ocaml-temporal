# Workflow patching

`Temporal.Workflow.patched` is the first workflow-versioning primitive in the
public OCaml API. It supports the initial **patch-in** step: a workflow can
retain its legacy branch for histories created before a behavior change while
new executions take the new branch.

It is not a general deployment-versioning system. Public patch deprecation and
removal, worker deployment/build-ID routing, side effects, arbitrary historic
compatibility, and migration tooling remain separate work.

## Authoring contract

Give each behavioral change a stable, descriptive identifier:

```ocaml
let process_order order =
  let open Temporal.Result_syntax in
  if Temporal.Workflow.patched ~id:"orders.validate-address.v2" then
    let* () = validate_address order in
    ship order
  else ship order
```

For a newly started execution, the first call returns `true` and emits a
non-deprecated patch-marker command. During replay, it returns `true` only
when Temporal Core reports that the marker exists in that execution's history;
replay of an older history without the marker returns `false`. The decision is
retained for the execution, so ordinary OCaml helper functions can call
`patched` without threading version state through their arguments.

Patch IDs are durable history keys. They must be non-empty, valid UTF-8,
NUL-free, and no more than 65,536 bytes. Invalid IDs and calls outside workflow
execution raise `Invalid_argument` because they are programmer defects. Never
reuse an ID for a different change, derive it from mutable configuration, or
build it from nondeterministic data.

Every call emits the idempotent `SetPatchMarker` command, including repeated
calls with the same ID. The OCaml runtime must not locally deduplicate those
commands: Core owns durable marker state and history-machine deduplication.
An emitted completion command is not itself history evidence; the live
acceptance below checks the normalized server history separately.

## Replay contract

The bridge preserves this order for each workflow task:

1. Rust converts Core's `NotifyHasPatch` job into the closed private JSON
   protocol.
2. OCaml validates the complete activation and installs the execution-local
   replay and patch-notification state before workflow fibers run.
3. `Temporal.Workflow.patched` selects the execution-local decision and emits
   `SetPatchMarker { deprecated = false }`.
4. Rust validates the completion and converts the marker back to Core's
   protobuf command.

The decision must not depend on environment variables, current deployment,
process-global mutable state, wall-clock time, or whether another workflow run
has seen the patch. Core gives query jobs their own activation. A query
activation mixed with a patch notification is rejected bilaterally rather than
accepted as a new bridge convention.

## Intended live replay acceptance

`make test-temporal-workflow-patching` is the dedicated real-server acceptance
target. It first runs the Docker-free
`make test-temporal-workflow-patching-contract`, then uses real PostgreSQL and
Temporal Server containers with separate public OCaml processes: a long-lived
worker executes workflow code, while a one-shot client driver starts exact runs
and checks their terminal results.

The controller executes two source-replacement scenarios for the same stable
test patch ID:

| Scenario | Required observations | What the result establishes |
| --- | --- | --- |
| Legacy history | A legacy worker runs a definition with no `patched` call and reaches a durable timer. The normalized initial and terminal histories contain zero patch markers. A fresh patch-aware worker receives that exact run with `is_replaying=true`, takes the legacy activity branch, and the driver receives the fixed legacy result. | A history created before the change retains its legacy behavior after worker replacement. |
| New history | A patch-aware worker starts a new run whose normalized initial and terminal histories contain exactly one non-deprecated patch marker. A fresh patch-aware worker receives that exact run with `is_replaying=true`, takes the new activity branch, and the driver receives the fixed new result. | A history created with the marker retains the new behavior after worker replacement. |

The initial and terminal history checks are both intentional. The controller
uses the server's normalized history and branch-specific activity/result
oracles instead of inferring a decision from a worker log or from the locally
buffered completion commands. The replacement workers must have distinct
containers and fresh native runtime graphs; a restarted process or a client
that manufactures a result is not sufficient evidence.

The Docker-free contract checks the checked-in normalized histories,
replay-diagnostic and controller fixtures, that the schema documents are
readable JSON objects, the history normalizer, and representative malformed
cases. It does not run a JSON Schema validator against those fixtures, build
workers, start containers, contact Temporal Server, or establish that a replay
occurred.
There is no recorded successful invocation of the real-server target in this
document.

## Focused evidence and remaining boundary

Focused OCaml runtime tests cover new execution, replay with and without a
notification, repeated calls, execution isolation, and copying a mutable
source string before it enters durable state. Shared fixtures and Rust tests
cover strict JSON validation, duplicate command preservation, and round trips
through the pinned Core protobuf types. These are local semantic and bridge
evidence, not a substitute for the live acceptance scenario above.

This first slice proves only the non-deprecated patch-in primitive. There is no
public deprecation or patch-removal operation yet. Worker deployment/build-ID
versioning, side effects, arbitrary historical compatibility, and migration
tooling remain separate roadmap work. For the overall evidence boundary, read
[live acceptance coverage](live-acceptance-coverage.md).
