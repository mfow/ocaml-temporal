# Workflow patching

`Temporal.Workflow.patched` is the first workflow-versioning primitive in the
public OCaml API. It lets a workflow add a new deterministic branch without
making older histories replay that branch.

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

For a newly started execution, the first call returns `true` and records a
non-deprecated Temporal patch marker. During replay, it returns `true` only
when Temporal Core reports that the marker exists in history; replay of an
older history without the marker returns `false`. The decision is retained by
that workflow execution, so helper functions can call `patched` without
threading version state through their arguments.

Every call emits Core's idempotent `SetPatchMarker` command, including repeated
calls with the same ID. This matches the Temporal Core language-SDK contract;
the OCaml runtime must not deduplicate those commands itself. Core remains the
owner of durable marker state and history-machine deduplication.

Patch IDs are durable history keys. They must be non-empty, valid UTF-8,
NUL-free, and no more than 65,536 bytes. Invalid IDs and calls outside workflow
execution raise `Invalid_argument` because they are programmer defects. Never
reuse an ID for a different change, derive it from mutable configuration, or
build it from nondeterministic data.

## Replay sequence

The bridge preserves this order for each workflow task:

1. Rust converts Core's `NotifyHasPatch` job to the closed JSON protocol.
2. OCaml validates the complete activation and installs `is_replaying`.
3. The execution applies all patch notifications before it runs workflow
   fibers.
4. `Temporal.Workflow.patched` selects the execution-local decision and emits
   `SetPatchMarker { deprecated = false }`.
5. Rust validates the completion and converts the marker back to Core's
   protobuf command.

Core gives query jobs their own activation. A query activation mixed with a
patch notification is rejected bilaterally rather than accepted as a new
bridge convention.

## Evidence and remaining boundary

Focused OCaml runtime tests cover new execution, replay with and without a
notification, repeated calls, execution isolation, and copying a mutable
source string before it enters durable state. Shared fixtures and Rust tests
cover strict JSON validation, duplicate command preservation, and round trips
through the pinned Core protobuf types.

This first slice proves only the non-deprecated patch-in primitive. Live
Temporal Server replay of both an older history without the marker and a newer
history with the marker remains pending. There is no public deprecation or
patch-removal operation yet. Worker deployment/build-ID versioning, side
effects, arbitrary historical compatibility, and migration tooling remain
separate roadmap work.
