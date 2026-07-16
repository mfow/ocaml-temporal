# Deterministic workflow time

`Temporal.Workflow.now ()` returns the timestamp that Temporal attached to the
activation currently executing a workflow. It is an exact pair of signed Unix
seconds and a normalized nanosecond fraction:

```ocaml
let workflow_timestamp () =
  match Temporal.Workflow.now () with
  | Ok instant ->
      Ok (Printf.sprintf "%Ld.%09d"
        (Temporal.Time.seconds instant)
        (Temporal.Time.nanoseconds instant))
  | Error error ->
      (* Return the typed defect to the caller; report it outside the workflow. *)
      Error error
```

Formatting a value into a string is deterministic, but printing or logging is
I/O. Keep that reporting outside workflow execution: have the workflow return
the formatted value (or its typed error), then let the worker or application
boundary decide how to display it. This keeps the example compatible with
replay as well as live execution.

The value is supplied by the activation protocol and is installed before the
workflow function runs. Live execution and replay therefore observe the same
clock value for the same history event. The OCaml implementation does not call
`Unix.time`, inspect the host timezone, or use floating-point conversion.

`Temporal.Time.of_unix` can validate an application-provided timestamp when a
helper needs to compare or store one. Its nanosecond component must be in the
range `0` through `999_999_999`; invalid values return a typed non-retryable
`Temporal.Error.t` with category `` `Defect `` rather than raising.
`Temporal.Time.compare` and `Temporal.Time.equal` compare the integer
representation exactly.

Outside workflow execution, or for a synthetic activation that has no Temporal
timestamp (such as cache eviction), `Temporal.Workflow.now ()` returns a typed
defect. It never silently falls back to local wall-clock time, because doing so
would make replay behavior depend on the worker host.

## Deterministic pseudo-random values

`Temporal.Workflow.random_int ~bound` draws an integer in `[0, bound)` from a
workflow-local stream seeded by Temporal when the run is initialized:

```ocaml
let choose_model models =
  match Temporal.Workflow.random_int ~bound:(List.length models) with
  | Ok index -> Ok (List.nth models index)
  | Error error -> Error error
```

The seed and generator state belong to one execution context. Replaying the
same history therefore produces the same sequence, while two workflow runs do
not share mutable random state. The implementation uses an integer-only
xorshift generator and rejection sampling; it never calls `Random`, reads host
entropy, or consults the wall clock. `bound` must be positive. The function
returns a typed defect for an invalid bound or when called outside workflow
execution.

Use this API only when a pseudo-random choice is part of deterministic workflow
logic. It does not make external randomness replay-safe: secrets, cryptographic
nonces, and entropy-dependent values belong in an activity whose result is
recorded by Temporal.
