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
