# Deterministic workflow time

`Temporal.Workflow.now ()` returns the timestamp that Temporal attached to the
activation currently executing a workflow. It is an exact pair of signed Unix
seconds and a normalized nanosecond fraction:

```ocaml
match Temporal.Workflow.now () with
| Ok instant ->
    Printf.printf "%Ld.%09d\n"
      (Temporal.Time.seconds instant)
      (Temporal.Time.nanoseconds instant)
| Error error ->
    (* Handle a typed SDK defect, for example when called outside a workflow. *)
    Logs.err (fun log -> log "workflow clock unavailable: %s"
      (Temporal.Error.message error))
```

The value is supplied by the activation protocol and is installed before the
workflow function runs. Live execution and replay therefore observe the same
clock value for the same history event. The OCaml implementation does not call
`Unix.time`, inspect the host timezone, or use floating-point conversion.

`Temporal.Time.of_unix` can validate an application-provided timestamp when a
helper needs to compare or store one. Its nanosecond component must be in the
range `0` through `999_999_999`; invalid values return a non-retryable
`Temporal.Error.t`. `Temporal.Time.compare` and `Temporal.Time.equal` compare
the integer representation exactly.

Outside workflow execution, or for a synthetic activation that has no Temporal
timestamp (such as cache eviction), `Temporal.Workflow.now ()` returns a typed
defect. It never silently falls back to local wall-clock time, because doing so
would make replay behavior depend on the worker host.
