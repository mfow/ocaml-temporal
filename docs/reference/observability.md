# OCaml SDK Logging

The OCaml SDK uses the [`logs`](https://ocaml.org/p/logs/latest) library for
application-configurable logging. The SDK creates sources and submits events,
but never installs a reporter or changes reporting levels. An application owns
those process-wide choices.

## Sources

Source names are stable filtering identifiers:

| Source | Purpose |
|---|---|
| `temporal.sdk.lifecycle` | Native SDK runtime initialization and shutdown |
| `temporal.sdk.bridge` | Calls through the private OCaml/C/Rust bridge |
| `temporal.sdk.workflow` | Workflow execution and activation processing |

Current events use these levels:

- `Debug` records verbose bridge calls, activation processing, counts, and
  latency.
- `Info` records important runtime and workflow lifecycle transitions.
- `Warning` records recoverable abnormal conditions, such as work delivered to
  an execution after cache eviction or a worker shutdown that still has leased
  tasks to finish.
- `Error` records bridge and workflow failures.

An empty non-blocking worker poll lane and an exact-run client wait whose
100 ms interval elapsed are reported at `Debug` as `not ready`; both are
expected scheduling states and are not failures. Protocol, lifecycle,
configuration, and native bridge failures remain `Error` records. This level
split keeps a healthy worker or waiting client from producing error-volume
logs while retaining actionable diagnostics for conditions that require
intervention.

The SDK never logs at `App`, which is reserved for the application.

## Tags

Reporters receive typed structural tags independently from message prose:

| Tag | Type | Meaning |
|---|---|---|
| `temporal.operation` | string | Stable operation identifier |
| `temporal.duration_ms` | float | Finite non-negative elapsed milliseconds |
| `temporal.workflow_type` | string | Registered workflow type |
| `temporal.job_count` | int | Jobs supplied in one activation |
| `temporal.command_count` | int | Commands emitted by one activation |
| `temporal.bridge_status` | string | Stable lowercase bridge status |
| `temporal.error_kind` | string | Stable lowercase Temporal error category |

Latency is measured around the local OCaml operation with the portable Unix
wall clock, expressed as fractional milliseconds, and clamped to zero if the
clock moves backwards. It is diagnostic metadata only: workflow code never
uses it to choose commands or results. Future modules should reuse the source
and tag definitions in `Temporal_base.Observability` instead of inventing
near-duplicate names.

The shared tag constructor normalizes negative counts to zero. Negative,
`NaN`, and infinite durations also become zero. This defensive boundary keeps
reporter and metrics backends free from impossible values if a future internal
caller supplies malformed metadata; valid numeric values are preserved.

## Application setup and filtering

The default `logs` reporter discards records and new sources inherit the
process's current default level. A small application setup can use the base
formatter reporter and then make bridge detail more verbose:

```ocaml
let () =
  Logs.set_reporter (Logs.format_reporter ());
  Logs.set_level (Some Logs.Info);
  Logs.Src.list ()
  |> List.find_opt (fun source ->
         Logs.Src.name source = "temporal.sdk.bridge")
  |> Option.iter (fun source -> Logs.Src.set_level source (Some Logs.Debug))
```

Applications using reporters from multiple Domains must also configure the
reporter synchronization appropriate to their runtime, for example with
`Logs.set_reporter_mutex`. The SDK does not select that policy or add an
optional reporter package on the application's behalf.

## Privacy and failure isolation

Events contain operation names, counts, type names, stable error categories,
and latency. They do not contain payload bytes, workflow inputs or outputs,
credentials, Rust diagnostic strings, or arbitrary remote failure detail.
User-controlled string tags are capped at 256 bytes, and current message prose
is constant and bounded.

Every SDK report passes through one exception shield. If an application
reporter or formatter raises, the SDK discards that record and returns the
same `result`, commands, or exception it would have produced without logging.
Logging therefore adds diagnostics without changing the public typed-error
model or deterministic command decisions.

Workflow-runtime reports also run with the Domain-local workflow context
temporarily masked. A reporter that calls a workflow API re-entrantly therefore
receives the normal outside-workflow behavior and cannot append deterministic
commands to the activation being reported.
