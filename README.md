# OCaml Temporal

[![Build](https://github.com/mfow/ocaml-temporal/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/mfow/ocaml-temporal/actions/workflows/build.yml)

OCaml Temporal is a pre-release SDK for writing durable Temporal workflows in
modern OCaml 5. Workflow code uses ordinary functions, explicit `result`
values, typed codecs and futures, and private algebraic effects for
direct-style suspension.

The repository currently contains a verified deterministic runtime kernel and
synthetic activation interpreter. It does **not yet connect to Temporal
Server**. The next milestone links a native OCaml worker executable to the
official Rust Temporal Core SDK.

## Current capabilities

- Typed local and remote workflow/activity definitions
- Explicit payload codecs and structured errors
- FIFO deterministic workflow fibers built on OCaml 5 deep effects
- Concurrent activity scheduling with `Future.both`
- Durable timer command generation with `Workflow.sleep`
- Deterministic synthetic replay, cancellation, and cache eviction tests
- Docker Compose development on OCaml 5.2 and current OCaml 5.5
- Executable no-copyleft dependency policy

Child workflows, live Temporal connectivity, signals, queries, updates,
continue-as-new, versioning, local activities, Nexus, and the remaining parity
surface are planned and tracked in the roadmap.

## Quick start

Requirements are Docker with Compose v2 and Make. No host OCaml installation
is required.

```sh
make build
make test-unit
make test-runtime
make verify
make license-check
```

All commands run through Docker Compose. `make license-check` is the separate
dependency audit, and `make clean` removes Compose services and Dune build
output. The default development image uses OCaml 5.2; use, for example,
`make verify OCAML_VERSION=5.5` to run the same gate on another supported
version. GitHub Actions builds and tests OCaml 5.2, 5.3, 5.4, and 5.5 on native
amd64 and arm64 runners for every pull request and every push to `master`. Its
dependency-license audit runs once in a separate job.

## Workflow style

```ocaml
let summarize =
  Temporal.Activity.remote
    ~name:"summarize"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string

let workflow document =
  let open Temporal.Result_syntax in
  let summary = Temporal.Activity.start summarize document in
  let* summary = Temporal.Future.await summary in
  let* () = Temporal.Workflow.sleep (Temporal.Duration.of_ms 10L) in
  Ok summary

let definition =
  Temporal.Workflow.define
    ~name:"summarize_document"
    ~input:Temporal.Codec.string
    ~output:Temporal.Codec.string
    workflow
```

The API above compiles and is exercised by the synthetic interpreter. It is
not a claim that a production worker can connect yet.

## Documentation

- [Architecture](docs/superpowers/specs/2026-07-11-ocaml-temporal-sdk-design.md)
- [Implementation roadmap](docs/implementation-roadmap.md)
- [Workflow guide](docs/guides/workflows.md)
- [Runtime invariants](docs/reference/runtime-invariants.md)
- [Temporal Core boundary decision](docs/decisions/0001-temporal-core-c-boundary.md)
- [Dependency and license inventory](docs/dependencies.md)
- [Verified progress](docs/progress.md)

## Status and compatibility

The project is not yet released and its API may change before `0.1.0`. The
compatibility floor is OCaml 5.2; the test matrix also exercises OCaml 5.5.
Temporal Core will be pinned by immutable commit and upgraded only with replay
and cross-language compatibility evidence.

## License

Project source is licensed under [Apache-2.0](LICENSE). Dependencies must pass
the repository's permissive-license policy; ordinary GPL, AGPL, LGPL, and
other copyleft or source-available dependencies are prohibited. The only
standing exception is the narrowly reviewed OCaml linking exception documented
in the dependency inventory.
