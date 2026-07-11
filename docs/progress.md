# Progress

This document records verified implementation milestones. Planned work remains
in [the implementation roadmap](implementation-roadmap.md).

## 2026-07-11: Repository foundation

Status: verified.

The repository now has an Apache-2.0 package definition, a parameterized OCaml
5.2 through 5.5 development image, Docker Compose command runner, Dune
metadata, and a Makefile-first command contract.

Evidence:

- `make build` completed with Dune 3.24.0 in the compatibility image.
- `docker compose run --rm dev opam exec -- dune runtest test/smoke`
  passed 1 test with 0 failures.
- `make verify` completed successfully.
- `docker compose run --rm dev ocamlc -version` reported OCaml 5.2.1.
- `git diff --check` reported no whitespace errors.

The initial formatter experiment was removed during the dependency audit:
although `ocamlformat` itself is MIT licensed, its build closure contains GPL
tools. Repository-owned whitespace checks provide the current formatting gate
without adding prohibited dependencies.

## 2026-07-11: Executable dependency policy

Status: verified.

The locked project closure is intentionally small: OCaml 5.2.1, Dune 3.24.0,
compiler selection packages, and compiler virtual packages. `make
license-check` rejects missing, unknown, or prohibited licenses. It is kept
separate from the compiler build/test matrix; `make check` runs both locally.

Evidence:

- The policy fixture rejected GPL-3.0-only, missing metadata, an unreviewed
  OCaml linking exception, and a mixed MIT/GPL declaration.
- The same fixture accepted MIT.
- `make license-check` accepted every exact package in
  `temporal.opam.locked` and printed its decision.
- `make verify` completed the build, formatting gate, and test suite, and the
  separate `make license-check` audit completed successfully.
- `git diff --check` reported no whitespace errors.

Next phase: typed codecs and structured errors.

## 2026-07-11: Typed codecs and structured errors

Status: verified.

The first installable `temporal` library now provides typed payload codecs,
UTF-8 JSON string handling, byte and null encodings, abstract structured
errors, stable error views, and `result` binding syntax. Internal constructors
live in the explicitly unstable `temporal.internal_base` library.

Evidence:

- The initial focused test failed because the `temporal` library was absent.
- `make test-unit` passed codec, error, and repository tests on OCaml 5.2.1.
- Codec tests cover escaping, surrogate-pair decoding, invalid UTF-8, copied
  byte storage, encoding mismatch, and `None`/`Some` payload behavior.
- `make lint` and `make license-check` passed.
- The same unit and smoke suite passed with `OCAML_VERSION=5.5`.

Next phase: typed workflow and activity definitions.

## 2026-07-11: Typed workflow and activity definitions

Status: verified.

Local and remote workflows and activities now share an internal typed
definition representation while exposing separate abstract public types.
Definitions retain their input/output codecs and optional implementation;
public callers can inspect only the stable Temporal name.

Evidence:

- The initial focused test failed with unbound `Temporal.Activity` and
  `Temporal.Workflow` modules.
- `make verify` passed the full build, policy, and test gates on OCaml 5.2.1.
- The unit and smoke suites passed on the OCaml 5.5 Compose image.
- `dune build @install` and `opam lint temporal.opam` passed.
- Name tests cover local/remote definitions and reject empty or NUL-containing
  names during configuration.

Next phase: deterministic futures and effect scheduler.

## 2026-07-11: Deterministic futures and effect scheduler

Status: verified.

The runtime now has typed promises, a private OCaml 5 deep effect for
suspension, and a deterministic FIFO runnable queue. Public futures expose
`await`, `map`, `map_error`, `both`, `is_ready`, and `peek` without exposing
effect constructors or continuation values.

Scheduler invariants:

- Every scheduler and queued runnable receives a monotonic identity.
- Resolution is single-assignment; a second resolution raises
  `Invalid_argument` at the internal defect boundary.
- Waiters resume in registration order and resolution jobs enqueue in the
  caller-provided order.
- A continuation is captured only while its owning scheduler is running.
- `both` settles after both siblings and selects the left error first when both
  fail.
- Callback exceptions become scheduler failures rather than escaping the run
  loop.
- Shutdown discontinues captured continuations and drops queued work.

Evidence:

- The initial runtime test failed because `temporal.runtime` did not exist.
- `make test-runtime`, `make test-unit`, `make lint`, and `make license-check`
  passed on OCaml 5.2.1.
- The runtime, unit, and smoke suites passed on OCaml 5.5. The current compiler
  gate caught and removed one newly reserved identifier before commit.
- Tests cover FIFO resolution order, immediate waits, multiple waiters,
  double-resolution rejection, owner mismatch, mapping, mapped errors, pairing,
  sibling settlement after failure, callback defects, and shutdown disposal.
- A source scan found no `Obj.magic` or other `Obj` representation casts.
- `dune build @install` and `git diff --check` passed.

Next phase: synthetic activation interpreter and command API.

## 2026-07-11: Synthetic activation interpreter and command API

Status: verified.

The first end-to-end runtime slice schedules typed activities, decodes their
results, starts durable timers, resumes suspended OCaml code, and emits encoded
workflow completion commands. A domain-local context makes public operations
available only during activation execution. This is a synthetic proof and
does not yet poll Temporal Server.

Evidence:

- The initial focused test failed because the activation and execution modules
  did not exist.
- The schedule/activity-resolution/timer/completion sequence passed on OCaml
  5.2.1 and 5.5.
- Replaying identical job lists produced structurally identical payload bytes
  and command lists.
- Concurrent activity resolution tests proved that runnable order follows the
  explicit activation job order.
- Tests reject unknown and duplicate sequences as bridge defects, validate
  zero/negative durations, emit cancellation exactly once, and evict blocked
  executions without a command or leaked continuation warning.
- Terminal completion and failure tear down pending runtime state while
  retaining the terminal command.
- Full unit/runtime tests, lint, license audit, install build, OPAM lint,
  unsafe-cast scan, and `git diff --check` passed.

Next phase: Phase 1 documentation and clean-checkout handoff.

## 2026-07-11: Phase 1 deterministic runtime handoff

Status: verified.

Phase 1 establishes the typed public kernel, effect scheduler, and synthetic
activation proof needed before binding to Temporal Core. Milestone commits are:

| Task | Commit | Outcome |
|---|---|---|
| Architecture | `5e80c6a` | Approved OCaml-over-Core design |
| Plan | `6d6d8b8` | Foundation/runtime implementation plan |
| 1 | `855d6b2` | Docker, Make, Dune, and package foundation |
| 2 | `174ad92` | Executable dependency-license gate |
| Repository metadata | `d1f84af` | GitHub location and `master` publication |
| 3 | `5c70b93` | Typed codecs and structured errors |
| 4 | `f4a49eb` | Typed workflows and activities |
| 5 | `fc352d1` | Deterministic effect scheduler |
| 6 | `a0e157d` | Synthetic activation interpreter |

The clean matrix executed from the repository root on 2026-07-11:

```sh
make clean
make build
make test-unit
make test-runtime
make license-check
make lint
make verify
docker compose run --rm dev opam exec -- ocamlc -version
git diff --check
```

Every command exited zero, and the compatibility image reported OCaml 5.2.1.
The complete runtime/unit/smoke suite also passed on OCaml 5.5.0 during the Task
6 compatibility gate. OPAM lint, the install target, and an explicit unsafe
`Obj` cast scan passed before handoff.

Known limitations:

- There is no live Temporal Core or Server connection yet.
- Compose does not yet include Temporal Server, PostgreSQL, UI, or a
  cross-language activity worker; those arrive with the first real bridge
  vertical slice.
- The synthetic protocol currently covers activities, timers, cancellation,
  completion, failure, and eviction only.
- Child workflows, structured cancellation, signals, queries, updates,
  continue-as-new, versioning, local activities, Nexus, replay-safe side
  effects, and the remaining parity surface are still planned.
- The current formatting gate checks repository whitespace because the
  formatter closure violated the all-dependencies license policy.

Next objective: pin and audit the Rust/Cargo closure, link the project-owned
Core static bridge into an OCaml-built worker, and run the same direct-style
workflow against Temporal Server and PostgreSQL in Docker Compose.

## 2026-07-11: Cross-version and cross-architecture CI

Status: verified.

GitHub Actions now runs every supported OCaml minor release from 5.2 through
5.5 on native amd64 and arm64 GitHub-hosted runners. The dependency-license
audit is one independent job rather than repeated for each compiler and
architecture. Compose commands run with the checkout owner's UID/GID, avoiding
host/container bind-mount ownership failures, and `version-check` proves each
matrix cell built the requested compiler image.

Evidence:

- The official OPAM images for OCaml 5.2, 5.3, 5.4, and 5.5 advertise both
  `amd64` and `arm64` manifests.
- Local `make verify OCAML_VERSION=<version>` passed for all four versions.
- Local `make license-check OCAML_VERSION=5.2` passed independently.
- [GitHub Actions run 29139710646](https://github.com/mfow/ocaml-temporal/actions/runs/29139710646)
  completed all eight compiler/architecture cells and the license job
  successfully.
- [GitHub Actions run 29139792049](https://github.com/mfow/ocaml-temporal/actions/runs/29139792049)
  repeated all nine jobs successfully after updating to the current official
  `actions/checkout` major version.

End-to-end Temporal/PostgreSQL Compose tests will be a separate Phase 2 job.
Their architecture matrix will be enabled only after every runtime image is
verified to publish the corresponding native manifest.

## 2026-07-11: Pinned Rust and Temporal Core build foundation

Status: locally verified; native CI matrix pending.

The development image now copies Rust 1.94.1 from a digest-pinned official
multi-architecture image and installs only Core's protobuf build tools. The
Apache-2.0 project bridge builds as a 21 MiB native static archive while the
final process architecture remains OCaml-owned. Temporal Core is a direct
Cargo dependency pinned to immutable commit
`95e97686a079dcfe6c42e3254b2f3f5e3d97408f`, with defaults disabled and the
`tls-ring` feature selected.

Local evidence:

- The toolchain smoke test first failed with no `rustc`, then passed with the
  pinned Rust 1.94.1 compiler, locked Cargo graph, and non-empty static archive.
- `cargo metadata --locked --offline` resolved 320 packages including the
  project bridge, and the fail-closed SPDX policy accepted the complete graph.
- Policy fixtures accepted compound permissive expressions and rejected GPL,
  LGPL, AGPL, MPL, missing, unknown, and malformed license metadata.
- The production Rust source is separate from its integration test under
  `rust/core-bridge/tests/`; the revision test passes.
- Action workflow lint, Rust format checking, repository formatting, Python
  syntax checking, and `git diff --check` pass.

The Cargo scanner is intentionally absent from the Makefile. The single
standalone GitHub Actions license job streams locked metadata from the build
container to a network-disabled, read-only, digest-pinned official Python
container. Every OCaml/compiler architecture cell runs the Rust build, Clippy,
and Rust tests through `make verify`; GitHub Actions is the compatibility gate
for OCaml 5.3 through 5.5 and native amd64/arm64.
