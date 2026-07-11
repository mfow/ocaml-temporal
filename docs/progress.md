# Progress

This document records verified implementation milestones. Planned work remains
in [the implementation roadmap](implementation-roadmap.md).

## 2026-07-11: Repository foundation

Status: verified.

The repository now has an Apache-2.0 package definition, OCaml 5.2 and 5.5
development images, Docker Compose command runner, Dune metadata, and a
Makefile-first command contract.

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
license-check` rejects missing, unknown, or prohibited licenses and is part of
`make verify`.

Evidence:

- The policy fixture rejected GPL-3.0-only, missing metadata, an unreviewed
  OCaml linking exception, and a mixed MIT/GPL declaration.
- The same fixture accepted MIT.
- `make license-check` accepted every exact package in
  `temporal.opam.locked` and printed its decision.
- `make verify` completed the build, formatting gate, license audit, and test
  suite successfully.
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
- The same unit and smoke suite passed with the Compose `dev-current` image on
  OCaml 5.5.

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
