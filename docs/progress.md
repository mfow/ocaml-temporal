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
