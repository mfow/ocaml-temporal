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

The first image build also verified the OPAM 2.1 compatibility path: project
and test dependencies use `--with-test`, while the development-only formatter
is installed explicitly because OPAM 2.1 does not expose
`--with-dev-setup`.

Next phase: typed public kernel.
