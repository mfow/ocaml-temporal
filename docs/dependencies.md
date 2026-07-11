# Dependency and License Inventory

All project and build dependencies are checked before a milestone commit.
`make license-check` reads `temporal-sdk.opam.locked`, asks OPAM for each package's
exact license metadata, and rejects missing or unapproved values. The
standalone GitHub Actions license job streams `cargo metadata --locked` into
the repository scanner running in a separate official Python container. Cargo
license scanning deliberately does not run in the compiler/architecture
matrix or from the Makefile.

## Policy

Accepted licenses are MIT, Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC, Zlib,
and PostgreSQL. An OCaml compiler/runtime package may use
`LGPL-2.1-or-later WITH OCaml-LGPL-linking-exception`. Other copyleft,
source-available, non-commercial, missing, and unknown terms are rejected.

`ocamlbuild.0.16.1` is an exact build-only exception for
`LGPL-2.0-or-later WITH OCaml-LGPL-linking-exception`. It enters the locked
closure solely because `logs.0.10.0` uses it to build; neither ocamlbuild nor
its code is linked into or redistributed with the SDK. Other ocamlbuild
versions and ordinary LGPL packages remain prohibited.

`ocaml-options-vanilla.1` is an exact reviewed exception for OPAM's historical
`CC0-1.0+` metadata. CC0 is permissive; the exception is package- and
version-specific rather than a general substring match.

The `base-*` entries below are virtual packages shipped by the OCaml compiler
distribution. Their OPAM records contain no independent source or license, so
the checker recognizes only these exact package names at version `base` and
attributes them to the reviewed compiler distribution.

## Locked OCaml closure

| Package | Exact version | License | Scope | Linked into release | Redistributed | Review note |
|---|---:|---|---|---|---|---|
| temporal-sdk | ~dev | Apache-2.0 | project | yes | yes | Project source and binary |
| dune | 3.24.0 | MIT | build | no | no | Build system only |
| logs | 0.10.0 | ISC | runtime | yes | no | Maintained application-configurable logging infrastructure; the SDK installs no reporter |
| ocamlbuild | 0.16.1 | LGPL-2.0-or-later WITH OCaml-LGPL-linking-exception | build | no | no | Exact reviewed build-only linking-exception dependency of `logs` |
| ocamlfind | 1.9.8 | MIT | build | no | no | Build-time library discovery required by `logs` |
| topkg | 1.1.1 | ISC | build | no | no | Build-time packaging tool required by `logs` |
| yojson | 3.0.0 | BSD-3-Clause | runtime | yes | no | Implements the optional cross-language `json/plain` codec; Temporal itself does not require JSON |
| ocaml | 5.2.1 | LGPL-2.1-or-later WITH OCaml-LGPL-linking-exception | compiler/runtime | yes | no | Approved OCaml linking exception |
| ocaml-base-compiler | 5.2.1 | LGPL-2.1-or-later WITH OCaml-LGPL-linking-exception | compiler | no | no | Approved OCaml linking exception |
| ocaml-config | 3 | ISC | build | no | no | Compiler configuration package |
| ocaml-options-vanilla | 1 | CC0-1.0+ | build | no | no | Exact reviewed permissive metadata exception |
| base-bigarray | base | compiler virtual package | runtime capability | no | no | No independent source; part of OCaml distribution |
| base-domains | base | compiler virtual package | runtime capability | no | no | No independent source; part of OCaml distribution |
| base-nnp | base | compiler virtual package | runtime capability | no | no | No independent source; part of OCaml distribution |
| base-threads | base | compiler virtual package | runtime capability | no | no | No independent source; part of OCaml distribution |
| base-unix | base | compiler virtual package | runtime capability | no | no | No independent source; part of OCaml distribution |

Protocol conformance tests use a small repository-owned standard-library
harness. Alcotest itself is ISC, but its complete OPAM test closure includes
ordinary LGPL packages outside the compiler/runtime exception allowed by this
project, so it is intentionally neither used nor declared as a package test
dependency.

## Reviewed OCaml linking exceptions

| Package | Exact version | License | Scope | Rationale |
|---|---:|---|---|---|
| ocamlbuild | 0.16.1 | LGPL-2.0-or-later WITH OCaml-LGPL-linking-exception | build only | Required to build `logs.0.10.0`; not linked or redistributed |

Only the exact compiler/runtime names and the exact ocamlbuild version
hard-coded by the policy are accepted. Adding a row here also requires a
matching exact-name and version checker change.

## Builder image tooling

The development image is based on `ocaml/opam:debian-12-ocaml-5.2` and uses
OPAM to install the locked closure. Operating-system and ambient base-image
tools are not linked into or redistributed with the future worker artifact.
Release containers will use a separate minimal runtime stage and will receive
their own package/SBOM audit before publication.

The image copies Rust 1.94.1, Cargo, Clippy, and rustfmt from the official
multi-architecture `rust:1.94-bookworm` image at manifest digest
`sha256:6ae102bdbf528294bc79ad6e1fae682f6f7c2a6e6621506ba959f9685b308a55`.
That manifest contains native `linux/amd64` and `linux/arm64/v8` images. Rust
is dual-licensed Apache-2.0 OR MIT. Debian's `protobuf-compiler` and
`libprotobuf-dev` packages are installed as build-only tools required by
Temporal Core's generated protobuf crates and standard Google protobuf
definitions; Protocol Buffers is BSD-3-Clause. Neither tool is intended for
the eventual minimal runtime image. The Cargo license policy script runs in a
separate official `python:3.14-slim-bookworm` image pinned at manifest digest
`sha256:4ff4b92a68355dbdb52584ab3391dff8d371a61d4e063468bfd0130e3189c6d9`
in the standalone GitHub Actions audit job. Its scanner container has no
network access and mounts the source read-only. Python is not installed in the
development or eventual runtime image.

`ocamlformat` is deliberately absent. Version 0.28.1 is MIT licensed, but its
build closure includes ordinary GPL packages (`menhir` and `fix`), which this
project's all-dependencies policy prohibits. `make lint` and `make fmt`
currently enforce repository-owned whitespace rules instead.

## Locked Cargo closure

`rust/Cargo.lock` locks 319 dependencies rooted at Temporal Core commit
`95e97686a079dcfe6c42e3254b2f3f5e3d97408f`; metadata contains 320 packages
including the project bridge itself. The Core dependency disables default
features and currently enables only `tls-ring`. The project bridge is
Apache-2.0 and emits a native `staticlib` for OCaml plus an internal `rlib` for
Rust integration tests.

The bridge declares `serde` 1.0.228 (MIT OR Apache-2.0), `serde_json` 1.0.150
(MIT OR Apache-2.0), and `base64` 0.22.1 (MIT OR Apache-2.0) directly for its
private control protocol. All three were already present at those exact
versions in the locked Temporal Core closure, so this changes package ownership
metadata but adds no package to the 319-dependency graph.

Dependabot checks the Cargo workspace under `/rust` every Monday and targets
updates at `master`. OCaml and OPAM are intentionally absent because GitHub
Dependabot does not support that ecosystem; the locked OPAM closure continues
to be reviewed and updated manually.

The Cargo scanner parses SPDX `AND`, `OR`, `WITH`, and parentheses rather than
matching substrings. For an `OR`, it prints the exact approved branch selected;
every `AND` branch must be approved. It also understands Cargo's historical
slash-as-OR spelling. GPL, LGPL, AGPL, MPL, missing, malformed, and unknown
licenses fail policy fixtures. Approved permissive identifiers found in the
closure include MIT, Apache-2.0, BSD, ISC, Zlib, Unicode-3.0, 0BSD, MIT-0,
CC0-1.0, Unlicense, and CDLA-Permissive-2.0. The Apache LLVM exception is an
exact approved exception.

Six first-party packages inherit the upstream workspace `LICENSE.txt` rather
than publishing a Cargo `license` expression: `temporalio-client`,
`temporalio-common`, `temporalio-common-wasm`, `temporalio-macros`,
`temporalio-protos`, and `temporalio-sdk-core`. The scanner permits this only
for those exact package names, the immutable Core git revision, and a file
named `LICENSE.txt`; the reviewed upstream license is MIT. See
[ADR 0001](decisions/0001-temporal-core-c-boundary.md).
