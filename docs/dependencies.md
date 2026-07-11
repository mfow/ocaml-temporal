# Dependency and License Inventory

All project and build dependencies are checked before a milestone commit. The
default `make license-check` reads `temporal.opam.locked`, asks OPAM for each
package's exact license metadata, and rejects missing or unapproved values.

## Policy

Accepted licenses are MIT, Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC, Zlib,
and PostgreSQL. An OCaml compiler/runtime package may use
`LGPL-2.1-or-later WITH OCaml-LGPL-linking-exception`. Other copyleft,
source-available, non-commercial, missing, and unknown terms are rejected.

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
| temporal | ~dev | Apache-2.0 | project | yes | yes | Project source and binary |
| dune | 3.24.0 | MIT | build | no | no | Build system only |
| ocaml | 5.2.1 | LGPL-2.1-or-later WITH OCaml-LGPL-linking-exception | compiler/runtime | yes | no | Approved OCaml linking exception |
| ocaml-base-compiler | 5.2.1 | LGPL-2.1-or-later WITH OCaml-LGPL-linking-exception | compiler | no | no | Approved OCaml linking exception |
| ocaml-config | 3 | ISC | build | no | no | Compiler configuration package |
| ocaml-options-vanilla | 1 | CC0-1.0+ | build | no | no | Exact reviewed permissive metadata exception |
| base-bigarray | base | compiler virtual package | runtime capability | no | no | No independent source; part of OCaml distribution |
| base-domains | base | compiler virtual package | runtime capability | no | no | No independent source; part of OCaml distribution |
| base-nnp | base | compiler virtual package | runtime capability | no | no | No independent source; part of OCaml distribution |
| base-threads | base | compiler virtual package | runtime capability | no | no | No independent source; part of OCaml distribution |
| base-unix | base | compiler virtual package | runtime capability | no | no | No independent source; part of OCaml distribution |

## Reviewed OCaml linking exceptions

There are currently no ecosystem packages in this table. Only the compiler
and runtime package names hard-coded by the policy are accepted. Adding a row
here also requires a matching exact-name checker change and documented review.

## Builder image tooling

The development image is based on `ocaml/opam:debian-12-ocaml-5.2` and uses
OPAM to install the locked closure. Operating-system and ambient base-image
tools are not linked into or redistributed with the future worker artifact.
Release containers will use a separate minimal runtime stage and will receive
their own package/SBOM audit before publication.

`ocamlformat` is deliberately absent. Version 0.28.1 is MIT licensed, but its
build closure includes ordinary GPL packages (`menhir` and `fix`), which this
project's all-dependencies policy prohibits. `make lint` and `make fmt`
currently enforce repository-owned whitespace rules instead.

## Planned Temporal Core dependency

Temporal Core is not yet part of the locked build. Phase 2 begins from
immutable upstream commit `95e97686a079dcfe6c42e3254b2f3f5e3d97408f`, whose
root license is MIT. No Core or Cargo package is approved for redistribution
until `Cargo.lock`, a complete transitive license inventory, native-library
review, and the executable license gate are committed. See
[ADR 0001](decisions/0001-temporal-core-c-boundary.md).
