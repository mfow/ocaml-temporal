# Release preflight

The package is still experimental and uses `~dev` as its checked-in
development version. The one-line `.release-version` file is the single
source-of-truth for that value; `temporal-sdk.opam` and its locked manifest
must contain the same package name and version. A future release preparation
change will replace `~dev` with a concrete version and add a tag only after the
release evidence is complete.

## Local gate

Run `make release-preflight` from a clean checkout. This deliberately performs
metadata and source checks only, so it does not require Docker, OCaml tooling,
or a Rust build. It verifies:

- the working tree has no staged, unstaged, or untracked files;
- the opam manifests, Dune project, README, license, and pinned Temporal Core
  revision agree on identity, ownership, licensing, and release metadata; and
- generated build trees are not tracked and the sorted Git source manifest can
  be fingerprinted reproducibly.

The clean-tree requirement is intentional: a preflight result must describe
the exact inputs that would be archived or built, not a mixture of committed
files and local output.

## CI SBOM

`.github/workflows/release-preflight.yml` runs the same gate on pull requests,
pushes to `master`, and manual dispatches. It obtains the locked Cargo graph
with `cargo metadata --locked`, then invokes the project-owned standard-library
SBOM generator inside the pinned official Python image with network access
disabled. The generated SPDX 2.3 document is deterministic: package IDs are
derived from Cargo IDs, package order is stable, and its creation timestamp is
fixed. A second isolated invocation validates the document before the job
finishes. The SBOM is a CI artifact/input check and is not committed to the
repository.

This workflow does not publish packages, create tags, or claim that a release
is ready. Those actions require a later, explicitly reviewed release process
including live acceptance, replay evidence, API compatibility review, and
artifact provenance.
