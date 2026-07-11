# Quality and security gates

The normal `make verify` path already treats OCaml compiler warnings, Rust
Clippy warnings, rustfmt drift, repository formatting errors, and test failures
as errors. The independent quality gate adds three checks that find different
defect classes without repeating that work in every compiler and architecture
job.

## Running the gate

Install these exact native binaries on the development host:

```text
cargo-deny 0.20.2
cargo-machete 0.9.2
typos 1.48.0
```

Then run:

```sh
make quality
```

`make quality-tool-version-check` fails before scanning when any executable is
missing or has a different version. The subordinate `make quality-rust` and
`make quality-spelling` targets are useful for focused local diagnosis but
retain the same version gate. These checks are native because their upstream
projects publish small binaries for supported development platforms; they are
not SDK dependencies and are intentionally absent from the Compose builder.

GitHub Actions uses
[`taiki-e/install-action`](https://github.com/taiki-e/install-action) at an
immutable commit with checksum validation enabled and fallback installation
disabled. The action installs the same exact versions and then invokes `make
quality`. This runs once on Ubuntu for each pull request and push to `master`;
the OCaml version matrix and Windows/macOS native compatibility jobs are
unchanged.

## Rust dependency advisories and sources

[`cargo-deny`](https://github.com/EmbarkStudios/cargo-deny) checks the complete
locked, all-feature Cargo graph against the current RustSec advisory database.
Security advisories fail by default. Unmaintained transitive crates do not
fail the gate because this project cannot replace dependencies inside the
immutable Temporal Core graph; an unmaintained direct workspace dependency
does fail. Unsound advisories fail when they affect a direct workspace
dependency.

The source policy in `deny.toml` admits crates.io and the exact Temporal Core
repository only. Every Git dependency must use a `rev` specification, so a
new dependency cannot follow a mutable branch or tag. The Cargo manifest and
lockfile continue to pin Temporal Core's precise commit; the source gate is an
additional structural safeguard rather than a replacement for that invariant.

Cargo-deny's licence mode is not run. The repository-owned scanner understands
the six project-reviewed Temporal workspace packages and is intentionally the
only Cargo licence gate.

## Unused Rust dependencies

[`cargo-machete`](https://github.com/bnjbvr/cargo-machete) scans workspace
manifests and source references for direct dependencies that no longer appear
to be used. The gate enables Cargo metadata to handle renamed dependencies and
workspace inheritance accurately. The tool is deliberately approximate, so
any future false-positive exception must name the dependency and explain the
generated-code or build-time use in Cargo metadata; blanket ignores are not
acceptable.

## Cross-language spelling

[`typos`](https://github.com/crate-ci/typos) scans OCaml, Rust, C, shell,
Markdown, JSON, YAML, and TOML together. This catches mistakes in the extensive
API and ownership documentation that compiler-only checks cannot see. Add a
narrow dictionary exception only for an intentional project term or external
proper name; do not suppress a file or directory merely to silence a genuine
documentation defect.

## Evaluated OCaml alternatives

No separate OCaml semantic linter was added. The maintained compiler and Dune
checks already cover type errors, exhaustiveness, unused declarations, and
configured warnings. The OCaml Platform's
[`odoc`](https://github.com/ocaml/odoc) would add valuable documentation-link
validation, but version 3.2.1 has an ordinary `tyxml` dependency licensed under
LGPL with the OCaml linking exception. That dependency is outside this
repository's exact approved exceptions, so adding odoc would violate policy.
The decision can be revisited if a future permissive dependency closure is
available.
