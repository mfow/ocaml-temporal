# JSON Control Protocol Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a strict, bounded, cross-language JSON envelope and opaque-payload codec for later Temporal worker messages.

**Architecture:** A private OCaml protocol library and a Rust `protocol` module implement the same closed envelope, strict JSON parser, semantic checks, canonical serializer, and structured errors. Shared fixtures are the conformance source, while Draft 2020-12 schemas and a plain-language reference document describe the contract independently.

**Tech Stack:** OCaml 5.2+, Yojson 3.0, Rust 1.94+, serde 1, serde_json 1, base64 0.22, Dune, Cargo.

## Global Constraints

- Base branch is `master`; integrate updates with merge commits, never rebase.
- Rust alone owns Temporal/Core protobuf; no protobuf dependency crosses into OCaml.
- The bridge compatibility number is checked once at startup, never per message.
- All input paths use explicit finite document, nesting, string, collection, node, and payload limits.
- Every new function and data structure receives contract-focused documentation.
- Tests and implementation remain in separate files.

---

### Task 1: Shared contract fixtures and schemas

**Files:** Create `test/bridge/fixtures/protocol/`, `docs/schemas/bridge/`, and `docs/reference/core-protocol.md`.

**Interfaces:**
- Produces: shared input and normalized/error fixtures consumed by both test suites.
- Produces: Draft 2020-12 schemas for envelope and opaque payload values.

- [x] Write valid request, response, error, and payload fixtures plus invalid duplicate, missing, unknown, wrong-type, base64, correlation, compatibility, oversized, and deep cases.
- [x] Add the closed schemas and plain-language protocol reference with exact limits and privacy rules.
- [x] Run `git diff --check` and confirm fixture JSON intended to be valid parses with Yojson/serde tooling, leaving raw duplicate fixtures as intentional parser-negative documents.

### Task 2: OCaml strict protocol implementation

**Files:** Create `lib/protocol/dune`, `lib/protocol/control_protocol.ml`, `lib/protocol/control_protocol.mli`, and `test/bridge/test_ocaml_protocol.ml`; modify `test/bridge/dune`.

**Interfaces:**
- Produces: `decode`, `encode`, `decode_payload`, `encode_payload`, `check_compatibility`, `error_view`, and immutable protocol value types.
- Consumes: shared fixture manifests and documented limits.

- [x] Add OCaml conformance tests first and run `DUNE_ROOT=. opam exec -- dune runtest test/bridge` to observe the missing-module failure.
- [x] Implement preflight depth/string scanning, strict Yojson tree validation, duplicate/unknown-field rejection, semantic envelope validation, canonical serialization, and base64 payload handling.
- [x] Run the focused bridge test until the complete OCaml fixture suite passes.

### Task 3: Rust strict protocol implementation

**Files:** Create `rust/core-bridge/src/protocol.rs` and `rust/core-bridge/tests/protocol.rs`; modify `rust/core-bridge/src/lib.rs`, `rust/Cargo.toml`, and `rust/core-bridge/Cargo.toml`.

**Interfaces:**
- Produces: `decode`, `encode`, `decode_payload`, `encode_payload`, `check_compatibility`, typed envelopes, and owned protocol errors.
- Consumes: shared fixture manifests and documented limits.

- [x] Add Rust conformance tests first and run `cargo test --manifest-path rust/Cargo.toml --test protocol` to observe the missing-module failure.
- [x] Add direct serde, serde_json, and base64 declarations at already-locked permissive versions.
- [x] Implement a custom serde visitor that retains object-member order long enough to reject duplicates, then apply the same structural and semantic limits as OCaml.
- [x] Run the focused Rust protocol test until all shared fixtures and outgoing self-validation cases pass.

### Task 4: Repository integration and verification

**Files:** Modify `README.md`, `docs/dependencies.md` only if the locked graph changes, `docs/progress.md`, and `docs/superpowers/plans/2026-07-11-core-bridge-and-first-real-workflow.md`.

**Interfaces:**
- Produces: accurate progress evidence and roadmap state.

- [x] Confirm both languages normalize every valid fixture identically and reject every invalid fixture.
- [x] Run focused OCaml and Rust protocol tests, `DUNE_ROOT=. make native-verify NATIVE_OCAML_VERSION=5.4 NATIVE_RUST_VERSION=1.96.0 NATIVE_ARCH=arm64 NATIVE_RUST_HOST=aarch64-apple-darwin`, license checks when required, and `git diff --check`.
- [ ] Review the diff against every requested deliverable, commit, fetch origin/master over HTTPS, merge it, rerun verification, and push.
- [ ] Open a PR against `master`, apply available documentation/enhancement/quality/test labels, monitor the complete matrix including Windows, fix failures with new tests, and merge with a merge commit only after fresh green checks.
