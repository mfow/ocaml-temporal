# Core Bridge and First Real Workflow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:executing-plans` to implement this plan task-by-task.

**Goal:** Produce a Dune-built OCaml worker executable that links a
project-owned Rust static library, polls the official Temporal Core SDK, and
completes a real typed OCaml workflow against Temporal Server and PostgreSQL
in Docker Compose.

**Architecture:** Rust owns Temporal Core, Tokio, networking, and opaque
worker handles. A versioned blocking C ABI transfers owned byte buffers; OCaml
C stubs release the runtime lock during blocking calls. OCaml owns workflow
definitions, effect continuations, deterministic scheduling, and activation
interpretation. The bridge transfers raw Core activation/completion protobuf
bytes. A small project-owned protobuf wire module handles the supported schema
surface and is checked against exact Core-generated fixtures, avoiding a
copyleft OCaml protobuf runtime.

**Tech stack:** OCaml 5.2, 5.3, 5.4, and 5.5 on amd64 and arm64, Dune,
Rust 1.94, Temporal Core commit
`95e97686a079dcfe6c42e3254b2f3f5e3d97408f`, C ABI, Docker Compose,
Temporal Server, PostgreSQL, and the Temporal CLI.

## Global constraints

- Work and release only from `master`.
- The final worker process is linked and launched as an OCaml executable; Rust
  is an implementation-only static library.
- Rust/Tokio threads never call arbitrary OCaml functions.
- Every blocking C stub releases the OCaml runtime lock and reacquires it
  before allocating or inspecting OCaml values.
- Every ABI function is panic-contained and returns an explicit status/error.
- Buffer allocation and freeing happen on the same side of the ABI.
- Core and every Cargo dependency are locked; unknown or prohibited licenses
  fail the standalone GitHub Actions license job.
- Expected workflow failures remain `result` values. Exceptions are caught
  only at the worker defect boundary.
- Core protobuf field compatibility is proven by cross-language fixtures;
  handwritten numeric tags are never accepted without such a fixture.
- Unknown protobuf fields are skipped safely, and malformed or oversized
  input returns a structured decode error.
- Every task starts with a failing focused test, ends with focused and full
  verification, updates documentation, and creates one commit.

## Rejected protobuf packages

`ocaml-protoc` 4.1 and `pbrt` 4.1 are MIT, but their runtime closure includes
`stdlib-shims` 0.3.0 under
`LGPL-2.1-only WITH OCaml-LGPL-linking-exception`. The standing exception is
limited to reviewed compiler/runtime packages, so this closure is not
accepted. `ocaml-protoc-plugin` 6.2.0 also reaches `stdlib-shims` and a much
larger build closure. Phase 2 therefore uses a project-owned wire codec with no
new OCaml runtime dependency.

## File map

| Path | Responsibility |
|---|---|
| `rust/Cargo.toml` | Rust workspace and release profile |
| `rust/Cargo.lock` | Exact Core and transitive Cargo closure |
| `rust/core-bridge/` | Project-owned Temporal Core static library and ABI tests |
| `rust/core-bridge/include/` | Installed versioned C header |
| `lib/proto/` | Private bounded protobuf wire reader/writer and Core subset |
| `lib/core_bridge/` | Private OCaml bindings and C stubs |
| `lib/worker/` | OCaml registry, Core activation adapter, and worker loop |
| `bin/` | Dune-built example/integration worker executable |
| `test/bridge/` | ABI, lock-release, and cross-language protobuf tests |
| `test/integration/` | Compose-backed real Temporal workflow tests |
| `scripts/check-cargo-licenses.py` | Cargo metadata license-policy gate |
| `compose.yaml` | Development, Temporal, PostgreSQL, UI, CLI, and test services |
| `docs/reference/core-bridge.md` | ABI ownership, threading, and upgrade contract |

### Task 1: Reproducible Rust toolchain and Cargo license gate

**Files:** Modify `Dockerfile.dev`, `compose.yaml`, `Makefile`,
`scripts/check-licenses.sh`, `docs/dependencies.md`; create `rust/Cargo.toml`,
`rust/rust-toolchain.toml`, `rust/core-bridge/Cargo.toml`,
`rust/core-bridge/src/lib.rs`, `scripts/check-cargo-licenses.py`, and focused
policy/toolchain tests.

- [x] Add a failing smoke test that requires `rustc 1.94`, `cargo`, a
  `staticlib` artifact, a locked Cargo build, and rejection of GPL, LGPL,
  AGPL, MPL, missing, and unknown fixture licenses.
- [x] Copy the Rust 1.94 toolchain from a pinned official Bookworm Rust image
  stage into the parameterized OCaml development image; record provenance and
  redistribution status.
- [x] Add a minimal Apache-2.0 Rust workspace and `staticlib` crate. Pin
  Temporal Core and related first-party crates by the immutable Git revision,
  disable unnecessary default features, and commit `Cargo.lock`.
- [x] Implement a standard-library Python Cargo metadata checker. Evaluate
  SPDX `AND`, `OR`, parentheses, and approved exceptions conservatively;
  require a documented chosen permissive branch for dual-licensed packages.
- [x] Integrate `cargo fmt --check`, `cargo clippy --locked -- -D warnings`,
  and `cargo test --locked` into the build/test Make target. Run the Cargo
  scanner from a separate pinned official Python image entirely inside the
  single standalone GitHub Actions license job; do not put it in the Makefile
  or duplicate it across the compiler/architecture matrix.
- [x] Run focused tests and `make verify` locally on OCaml 5.2, update the
  dependency inventory, push, and use the amd64/arm64 GitHub matrix as the
  compatibility gate for OCaml 5.3 through 5.5. Reproduce another compiler
  locally only if its CI cell fails.

### Task 2: Versioned, panic-safe Rust C ABI

**Files:** Modify `rust/core-bridge/src/lib.rs`; create
`rust/core-bridge/include/ocaml_temporal_core.h`, Rust unit tests, and a C ABI
harness under `test/bridge/`.

- [x] Write failing tests for ABI version negotiation, explicit status codes,
  owned success/error buffers, zero-length buffers, double-free prevention by
  contract, invalid pointers, and a deliberately caught Rust panic.
- [x] Define opaque runtime/client/worker handles and a single owned result
  shape. Keep exported symbols prefixed `ocaml_temporal_core_v1_`.
- [x] Contain every exported function with `catch_unwind`; never unwind across
  C and never expose a Rust layout other than documented `repr(C)` values.
- [x] Add compile-time header/layout checks and a C executable that links the
  static archive, calls the ABI, frees every returned allocation, and exits
  cleanly under sanitizers where available.
- [x] Run Rust, C, license, and aggregate verification; document ABI ownership
  in `docs/reference/core-bridge.md`, then commit.

### Task 3: OCaml C stubs and native static linking

**Files:** Create `lib/core_bridge/`, Dune Cargo build rules, OCaml bridge tests,
and an install smoke test; modify package metadata and documentation.

- [x] Write a failing OCaml test that calls the ABI version function and a
  blocking test operation from the final native link.
- [x] Build the Rust archive through Dune without requiring a host Rust
  installation outside Docker; link it through private C stubs into an OCaml
  executable.
- [x] Convert bridge statuses to a private typed OCaml error without raising
  expected exceptions. Copy bridge buffers exactly once and always invoke the
  Rust free function.
- [x] Surround blocking calls with `caml_enter_blocking_section` and
  `caml_leave_blocking_section`. Prove another OCaml Domain makes progress
  while a bridge call waits.
- [x] Prove `dune build @install` includes the public OCaml library while the
  Rust and C implementation remain private package details.
- [ ] Run all supported OCaml/compiler architecture cells plus Rust/C tests,
  update docs, and commit.

### Task 4: Bounded Core protobuf compatibility layer

**Files:** Create `lib/proto/`, `test/bridge/fixtures/`, a Rust fixture
generator/validator, and `docs/reference/core-protocol.md`.

- [ ] Write failing pure OCaml tests for canonical varints, signed/fixed
  values, length-delimited messages, unknown-field skipping, truncation,
  overflow, recursion depth, and configured byte limits.
- [ ] Implement a dependency-free wire reader/writer using `bytes`/`string`
  from the OCaml standard library only. Decoders return structured `result`
  errors with field context.
- [ ] Add typed decoding for the Phase 2 activation jobs: initialize workflow,
  resolve activity, fire timer, cancel workflow, and remove from cache.
- [ ] Add typed encoding for schedule activity, start/cancel timer, complete,
  fail, and cancel workflow completion commands.
- [ ] Generate binary fixtures with the exact pinned Core `prost` types and
  prove OCaml decodes them. Prove Rust decodes OCaml-produced completion bytes.
  Record the upstream message and field provenance for every supported tag.
- [ ] Run fuzz/property-style bounded-input tests, full verification, update
  protocol docs, and commit.

### Task 5: Core runtime, client, worker, and blocking poll loop

**Files:** Extend `rust/core-bridge/`, `lib/core_bridge/`, and tests; modify
bridge documentation.

- [ ] Write failing Rust and OCaml tests for invalid server URLs, connection
  failure, worker construction, workflow polling, completion decode failure,
  shutdown, and repeated free/shutdown calls.
- [ ] Create one Tokio runtime per bridge runtime handle. Connect the official
  Core client and initialize a workflow-only worker with explicit namespace,
  task queue, identity, build ID, cache, poller, and graceful-shutdown options.
- [ ] Implement blocking poll and completion calls which encode/decode the
  official Core protobuf types and return owned buffers or structured errors.
- [ ] Implement idempotent shutdown ordering: initiate shutdown, drain pollers,
  finalize worker, then release client/runtime handles.
- [ ] Add leak, shutdown-race, and repeated create/destroy tests; run full
  verification, document configuration, and commit.

### Task 6: Temporal, PostgreSQL, UI, and CLI Compose stack

**Files:** Modify `compose.yaml`, `Makefile`, CI, dependency inventory, and
operator documentation; create health/readiness scripts.

- [ ] Write a failing Compose integration preflight that requires healthy
  PostgreSQL, Temporal gRPC, namespace registration, and the Temporal UI.
- [ ] Add pinned official PostgreSQL, Temporal Server, Temporal UI, and
  Temporal CLI images with named volumes, health checks, explicit networks,
  restart behavior, and no host-only assumptions.
- [ ] Add `make up`, `make down`, `make temporal-ready`,
  `make test-integration`, and log-diagnostic targets. Keep ordinary unit
  verification independent of long-lived services.
- [ ] Audit image licenses, bundled notices, architectures, and Kubernetes
  correspondence; document configuration and data lifecycle.
- [ ] Bring the stack up from empty volumes, run readiness checks, tear it
  down, run full verification, and commit.

### Task 7: First real typed OCaml workflow

**Files:** Create `lib/worker/`, `bin/`, and `test/integration/`; modify public
worker registration API, README, workflow guide, progress log, and parity
matrix.

- [ ] Write a failing end-to-end test that starts a workflow via Temporal CLI,
  waits for completion, and asserts the typed OCaml result and event history.
- [ ] Add a typed workflow registry keyed by Temporal workflow type. Decode
  initialization payloads through the definition codec and reject duplicate or
  missing registrations before polling.
- [ ] Adapt Core activations into the existing deterministic interpreter and
  encode its commands as Core completions. Keep cached executions keyed by run
  ID and honor remove-from-cache jobs.
- [ ] Add an OCaml-owned worker loop with explicit start/run/shutdown results
  and signal-aware graceful termination.
- [ ] Build an example `greeting` workflow into the final OCaml executable,
  start it against Compose, verify completion through the CLI, restart the
  worker, and repeat from clean and warm caches.
- [ ] Run unit, bridge, integration, replay, standalone license, install, and
  full compiler/architecture CI-equivalent gates. Update all user and operator
  docs, commit, push
  `master`, and verify GitHub Actions.

## Phase 2 exit evidence

Phase 2 is complete only when all of the following are linked from
`docs/progress.md`:

1. The final worker is a Dune-built OCaml executable containing the project
   Rust static library.
2. Rust/Core threads never invoke OCaml callbacks and lock-release tests pass.
3. Cargo and OPAM closures pass the executable license policy.
4. Cross-language protobuf fixtures pass in both directions.
5. Fresh Docker Compose volumes reach healthy PostgreSQL and Temporal Server.
6. A CLI-started workflow completes with the expected typed OCaml result.
7. OCaml 5.2, 5.3, 5.4, and 5.5 GitHub Actions jobs pass on native amd64 and
   arm64 runners on `master`, with the standalone license job also green.
