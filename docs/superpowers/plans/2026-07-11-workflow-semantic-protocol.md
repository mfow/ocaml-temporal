# Workflow Semantic Protocol Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a closed, bilaterally validated JSON representation for the first Temporal workflow activation jobs and completion commands, with proven Rust conversions at the pinned Temporal Core boundary.

**Architecture:** A new private `Workflow_protocol` module owns typed OCaml values and operation-body JSON. The Rust bridge mirrors those values and is the only layer that imports Temporal protobuf types. Both implementations first use the existing duplicate-aware, resource-bounded JSON foundation, then apply the same exact-field semantic validation, and both encode paths reparse their own output. Shared fixtures and Draft 2020-12 schemas define the reviewed contract without replacing runtime validation.

**Tech Stack:** OCaml 5, Yojson, Rust 1.94, serde_json, base64, pinned `temporalio-protos`, Dune, Cargo.

## Global Constraints

- OCaml owns the executable; protobuf and Rust types remain private.
- Expected validation and translation failures use typed `result` values.
- Job and command arrays preserve source order exactly; sequence numbers are unsigned 32-bit values represented as JSON integers.
- All JSON objects are closed and reject duplicate, missing, and unknown fields.
- Binary values use canonical padded RFC 4648 base64 and the shared 128 MiB
  per-field bridge safety ceiling. The 192 MiB aggregate document ceiling
  remains the effective bound when multiple values are batched.
- No new non-permissive dependency may enter either locked graph.

---

### Task 1: Reusable strict object boundary and shared contract fixtures

**Files:**
- Modify: `lib/protocol/control_protocol.ml`
- Modify: `lib/protocol/control_protocol.mli`
- Modify: `rust/core-bridge/src/protocol.rs`
- Create: `docs/schemas/bridge/temporal-payload.schema.json`
- Create: `docs/schemas/bridge/temporal-failure.schema.json`
- Create: `docs/schemas/bridge/workflow-activation.schema.json`
- Create: `docs/schemas/bridge/workflow-completion.schema.json`
- Create: `test/bridge/fixtures/workflow-protocol/valid/*.json`
- Create: `test/bridge/fixtures/workflow-protocol/invalid/*.json`

**Interfaces:**
- Produces OCaml `decode_object : string -> (Yojson.Safe.t, error) result` and `encode_object : Yojson.Safe.t -> (string, error) result`.
- Produces Rust `decode_object : &str -> Result<JsonValue, ProtocolError>` and `encode_object : &JsonValue -> Result<String, ProtocolError>`.

- [ ] Write OCaml and Rust tests proving a duplicate nested key is rejected and outgoing objects are normalized and reparsed.
- [ ] Run the focused tests and confirm they fail because the object functions do not exist.
- [ ] Add the two object-boundary functions by reusing the existing strict parser, tree validator, normalized renderer, and independent reparse checks.
- [ ] Add closed schemas and positive/malformed fixtures for every variant in Tasks 2 and 3.
- [ ] Rerun focused tests and retain the fixture tree as the shared language-neutral contract.

### Task 2: Typed workflow activations in OCaml and Rust

**Files:**
- Create: `lib/protocol/workflow_protocol.mli`
- Create: `lib/protocol/workflow_protocol.ml`
- Modify: `lib/protocol/dune`
- Create: `test/bridge/test_ocaml_workflow_protocol.ml`
- Create: `rust/core-bridge/src/workflow_protocol.rs`
- Create: `rust/core-bridge/tests/workflow_protocol.rs`
- Modify: `rust/core-bridge/src/lib.rs`

**Interfaces:**
- Produces mirrored `payload`, `failure`, `activity_resolution`, `eviction_reason`, `activation_job`, and `activation` types.
- Produces `decode_activation` and `encode_activation` in both languages.
- Produces Rust `activation_from_core` for the supported official activation job variants.

- [ ] Write shared-fixture tests for initialization, activity completion/failure/cancellation, timer firing, workflow cancellation, and eviction; assert source job order survives a round trip.
- [ ] Write malformed-fixture tests for every discriminator, exact nested shape, sequence bound, identifier, failure, payload, eviction invariant, canonical base64, and resource limit.
- [ ] Run both tests and observe missing API failures.
- [ ] Implement typed decoders with exact-field checks at every object and path-specific privacy-safe errors.
- [ ] Implement normalized encoders that validate typed values, serialize, and decode again before returning bytes.
- [ ] Implement Core-to-semantic conversion using the exact pinned protobuf variants; return `Unsupported` for unimplemented jobs, absent oneofs, unknown enums, external payload references, or failure-info kinds not represented by this milestone.
- [ ] Prove representative official Core values translate to the same shared normalized JSON.

### Task 3: Typed workflow completions and Core commands

**Files:**
- Modify: `lib/protocol/workflow_protocol.mli`
- Modify: `lib/protocol/workflow_protocol.ml`
- Modify: `test/bridge/test_ocaml_workflow_protocol.ml`
- Modify: `rust/core-bridge/src/workflow_protocol.rs`
- Modify: `rust/core-bridge/tests/workflow_protocol.rs`

**Interfaces:**
- Produces mirrored `activity_cancellation_type`, `duration`, `completion_command`, and `completion` types.
- Produces `decode_completion` and `encode_completion` in both languages.
- Produces Rust `completion_to_core` and `completion_from_core` for the supported official command variants.

- [ ] Write shared-fixture tests for schedule/cancel activity, start/cancel timer, and complete/fail/cancel workflow commands; assert command order survives exactly.
- [ ] Write malformed tests for duration normalization/ranges, cancellation type, missing payload/failure, terminal-command ordering, identifiers, sequence bounds, and all exact object shapes.
- [ ] Run both tests and observe missing API failures.
- [ ] Implement typed decode/encode and semantic validation, including nonnegative normalized protobuf durations and at most one terminal command in final position.
- [ ] Implement conversions to the exact Core completion and command protobufs with documented defaults; reject Core values containing unsupported metadata/options rather than silently discarding them in reverse conversion.
- [ ] Round-trip every supported official Core command through semantic JSON and back.

### Task 4: Contributor documentation and delivery gate

**Files:**
- Modify: `docs/reference/core-protocol.md`
- Modify: `docs/reference/core-bridge.md`
- Modify: `docs/progress.md`
- Modify: `docs/implementation-roadmap.md`
- Add: `docs/decisions/0006-first-workflow-semantic-protocol.md`

- [ ] Document every field, unit, ordering rule, lifecycle direction, payload/failure mapping, Core conversion boundary, and deliberately unsupported pinned variant.
- [ ] Run formatting, focused OCaml/Rust protocol tests, full native verification, and the locked license audit where available.
- [ ] Sync current `master`, resolve conflicts, and rerun all changed-head checks including the live Temporal Compose CI lane.
- [ ] Commit and push over HTTPS; open a labeled PR; inspect all conversation comments, reviews, and unresolved threads.
- [ ] Fix and resolve actionable feedback, wait for the full current-head CI matrix, refresh feedback immediately before merge, and squash-merge.
