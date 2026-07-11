# Phase 1 Workflow Authoring API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add idiomatic, typed OCaml authoring operations for child workflows, durable timers, aggregate waits, and first-completed selection.

**Architecture:** Extend the established public modules and private synthetic activation runtime. Public futures retain structured `Error.t` for expected failures, while the runtime keeps effects, continuations, command IDs, and resolver ownership private.

**Tech Stack:** OCaml 5 algebraic effects, Dune, the existing Temporal runtime scheduler, and Alcotest-free executable unit tests.

## Global Constraints

- Expected operational failures and ownership misuse return typed `result` values.
- `race` and `first` settle on the first completion, including errors, without cancelling losers.
- `all` waits for every input, preserves value order, and reports the first error in input order.
- Workflow command creation and callback delivery remain deterministic and FIFO.
- Effect constructors, continuations, protocol JSON, and Rust handles remain private.
- Core polling, cancellation, retries, options, and structured concurrency are outside this phase but remain parity goals.

---

### Task 1: Future aggregation semantics

**Files:**
- Modify: `test/runtime/test_scheduler.ml`
- Modify: `lib/runtime/future_store.ml`
- Modify: `lib/runtime/future_store.mli`
- Modify: `lib/public/future.ml`
- Modify: `lib/public/future.mli`

**Interfaces:**
- Consumes: existing scheduler-owned `Future_store.t` and FIFO observers.
- Produces: `Future.all`, `Future.race`, `Future.first`, and typed ownership failures.

- [ ] **Step 1: Write failing scheduler tests**

  Add tests that create real scheduler promises and assert ordered `all`, first
  completion success/error, deterministic ready-input order, loser observation,
  and a structured defect from cross-scheduler inputs.

- [ ] **Step 2: Verify the tests fail for missing functions**

  Run: `opam exec -- dune runtest --root . test/runtime`

  Expected: compilation fails because `Temporal.Future.all`, `race`, and
  `first` do not exist.

- [ ] **Step 3: Implement minimal aggregation**

  Add a public race variant, owner validation, one-shot completion guards, and
  ordered result collection. Constrain aggregate public errors to `Error.t` so
  ownership defects are values.

- [ ] **Step 4: Verify future tests pass**

  Run: `opam exec -- dune runtest --root . test/runtime`

  Expected: all runtime tests pass without warnings.

### Task 2: Durable asynchronous sleep

**Files:**
- Modify: `test/runtime/test_activation.ml`
- Modify: `lib/public/workflow.ml`
- Modify: `lib/public/workflow.mli`

**Interfaces:**
- Consumes: `Workflow_context_store.start_timer` and `Future.await`.
- Produces: `Workflow.start_sleep`; preserves `Workflow.sleep` as a composition.

- [ ] **Step 1: Write failing timer tests**

  Assert that nonzero `start_sleep` emits a timer without waiting, zero duration
  is immediately ready without a command, and two timers can start before one
  is awaited.

- [ ] **Step 2: Verify the tests fail for missing `start_sleep`**

  Run: `opam exec -- dune runtest --root . test/runtime`

  Expected: compilation fails because `Temporal.Workflow.start_sleep` is absent.

- [ ] **Step 3: Implement `start_sleep` and compose `sleep`**

  Move timer creation into `start_sleep`; return a context-owned ready future
  for zero and a typed detached defect outside workflow execution.

- [ ] **Step 4: Verify timer tests pass**

  Run: `opam exec -- dune runtest --root . test/runtime`

  Expected: all activation tests pass.

### Task 3: Typed child-workflow invocation

**Files:**
- Create: `lib/public/child_workflow.ml`
- Create: `lib/public/child_workflow.mli`
- Modify: `lib/runtime/activation.ml`
- Modify: `lib/runtime/activation.mli`
- Modify: `lib/runtime/workflow_context_store.ml`
- Modify: `lib/runtime/workflow_context_store.mli`
- Modify: `lib/runtime/execution.ml`
- Modify: `test/runtime/test_activation.ml`

**Interfaces:**
- Consumes: typed `Workflow.t` definitions, codecs, context sequence allocation,
  and scheduler futures.
- Produces: `Child_workflow.start`, `Child_workflow.execute`, child schedule
  commands, and child resolution jobs.

- [ ] **Step 1: Write failing child-workflow tests**

  Assert typed input encoding, emitted child type name and command order,
  suspension, successful output decoding, remote errors, invalid codec input,
  and duplicate/unknown resolution rejection.

- [ ] **Step 2: Verify the tests fail for missing child operations**

  Run: `opam exec -- dune runtest --root . test/runtime`

  Expected: compilation fails because the child module and activation variants
  are absent.

- [ ] **Step 3: Implement the private child command path**

  Add one resolver table owned by the workflow context, remove entries before
  resolution, and route activation jobs through execution without exposing the
  table or command IDs publicly.

- [ ] **Step 4: Implement the public child module**

  Mirror the proven activity API: encode before allocating a command, return a
  typed failed future for codec errors, and define `execute` as `start` followed
  by `Future.await`.

- [ ] **Step 5: Verify child tests pass**

  Run: `opam exec -- dune runtest --root . test/runtime`

  Expected: all runtime tests pass.

### Task 4: Public examples, documentation, and full verification

**Files:**
- Modify: `docs/guides/workflows.md`
- Create: `test/unit/test_workflow_authoring.ml`
- Modify: `test/unit/dune`
- Modify: `docs/progress.md`

**Interfaces:**
- Consumes: all Phase 1 public operations.
- Produces: compile-checked ordinary helper examples and accurate parity status.

- [ ] **Step 1: Write a failing helper-composition test**

  Define ordinary higher-order OCaml functions that accept partially applied
  activity/child starters, start multiple operations, and compose `all` and
  `race` without exposing runtime types beyond `Future.t`.

- [ ] **Step 2: Verify the helper test fails until wired into Dune**

  Run: `opam exec -- dune runtest --root . test/unit`

  Expected: the new test is not yet built or fails on its asserted API contract.

- [ ] **Step 3: Add docs and Dune wiring**

  Document complete examples, determinism rules, settlement semantics, and the
  explicitly incomplete parity features. Add the test executable to Dune.

- [ ] **Step 4: Run all applicable local gates**

  Run native Dune build/tests, Rust fmt/clippy/tests, repository formatting,
  OPAM lint, `make quality`, and diff checks. Docker Compose remains the CI gate
  when the local Docker daemon is absent.

  Expected: every available local check passes with no new warnings.
