# Private Mailbox Processor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a reusable, Dune-private, bounded FIFO mailbox processor that owns a dedicated OCaml Domain and supports typed posts, typed calls, deterministic close, and leak-free failure propagation.

**Architecture:** A functor accepts a GADT request family and exposes an abstract processor handle. Producers synchronize through a mutex-protected bounded queue; one spawned owner Domain invokes the rank-2 polymorphic handler. Per-call reply cells use their own mutex and condition, while lifecycle transitions wake every producer and waiter affected by close or failure.

**Tech Stack:** OCaml 5.2+, Dune 3.18+, standard `Domain`, `Mutex`, `Condition`, and `Queue`; no new package dependency.

## Global Constraints

- The library lives in `lib/mailbox_processor/` and has no `public_name`.
- Its `.mli` exposes no Eio, Temporal, Rust, mutex, condition, continuation, or backend types.
- FIFO order is the mutex-protected enqueue linearization order; per-producer program order is preserved, while simultaneous senders have no stronger ordering before admission.
- Capacity is positive and counts queued, admitted work; producers block while that queue is full.
- Normal close rejects new work and drains admitted work; handler exceptions reject new work, discard queued posts, and fail queued calls.
- Blocking entry points are not fiber-safe and must not run on a cooperative scheduler Domain; a future Eio adapter must offload them with `Eio_unix.run_in_systhread` or an equivalent bridge.
- All source and test helpers receive proportionate documentation.

---

### Task 1: Executable mailbox contract tests

**Files:**
- Create: `test/mailbox_processor/dune`
- Create: `test/mailbox_processor/test_mailbox_processor.ml`
- Modify: `test/smoke/test_repository.ml`

**Interfaces:**
- Consumes: A future `Mailbox_processor.Make` functor over `type _ t`.
- Produces: Failing tests for FIFO, cross-Domain exactly-once delivery, typed call, backpressure, close, handler failure, join, and install privacy.

- [ ] Write each focused behavior test before implementation.
- [ ] Run `opam exec -- dune runtest test/mailbox_processor` and confirm compilation fails because `Mailbox_processor` does not exist.
- [ ] Add the smoke assertion for a private Dune stanza and absence from `dune build @install` output.

### Task 2: Minimal private mailbox processor

**Files:**
- Create: `lib/mailbox_processor/dune`
- Create: `lib/mailbox_processor/mailbox_processor.mli`
- Create: `lib/mailbox_processor/mailbox_processor.ml`

**Interfaces:**
- Consumes: `module type Request = sig type _ t end` and the focused tests.
- Produces: `Make.create`, `post`, `call`, `close`, and `join`, plus abstract `t` and typed `failure`.

- [ ] Add the private Dune library and documented public-to-repository interface.
- [ ] Implement mutex-protected admission and owner dequeue loops with condition predicates rechecked after every wake.
- [ ] Implement typed one-shot reply cells and terminal failure settlement without representation casts.
- [ ] Run the focused suite after each minimal behavior is added.
- [ ] Run the stress cases repeatedly to expose race, ordering, and waiter-leak defects.

### Task 3: Ownership decision and project progress

**Files:**
- Create: `docs/decisions/0003-private-mailbox-processor.md`
- Modify: `docs/reference/runtime-invariants.md`
- Modify: `docs/implementation-roadmap.md`
- Modify: `docs/progress.md`
- Modify: `README.md` if the model disclosure is absent.

**Interfaces:**
- Consumes: The verified implementation behavior and official OCaml/Eio documentation.
- Produces: Plain-language state transitions, happens-before rules, blocking restrictions, dependency decision, and verified progress.

- [ ] Record why the standard Domain-safe primitives are used and why Eio remains an adapter concern.
- [ ] Document normal and exceptional shutdown, ordering, capacity, ownership, and future scheduler integration.
- [ ] Mark only behavior demonstrated by tests as complete.
- [ ] Add the source-changing model name/version to `README.md` if needed.

### Task 4: Verification and delivery

**Files:**
- Verify all changed files.

**Interfaces:**
- Consumes: Tasks 1-3.
- Produces: A committed, pushed PR merged to `master` only after a fresh complete green matrix.

- [ ] Run `opam exec -- dune runtest test/mailbox_processor test/smoke` repeatedly for focused stress.
- [ ] Run `make native-verify NATIVE_OCAML_VERSION=5.4 NATIVE_RUST_VERSION=1.96.0` for the local representative toolchain.
- [ ] Run `git diff --check` and inspect `dune build @install` for mailbox artifacts.
- [ ] Commit, fetch HTTPS, merge `origin/master` without rebasing, rerun verification, and push.
- [ ] Open a PR to `master`, apply `enhancement`, `quality`, and `test` when available, and wait for the complete matrix including Windows x64.
- [ ] Fix failures, repeat the final origin merge when needed, and merge the PR with a merge commit only when green and mergeable.
