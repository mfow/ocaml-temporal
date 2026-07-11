# Documentation Guide

This directory explains both the API that exists now and the SDK that the
project intends to build. Start with these documents:

1. [Writing workflows](guides/workflows.md) explains the current OCaml API with
   examples.
2. [Verified progress](progress.md) records what has actually been built and
   tested. Use it to distinguish current behavior from planned behavior.
3. [Implementation roadmap](implementation-roadmap.md) lists the work required
   to reach full Temporal SDK feature parity.
4. [Runtime invariants](reference/runtime-invariants.md) describes rules that
   runtime changes must preserve.
5. [Native Core bridge](reference/core-bridge.md) explains how OCaml safely
   calls the Rust library linked into the final executable.
6. [Private JSON control protocol](reference/core-protocol.md) defines the
   strict bounded envelope used by the two compiled halves of the SDK.
7. [OCaml SDK logging](reference/observability.md) documents stable sources,
   tags, levels, application setup, and privacy rules.
8. [SDK instance supervisor decision](decisions/0004-sdk-instance-supervisor.md)
   explains how one owner Domain serializes the complete native handle graph.
9. [Architecture specification](superpowers/specs/2026-07-11-ocaml-temporal-sdk-design.md)
   describes the long-term design. Unimplemented APIs in that document are
   targets, not claims about the current package.

Files under `superpowers/plans/` are historical implementation plans. They
record why milestones were ordered in a particular way. The progress log and
current source are authoritative when a plan has become outdated.

## Terms used in this project

- **Temporal Server** stores workflow history and dispatches work. It does not
  execute OCaml workflow functions itself.
- **Temporal Core** is the official Rust library that handles server
  communication and Temporal's worker state machines for language SDKs.
- **Activation** is one batch of work that Core gives to the OCaml runtime,
  such as starting a workflow or delivering an activity result.
- **Command** is an instruction returned by workflow code, such as scheduling
  an activity or starting a timer. Temporal records commands in history.
- **Replay** means running workflow code again against recorded history. The
  code must make the same decisions and issue the same commands.
- **Payload** is an opaque byte sequence plus metadata describing its encoding.
  JSON is one supported encoding, not a Temporal requirement.
- **Codec** converts a typed OCaml value to and from a payload.
- **Future** represents a result that may arrive in a later activation. Waiting
  for one suspends the current workflow fiber, not the whole worker process.
- **Bridge** is the small C-compatible interface between OCaml and the
  project-owned Rust static library.
