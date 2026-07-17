# ADR 0005: Real Temporal Server and PostgreSQL Compose Substrate

**Status:** Accepted

**Date:** 2026-07-11

## Context

The SDK needs a repeatable cluster for live worker acceptance tests. A TCP
listener by itself is insufficient evidence: PostgreSQL may be reachable before
Temporal's schemas exist, and the Temporal frontend may accept connections
before its gRPC API can serve requests.

Temporal deprecated the `temporalio/auto-setup` image. Its maintained
[server samples](https://github.com/temporalio/samples-server/tree/main/compose)
now use the supported `temporalio/server` image, an explicit schema job based
on `temporalio/admin-tools`, and a separate database. Compose remains a local
development and test mechanism; Temporal recommends its Helm charts and
explicit schema management for Kubernetes deployments.

## Decision

The repository provides an opt-in `temporal` Compose profile containing:

- PostgreSQL 16 with a named data volume and an in-container `pg_isready`
  health check;
- a one-shot official admin-tools container that creates and migrates both the
  primary and visibility schemas;
- the official Temporal Server image with the PostgreSQL dynamic configuration
  and an internal frontend port check; and
- a run-on-demand admin-tools service used for the real gRPC health RPC and
  idempotent test-namespace registration.

Every image reference includes both a human-readable version tag and its OCI
manifest digest. The selected manifests publish native Linux `amd64` and
`arm64` variants, so Apple Silicon executes the ARM image rather than an
emulated x86 image. PostgreSQL is not published on a host port; only Temporal's
frontend port is exposed for later OCaml clients and workers.

Make targets are the supported interface. `temporal-start` waits for the
dependency chain and then runs the stronger health gate. `temporal-stop`
preserves database data, while `temporal-clean` explicitly removes it. The
integration smoke starts from an empty named volume, proves both Temporal SQL
schemas, invokes `temporal operator cluster health`, verifies a namespace, and
always removes its containers and volume.

## Consequences

The ordinary unit and native desktop jobs remain independent of these Linux
services. This keeps Windows x64 and macOS ARM build verification focused on
the native OCaml/Rust artifact. The Linux `temporal-integration` workflow job
invokes `make test-temporal-integration` and the related live restart,
recovery, cache-eviction, patching, and parent/child replay gates. Keeping
those commands in the Makefile lets CI reuse the same service setup without
duplicating it in workflow YAML.

This milestone does not claim that the OCaml SDK can connect, start, poll, or
complete a workflow. It establishes the real database and server substrate for
the later OCaml test-client, workflow worker, and mock-activity binaries. The
Temporal UI is also deferred until it becomes part of the operator acceptance
surface; it is not required to prove frontend or persistence readiness.

For Kubernetes, the conceptual mapping is a schema migration Job, a managed or
stateful PostgreSQL deployment, a Temporal Server Deployment/Service, native
readiness probes, and Secrets rather than the development credentials in this
Compose file. Compose is not a production deployment template.
