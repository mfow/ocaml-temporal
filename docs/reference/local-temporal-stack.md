# Local Temporal and PostgreSQL Stack

This reference describes the live infrastructure and Core lifecycle acceptance
available now. Private typed poll/completion operations exist, but the
native readiness seam is not yet composed with them in a production scheduling
loop, and live workflow execution remains under development.

The complete Compose fixture lives under `test/integration/temporal/`, including
its PostgreSQL/Temporal configuration and fixture-only helper scripts. The
repository root intentionally contains no Compose file. Use the Make commands
below: they select the fixture with explicit Compose file, project-directory,
and project-name arguments while preserving the repository root as the
development-container build context and source mount.

## Requirements and ports

Install Docker with Compose v2 and Make. PostgreSQL is reachable only by other
containers on the project network. Temporal's gRPC frontend is published at
`localhost:7233` by default; set `TEMPORAL_FRONTEND_PORT` when that host port is
already in use:

```sh
TEMPORAL_FRONTEND_PORT=17233 make temporal-start
```

The development credentials are the fixed user/password `temporal`. They are
appropriate only for this isolated local stack and must not be copied into a
shared or production deployment.

## Operator commands

```sh
make temporal-start
make temporal-health
make temporal-status
make temporal-logs
make temporal-stop
```

`temporal-start` waits for PostgreSQL, runs the one-shot schema migration,
waits for Temporal's frontend listener, and then executes the full health gate.
The health gate verifies the primary and visibility `schema_version` tables,
calls Temporal's gRPC cluster-health operation, and describes the
`temporal-sdk-test` namespace. Namespace creation is idempotent.

`temporal-stop` removes the containers and network but retains the named
PostgreSQL volume. A later `temporal-start` reuses that data and safely reruns
the schema updater. To delete local histories and all schema data explicitly:

```sh
make temporal-clean
```

## Clean integration smoke

Run the infrastructure acceptance test with:

```sh
make test-temporal-integration
```

The test intentionally removes this Compose project's Temporal volume before
and after the run. After database/frontend readiness, an OCaml executable uses
the private supervisor and C/Rust bridge to connect the official Core client,
construct and namespace-validate a workflow/remote-activity worker, exercise
invalid and repeated lifecycle transitions, and shut the graph down
deterministically.
This is a lifecycle acceptance test, not an end-to-end workflow test: a later
milestone must coordinate the existing readiness and poll/completion operations
with the deterministic executor and separate OCaml client and worker services,
then prove start, poll, execute, and completion against this cluster.
On every pull request and push to `master`, GitHub Actions runs this target once
in a standalone Ubuntu job labelled for OCaml 5.5. It is intentionally absent
from the multi-version build matrix because starting a real database and
Temporal cluster once provides the same infrastructure evidence.

Running `docker compose` directly from the repository root is unsupported. The
root Make targets are the stable interface and deliberately hide the fixture's
test-only location and Compose project identity.

If startup fails, `make temporal-logs` prints the last 200 lines from
PostgreSQL, schema migration, and Temporal Server. Image pulls can be large;
check local storage before the first run. The integration target prints those
logs automatically before cleanup when a readiness command fails.

## Image and platform policy

The stack pins official PostgreSQL 16.13 Bookworm and Temporal 1.31.0 Server
and admin-tools OCI manifests. All three manifest indexes contain native Linux
`amd64` and `arm64` images. Version upgrades require rerunning the clean smoke,
the stop/start persistence path, manifest inspection, and the dependency and
license review.

The primary software licenses are permissive: Temporal Server and its official
tools are MIT, PostgreSQL uses the PostgreSQL License, and the Docker Official
Image packaging for PostgreSQL is MIT. See the
[dependency inventory](../dependencies.md) for exact references and scope.

## Kubernetes correspondence

The Compose services intentionally separate schema migration from the server,
matching the production responsibility split:

| Compose component | Kubernetes responsibility |
|---|---|
| `postgresql` | Managed PostgreSQL or a reviewed StatefulSet |
| `temporal-schema` | Versioned schema migration Job |
| `temporal` | Temporal Server Deployment and Service |
| Compose health checks | Startup/readiness probes and cluster monitoring |
| fixed environment values | ConfigMaps and Secrets |

Use Temporal's maintained Helm charts and production guidance rather than
translating this local Compose file mechanically.
