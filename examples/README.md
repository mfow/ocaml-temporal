# Temporal SDK examples

These examples form one small three-process application. They use only the
public `temporal-sdk` library, so they also show the boundary an application
uses after installing the package.

Start Temporal Server, then run these programs in separate terminals in this
order:

```sh
dune exec examples/activity_worker/activity_worker.exe
dune exec examples/workflow_worker/workflow_worker.exe
dune exec examples/client/client.exe -- "Ada Lovelace"
```

The activity worker turns two requested message styles into text. The workflow
worker concurrently schedules those activities, records a short durable timer,
and returns the combined message. The client starts one execution, waits for
its exact run, and prints the completed value.

All three programs read the same optional environment variables:

- `TEMPORAL_ADDRESS` (default: `http://127.0.0.1:7233`)
- `TEMPORAL_NAMESPACE` (default: `default`)
- `TEMPORAL_TASK_QUEUE` (default: `ocaml-temporal-example`)

The client also accepts an optional name argument and `TEMPORAL_WORKFLOW_ID`.
Set a unique workflow ID when deliberately re-running an execution in a shared
namespace. The worker programs handle `SIGINT` and `SIGTERM` by requesting the
public graceful shutdown operation before their processes exit.
