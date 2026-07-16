# Child failure after worker-replay acceptance

This acceptance fixture complements the successful parent/child replacement
gate. It starts a parent and a child in generation one, stops that worker while
the child is waiting on a durable 60-second timer, and starts a fresh worker
generation. Both workflow activations must report replay before the child is
allowed to finish.

The child then returns a typed, non-retryable workflow failure. Temporal records
that as `WorkflowExecutionFailed` in the child history and
`ChildWorkflowExecutionFailed` in the parent history. The parent handles the
typed child failure and returns the stable result
`SMOKE:PARENT:CHILD:FAILURE_RECOVERED`. An unexpected successful child result,
wrong child identity, or a failure before replay is rejected.

The client-only driver and worker-only binary are the same binaries used by the
successful parent/child fixture. `SMOKE_PARENT_CHILD_REPLAY_SCENARIO=failure`
selects the failure definitions and the external controller passes the exact
parent and child run IDs into every history query. The history validator is
parameterized with the child workflow type and outcome, while the controller
validator requires `child_failure_recovered`. The source-only contract derives
failure snapshots from the checked-in success snapshots and proves that the
success validators reject the failure event sequence.

Run the contract with:

```sh
make test-temporal-parent-child-failure-replay-contract
```

The live gate, which requires Docker, PostgreSQL, and Temporal Server, is:

```sh
make test-temporal-parent-child-failure-replay-live
```

This fixture is intentionally documented as pending live evidence until its
complete CI run has passed; the contract does not substitute for a real
Temporal replay and failure observation.
