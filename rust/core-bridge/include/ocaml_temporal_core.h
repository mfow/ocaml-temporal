#ifndef OCAML_TEMPORAL_CORE_H
#define OCAML_TEMPORAL_CORE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define OCAML_TEMPORAL_CORE_ABI_VERSION UINT32_C(1)

/* Signed fixed-width status keeps layout identical across supported ABIs. */
typedef int32_t ocaml_temporal_core_status;

enum {
  OCAML_TEMPORAL_CORE_STATUS_OK = 0,
  OCAML_TEMPORAL_CORE_STATUS_INVALID_ARGUMENT = 1,
  OCAML_TEMPORAL_CORE_STATUS_ABI_MISMATCH = 2,
  OCAML_TEMPORAL_CORE_STATUS_PANIC = 3,
  OCAML_TEMPORAL_CORE_STATUS_INTERNAL = 4,
  OCAML_TEMPORAL_CORE_STATUS_INVALID_STATE = 5,
  OCAML_TEMPORAL_CORE_STATUS_CONFIGURATION = 6,
  OCAML_TEMPORAL_CORE_STATUS_CONNECTION = 7,
  OCAML_TEMPORAL_CORE_STATUS_WORKER = 8,
  OCAML_TEMPORAL_CORE_STATUS_OUTSTANDING_TASKS = 9,
  OCAML_TEMPORAL_CORE_STATUS_NOT_READY = 10,
  OCAML_TEMPORAL_CORE_STATUS_PROTOCOL = 11,
  OCAML_TEMPORAL_CORE_STATUS_ALREADY_STARTED = 12,
  OCAML_TEMPORAL_CORE_STATUS_RETRYABLE = 13
};

/* Rust-owned byte allocation. `{ NULL, 0 }` is the sole empty representation. */
typedef struct ocaml_temporal_core_buffer {
  uint8_t *ptr;
  size_t len;
} ocaml_temporal_core_buffer;

/* Uniform fallible return object. Exactly one buffer may be live according to
 * status; release through the versioned result-free function only. */
typedef struct ocaml_temporal_core_result {
  ocaml_temporal_core_status status;
  ocaml_temporal_core_buffer value;
  ocaml_temporal_core_buffer error;
} ocaml_temporal_core_result;

/* Opaque handle types reserved by ABI v1 for the worker implementation. */
typedef struct ocaml_temporal_core_runtime ocaml_temporal_core_runtime;
typedef struct ocaml_temporal_core_client ocaml_temporal_core_client;
typedef struct ocaml_temporal_core_worker ocaml_temporal_core_worker;

/*
 * Every fallible operation initializes `output` and returns the same status
 * stored in `output->status`. Exactly one of `value` or `error` may own bytes.
 * A zero-length buffer is represented canonically as { NULL, 0 }.
 *
 * The caller must release every initialized result with
 * ocaml_temporal_core_v1_result_free. Releasing the same result object twice
 * is safe because the first call resets it. Copying a live result and freeing
 * both copies is invalid. Callers must not modify owned pointer/length fields.
 */
ocaml_temporal_core_status ocaml_temporal_core_v1_check_abi_version(
    uint32_t requested_version, ocaml_temporal_core_result *output);

ocaml_temporal_core_status ocaml_temporal_core_v1_echo(
    const uint8_t *input, size_t input_len,
    ocaml_temporal_core_result *output);

/*
 * Bounded native wait used only to verify that bindings release their runtime
 * lock. This is not a Temporal workflow timer.
 */
ocaml_temporal_core_status ocaml_temporal_core_v1_conformance_wait_ms(
    uint32_t milliseconds, ocaml_temporal_core_result *output);

/*
 * Create the sole native runtime owner for one SDK instance. On success,
 * `runtime` receives an opaque handle and `output` is an empty success.
 */
ocaml_temporal_core_status ocaml_temporal_core_v1_runtime_new(
    ocaml_temporal_core_runtime **runtime,
    ocaml_temporal_core_result *output);

/*
 * Strictly decode one UTF-8 JSON client configuration, connect through the
 * official Temporal client, and retain it beneath `runtime`. No client is
 * retained on failure. The input is borrowed only for this blocking call.
 */
ocaml_temporal_core_status ocaml_temporal_core_v1_client_connect_json(
    ocaml_temporal_core_runtime *runtime, const uint8_t *input,
    size_t input_len, ocaml_temporal_core_result *output);

/* Start one dynamically named workflow using strict JSON. The successful
 * value is an execution reference. An AlreadyStarted failure has status 12
 * and a closed JSON body in `error` containing workflow_id and existing_run_id. */
ocaml_temporal_core_status ocaml_temporal_core_v1_client_start_workflow_json(
    ocaml_temporal_core_runtime *runtime, const uint8_t *input,
    size_t input_len, ocaml_temporal_core_result *output);

/* Request cancellation of one exact workflow run. The successful value is a
 * strict {"acknowledged":true} document; the request and response are copied
 * across the private JSON boundary and Temporal protobuf remains Rust-owned. */
ocaml_temporal_core_status ocaml_temporal_core_v1_client_cancel_workflow_json(
    ocaml_temporal_core_runtime *runtime, const uint8_t *input,
    size_t input_len, ocaml_temporal_core_result *output);

/* Admit one workflow start without waiting for the RPC. The successful value
 * is an opaque JSON ticket. The caller must later poll or wait that ticket;
 * the Rust owner keeps its Core connection and Tokio task alive until one
 * terminal result is observed or the runtime is closed. */
ocaml_temporal_core_status ocaml_temporal_core_v1_client_begin_start_workflow_json(
    ocaml_temporal_core_runtime *runtime, const uint8_t *input,
    size_t input_len, ocaml_temporal_core_result *output);

/* Poll one asynchronous start ticket without waiting. STATUS_NOT_READY means
 * the RPC remains in flight. A terminal success value is a closed JSON object
 * with kind accepted, rejected, or unknown; the ticket is then retired. */
ocaml_temporal_core_status ocaml_temporal_core_v1_client_poll_start_workflow_json(
    ocaml_temporal_core_runtime *runtime, const uint8_t *input,
    size_t input_len, ocaml_temporal_core_result *output);

/* Wait for one asynchronous start ticket for a bounded interval. The C stub
 * must release the OCaml runtime lock. A timeout is STATUS_NOT_READY so the
 * supervisor can service its mailbox and retry. */
ocaml_temporal_core_status ocaml_temporal_core_v1_client_wait_start_workflow_json(
    ocaml_temporal_core_runtime *runtime, const uint8_t *input,
    size_t input_len, ocaml_temporal_core_result *output);

/* Wait for one exact run using fixed follow_runs=false semantics. Each call
 * keeps the close-event history long poll for at most 100 ms. An open run
 * returns NOT_READY (status 10) without terminal JSON so the caller can retry;
 * this keeps the single supervisor owner responsive to lifecycle messages. A
 * continued-as-new close is returned as a terminal outcome with successor
 * metadata; the bridge never follows it implicitly. */
ocaml_temporal_core_status ocaml_temporal_core_v1_client_wait_workflow_json(
    ocaml_temporal_core_runtime *runtime, const uint8_t *input,
    size_t input_len, ocaml_temporal_core_result *output);

/*
 * Strictly decode workflow-only worker configuration, construct the official
 * Core worker, and validate its namespace before returning success. A failed
 * temporary worker is cleaned before this operation returns.
 */
ocaml_temporal_core_status ocaml_temporal_core_v1_worker_start_json(
    ocaml_temporal_core_runtime *runtime, const uint8_t *input,
    size_t input_len, ocaml_temporal_core_result *output);

/* Non-blocking Rust-owned task handoff. `NOT_READY` is an expected empty-lane
 * result; success owns one strictly validated semantic JSON document. */
ocaml_temporal_core_status ocaml_temporal_core_v1_worker_try_poll_workflow(
    ocaml_temporal_core_runtime *runtime,
    ocaml_temporal_core_result *output);

/* Bounded readiness wait for the workflow lane. The C/OCaml binding must call
 * this with the OCaml runtime lock released; `NOT_READY` means the bounded
 * wait elapsed and the owner supervisor should service its mailbox and retry.
 * Success does not consume a task; callers drain it with try_poll_workflow. */
ocaml_temporal_core_status ocaml_temporal_core_v1_worker_wait_workflow(
    ocaml_temporal_core_runtime *runtime,
    ocaml_temporal_core_result *output);

/* Complete exactly one previously handed-off workflow activation. */
ocaml_temporal_core_status ocaml_temporal_core_v1_worker_complete_workflow_json(
    ocaml_temporal_core_runtime *runtime, const uint8_t *input,
    size_t input_len, ocaml_temporal_core_result *output);

/* Reparse and reject the exact Rust-produced activation document when the
 * OCaml semantic decoder cannot accept it after lease handoff. */
ocaml_temporal_core_status ocaml_temporal_core_v1_worker_reject_workflow_json(
    ocaml_temporal_core_runtime *runtime, const uint8_t *input,
    size_t input_len, ocaml_temporal_core_result *output);

/* Non-blocking task handoff for the independently guarded remote-activity
 * lane. Local activities and Nexus are disabled by worker configuration. */
ocaml_temporal_core_status ocaml_temporal_core_v1_worker_try_poll_activity(
    ocaml_temporal_core_runtime *runtime,
    ocaml_temporal_core_result *output);

/* Bounded readiness wait for the remote-activity lane. It has the same lock,
 * timeout, and non-consuming semantics as the workflow readiness operation. */
ocaml_temporal_core_status ocaml_temporal_core_v1_worker_wait_activity(
    ocaml_temporal_core_runtime *runtime,
    ocaml_temporal_core_result *output);

/* Apply the fixed native delay used only after Rust has explicitly proven
 * that an activity completion was not consumed. The C binding must release
 * the OCaml runtime lock while this bounded timer runs. */
ocaml_temporal_core_status
ocaml_temporal_core_v1_worker_wait_activity_completion_retry_backoff(
    ocaml_temporal_core_runtime *runtime,
    ocaml_temporal_core_result *output);

/* Complete exactly one previously handed-off remote activity task. */
ocaml_temporal_core_status ocaml_temporal_core_v1_worker_complete_activity_json(
    ocaml_temporal_core_runtime *runtime, const uint8_t *input,
    size_t input_len, ocaml_temporal_core_result *output);

/* Record progress for one currently leased remote activity task. */
ocaml_temporal_core_status
ocaml_temporal_core_v1_worker_record_activity_heartbeat_json(
    ocaml_temporal_core_runtime *runtime, const uint8_t *input,
    size_t input_len, ocaml_temporal_core_result *output);

/* Reparse and reject the exact Rust-produced activity-task document when the
 * OCaml semantic decoder cannot accept it after lease handoff. */
ocaml_temporal_core_status ocaml_temporal_core_v1_worker_reject_activity_json(
    ocaml_temporal_core_runtime *runtime, const uint8_t *input,
    size_t input_len, ocaml_temporal_core_result *output);

/* Gracefully stop the worker. Repeating this operation is safe. */
ocaml_temporal_core_status ocaml_temporal_core_v1_worker_shutdown(
    ocaml_temporal_core_runtime *runtime,
    ocaml_temporal_core_result *output);

/* Drop the client after its worker is absent. Repeating this operation is safe. */
ocaml_temporal_core_status ocaml_temporal_core_v1_client_disconnect(
    ocaml_temporal_core_runtime *runtime,
    ocaml_temporal_core_result *output);

/*
 * Destroy a runtime and clear the caller's slot. Calling this again with the
 * same now-null slot is safe. The runtime defensively finalizes a remaining
 * worker and drops its client in reverse ownership order before Core.
 */
ocaml_temporal_core_status ocaml_temporal_core_v1_runtime_free(
    ocaml_temporal_core_runtime **runtime);

/*
 * GC fallback that transfers runtime destruction to its Rust cleanup thread
 * without waiting. SDK supervisors use runtime_free for explicit shutdown.
 */
ocaml_temporal_core_status ocaml_temporal_core_v1_runtime_dispose(
    ocaml_temporal_core_runtime **runtime);

ocaml_temporal_core_status ocaml_temporal_core_v1_result_free(
    ocaml_temporal_core_result *result);

#ifdef __cplusplus
} /* extern "C" */
#endif

#if defined(__cplusplus)
static_assert(sizeof(ocaml_temporal_core_status) == 4,
              "bridge status must be exactly 32 bits");
static_assert(offsetof(ocaml_temporal_core_buffer, ptr) == 0,
              "buffer pointer must be the first field");
static_assert(offsetof(ocaml_temporal_core_buffer, len) == sizeof(void *),
              "buffer length layout mismatch");
static_assert(offsetof(ocaml_temporal_core_result, status) == 0,
              "result status must be the first field");
#else
_Static_assert(sizeof(ocaml_temporal_core_status) == 4,
               "bridge status must be exactly 32 bits");
_Static_assert(offsetof(ocaml_temporal_core_buffer, ptr) == 0,
               "buffer pointer must be the first field");
_Static_assert(offsetof(ocaml_temporal_core_buffer, len) == sizeof(void *),
               "buffer length layout mismatch");
_Static_assert(offsetof(ocaml_temporal_core_result, status) == 0,
               "result status must be the first field");
#endif

#endif /* OCAML_TEMPORAL_CORE_H */
