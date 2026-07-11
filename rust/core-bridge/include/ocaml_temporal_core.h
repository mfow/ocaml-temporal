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
  OCAML_TEMPORAL_CORE_STATUS_INTERNAL = 4
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
 * Destroy a runtime and clear the caller's slot. Calling this again with the
 * same now-null slot is safe. Child worker/client handles must already be shut
 * down before their owning runtime is released.
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
