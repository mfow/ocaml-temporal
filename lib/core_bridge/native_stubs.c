#include "ocaml_temporal_core.h"

#include <caml/alloc.h>
#include <caml/custom.h>
#include <caml/fail.h>
#include <caml/memory.h>
#include <caml/mlvalues.h>
#include <caml/threads.h>

#include <stdint.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

/* OCaml custom block that is the sole owner of one initialized Rust result.
 * `live` makes explicit free and finalization idempotent with respect to the
 * same OCaml value. Copying this C structure remains forbidden. */
typedef struct owned_response {
  ocaml_temporal_core_result result;
  int live;
} owned_response;

/* OCaml custom block owning the sole native runtime pointer for one SDK
 * instance. Future client and worker state remains subordinate to this owner. */
typedef struct owned_runtime {
  _Atomic(ocaml_temporal_core_runtime *) runtime;
} owned_runtime;

/* Extract the custom-block payload; callers must separately require liveness
 * before reading Rust-owned pointer fields. */
static owned_response *Response_val(value response) {
  return (owned_response *)Data_custom_val(response);
}

/* Extract the private runtime owner stored in an OCaml custom block. */
static owned_runtime *Runtime_val(value runtime) {
  return (owned_runtime *)Data_custom_val(runtime);
}

/* Release Rust allocations exactly once and poison further field access. */
static void release_response(owned_response *response) {
  if (response->live) {
    (void)ocaml_temporal_core_v1_result_free(&response->result);
    response->live = 0;
  }
}

/* GC fallback for responses not explicitly decoded because OCaml unwound. */
static void finalize_response(value response) {
  release_response(Response_val(response));
}

/* GC fallback for a runtime whose supervisor did not complete explicit
 * shutdown. Normal operation closes the owner deterministically first. */
static void finalize_runtime(value runtime) {
  owned_runtime *owned = Runtime_val(runtime);
  ocaml_temporal_core_runtime *native_runtime =
      atomic_exchange_explicit(&owned->runtime, NULL, memory_order_acq_rel);
  if (native_runtime != NULL) {
    (void)ocaml_temporal_core_v1_runtime_dispose(&native_runtime);
  }
}

/* Custom operations deliberately use identity/default behavior; responses are
 * resource owners, not serializable or semantically comparable values. */
static struct custom_operations response_operations = {
    .identifier = "org.ocaml-temporal.native-response.v1",
    .finalize = finalize_response,
    .compare = custom_compare_default,
    .hash = custom_hash_default,
    .serialize = custom_serialize_default,
    .deserialize = custom_deserialize_default,
    .compare_ext = custom_compare_ext_default,
    .fixed_length = NULL,
};

/* Runtime owners are identity resources and cannot be serialized or compared
 * by native contents. */
static struct custom_operations runtime_operations = {
    .identifier = "org.ocaml-temporal.native-runtime.v1",
    .finalize = finalize_runtime,
    .compare = custom_compare_default,
    .hash = custom_hash_default,
    .serialize = custom_serialize_default,
    .deserialize = custom_deserialize_default,
    .compare_ext = custom_compare_ext_default,
    .fixed_length = NULL,
};

/* Allocate and initialize the finalizable owner before any Rust call can write
 * allocations into it. Zeroing also satisfies the Rust result precondition. */
static value alloc_response(void) {
  CAMLparam0();
  CAMLlocal1(response);
  owned_response *owned;

  response = caml_alloc_custom(&response_operations, sizeof(owned_response), 0, 1);
  owned = Response_val(response);
  memset(owned, 0, sizeof(*owned));
  owned->live = 1;
  CAMLreturn(response);
}

/* Allocate a null-initialized runtime owner before entering native code so a
 * later OCaml allocation failure cannot orphan a successfully created handle. */
static value alloc_runtime(void) {
  CAMLparam0();
  CAMLlocal1(runtime);
  owned_runtime *owned;

  runtime = caml_alloc_custom(&runtime_operations, sizeof(owned_runtime), 0, 1);
  owned = Runtime_val(runtime);
  atomic_init(&owned->runtime, NULL);
  CAMLreturn(runtime);
}

/* Reject use-after-free deterministically at the private binding boundary. */
static owned_response *require_live(value response) {
  owned_response *owned = Response_val(response);
  if (!owned->live) {
    caml_invalid_argument("Temporal native response has already been freed");
  }
  return owned;
}

/* Negotiate ABI compatibility without blocking; the returned custom block owns
 * any diagnostic allocated by Rust. */
CAMLprim value ocaml_temporal_check_abi_version(value requested_version) {
  CAMLparam1(requested_version);
  CAMLlocal1(response);
  owned_response *owned;

  response = alloc_response();
  owned = Response_val(response);
  (void)ocaml_temporal_core_v1_check_abi_version(
      (uint32_t)Int32_val(requested_version), &owned->result);
  CAMLreturn(response);
}

/* Copy mutable OCaml bytes before releasing the runtime lock. Rust never holds
 * an OCaml heap pointer while another Domain or the GC can run. */
CAMLprim value ocaml_temporal_echo(value input) {
  CAMLparam1(input);
  CAMLlocal1(response);
  size_t input_length = caml_string_length(input);
  uint8_t *input_copy = NULL;
  ocaml_temporal_core_result native_result = {0};
  owned_response *owned;

  /* Allocate the finalizable owner before acquiring any unmanaged resource. */
  response = alloc_response();
  owned = Response_val(response);

  if (input_length > 0) {
    input_copy = malloc(input_length);
    if (input_copy == NULL) {
      caml_raise_out_of_memory();
    }
    memcpy(input_copy, Bytes_val(input), input_length);
  }

  caml_enter_blocking_section();
  (void)ocaml_temporal_core_v1_echo(input_copy, input_length, &native_result);
  caml_leave_blocking_section();
  free(input_copy);
  owned->result = native_result;

  CAMLreturn(response);
}

/* Exercise a blocking Rust call with the OCaml runtime lock released. The
 * sentinel UINT32_MAX preserves invalid negative/overflow input for Rust-side
 * validation instead of truncating it into a valid duration. */
CAMLprim value ocaml_temporal_conformance_wait_ms(value milliseconds) {
  CAMLparam1(milliseconds);
  CAMLlocal1(response);
  intnat requested = Long_val(milliseconds);
  uint32_t bounded_request;
  ocaml_temporal_core_result native_result = {0};
  owned_response *owned;

  bounded_request =
      requested < 0 || (uintnat)requested > UINT32_MAX ? UINT32_MAX
                                                       : (uint32_t)requested;

  response = alloc_response();
  owned = Response_val(response);
  caml_enter_blocking_section();
  (void)ocaml_temporal_core_v1_conformance_wait_ms(bounded_request,
                                                   &native_result);
  caml_leave_blocking_section();
  owned->result = native_result;
  CAMLreturn(response);
}

/* Create Core/Tokio while the OCaml runtime lock is released. Rust writes only
 * C-stack storage during the blocking section; the opaque pointer and result
 * are copied into rooted custom blocks after reacquiring the lock. */
CAMLprim value ocaml_temporal_runtime_create(value unit) {
  CAMLparam1(unit);
  CAMLlocal3(runtime, response, pair);
  ocaml_temporal_core_runtime *native_runtime = NULL;
  ocaml_temporal_core_result native_result = {0};
  owned_runtime *runtime_owner;
  owned_response *response_owner;

  runtime = alloc_runtime();
  response = alloc_response();

  caml_enter_blocking_section();
  (void)ocaml_temporal_core_v1_runtime_new(&native_runtime, &native_result);
  caml_leave_blocking_section();

  runtime_owner = Runtime_val(runtime);
  atomic_store_explicit(&runtime_owner->runtime, native_runtime,
                        memory_order_release);
  response_owner = Response_val(response);
  response_owner->result = native_result;

  pair = caml_alloc_tuple(2);
  Store_field(pair, 0, runtime);
  Store_field(pair, 1, response);
  CAMLreturn(pair);
}

/* Atomically detach the pointer while holding the OCaml lock, then destroy
 * Core/Tokio without blocking another Domain. A second close observes null. */
CAMLprim value ocaml_temporal_runtime_close(value runtime) {
  CAMLparam1(runtime);
  owned_runtime *owned = Runtime_val(runtime);
  ocaml_temporal_core_runtime *native_runtime =
      atomic_exchange_explicit(&owned->runtime, NULL, memory_order_acq_rel);
  ocaml_temporal_core_status status;

  caml_enter_blocking_section();
  status = ocaml_temporal_core_v1_runtime_free(&native_runtime);
  caml_leave_blocking_section();
  CAMLreturn(Val_int(status));
}

/* Read the status while the custom block remains live. */
CAMLprim value ocaml_temporal_response_status(value response) {
  CAMLparam1(response);
  owned_response *owned = require_live(response);
  CAMLreturn(Val_int(owned->result.status));
}

/* Copy successful Rust bytes into GC-owned OCaml storage before cleanup. */
CAMLprim value ocaml_temporal_response_value(value response) {
  CAMLparam1(response);
  CAMLlocal1(output);
  owned_response *owned = require_live(response);

  if (owned->result.status != OCAML_TEMPORAL_CORE_STATUS_OK) {
    caml_invalid_argument("Temporal native response does not contain a value");
  }
  output = caml_alloc_initialized_string(owned->result.value.len,
                                         (const char *)owned->result.value.ptr);
  CAMLreturn(output);
}

/* Copy failure diagnostics into GC-owned OCaml storage before cleanup. */
CAMLprim value ocaml_temporal_response_error(value response) {
  CAMLparam1(response);
  CAMLlocal1(output);
  owned_response *owned = require_live(response);

  if (owned->result.status == OCAML_TEMPORAL_CORE_STATUS_OK) {
    caml_invalid_argument("Temporal native response does not contain an error");
  }
  output = caml_alloc_initialized_string(owned->result.error.len,
                                         (const char *)owned->result.error.ptr);
  CAMLreturn(output);
}

/* Explicit cleanup used by [Fun.protect]; finalization remains a fallback. */
CAMLprim value ocaml_temporal_response_free(value response) {
  CAMLparam1(response);
  release_response(Response_val(response));
  CAMLreturn(Val_unit);
}
