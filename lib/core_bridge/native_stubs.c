#include "ocaml_temporal_core.h"

#include <caml/alloc.h>
#include <caml/custom.h>
#include <caml/fail.h>
#include <caml/memory.h>
#include <caml/mlvalues.h>
#include <caml/threads.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct owned_response {
  ocaml_temporal_core_result result;
  int live;
} owned_response;

static owned_response *Response_val(value response) {
  return (owned_response *)Data_custom_val(response);
}

static void release_response(owned_response *response) {
  if (response->live) {
    (void)ocaml_temporal_core_v1_result_free(&response->result);
    response->live = 0;
  }
}

static void finalize_response(value response) {
  release_response(Response_val(response));
}

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

static owned_response *require_live(value response) {
  owned_response *owned = Response_val(response);
  if (!owned->live) {
    caml_invalid_argument("Temporal native response has already been freed");
  }
  return owned;
}

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

CAMLprim value ocaml_temporal_echo(value input) {
  CAMLparam1(input);
  CAMLlocal1(response);
  size_t input_length = caml_string_length(input);
  uint8_t *input_copy = NULL;
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
  (void)ocaml_temporal_core_v1_echo(input_copy, input_length, &owned->result);
  caml_leave_blocking_section();
  free(input_copy);

  CAMLreturn(response);
}

CAMLprim value ocaml_temporal_conformance_wait_ms(value milliseconds) {
  CAMLparam1(milliseconds);
  CAMLlocal1(response);
  intnat requested = Long_val(milliseconds);
  uint32_t bounded_request;
  owned_response *owned;

  bounded_request =
      requested < 0 || (uintnat)requested > UINT32_MAX ? UINT32_MAX
                                                       : (uint32_t)requested;

  response = alloc_response();
  owned = Response_val(response);
  caml_enter_blocking_section();
  (void)ocaml_temporal_core_v1_conformance_wait_ms(bounded_request,
                                                   &owned->result);
  caml_leave_blocking_section();
  CAMLreturn(response);
}

CAMLprim value ocaml_temporal_response_status(value response) {
  CAMLparam1(response);
  owned_response *owned = require_live(response);
  CAMLreturn(Val_int(owned->result.status));
}

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

CAMLprim value ocaml_temporal_response_free(value response) {
  CAMLparam1(response);
  release_response(Response_val(response));
  CAMLreturn(Val_unit);
}
