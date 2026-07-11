#include "ocaml_temporal_core.h"

#include <assert.h>
#include <stdint.h>
#include <string.h>

static void assert_empty(ocaml_temporal_core_buffer buffer) {
  assert(buffer.ptr == NULL);
  assert(buffer.len == 0);
}

int main(void) {
  ocaml_temporal_core_result result = {0};
  ocaml_temporal_core_runtime *runtime = NULL;

  assert(ocaml_temporal_core_v1_check_abi_version(
             OCAML_TEMPORAL_CORE_ABI_VERSION, &result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(result.status == OCAML_TEMPORAL_CORE_STATUS_OK);
  assert_empty(result.value);
  assert_empty(result.error);
  assert(ocaml_temporal_core_v1_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);

  assert(ocaml_temporal_core_v1_check_abi_version(
             OCAML_TEMPORAL_CORE_ABI_VERSION + 1, &result) ==
         OCAML_TEMPORAL_CORE_STATUS_ABI_MISMATCH);
  assert(result.status == OCAML_TEMPORAL_CORE_STATUS_ABI_MISMATCH);
  assert_empty(result.value);
  assert(result.error.ptr != NULL);
  assert(result.error.len > 0);
  assert(ocaml_temporal_core_v1_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);

  const uint8_t input[] = {0x00, 0x7f, 0x80, 0xff};
  assert(ocaml_temporal_core_v1_echo(input, sizeof(input), &result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(result.value.len == sizeof(input));
  assert(memcmp(result.value.ptr, input, sizeof(input)) == 0);
  assert_empty(result.error);
  assert(ocaml_temporal_core_v1_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(ocaml_temporal_core_v1_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert_empty(result.value);
  assert_empty(result.error);

  assert(ocaml_temporal_core_v1_echo(NULL, 0, &result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert_empty(result.value);
  assert(ocaml_temporal_core_v1_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);

  assert(ocaml_temporal_core_v1_echo(NULL, 1, &result) ==
         OCAML_TEMPORAL_CORE_STATUS_INVALID_ARGUMENT);
  assert(result.error.ptr != NULL);
  assert(result.error.len > 0);
  assert(ocaml_temporal_core_v1_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);

  assert(ocaml_temporal_core_v1_conformance_wait_ms(0, &result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(ocaml_temporal_core_v1_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);

  assert(ocaml_temporal_core_v1_runtime_new(&runtime, &result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(runtime != NULL);
  assert(ocaml_temporal_core_v1_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(ocaml_temporal_core_v1_runtime_free(&runtime) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);

  assert(ocaml_temporal_core_v1_runtime_new(&runtime, &result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(ocaml_temporal_core_v1_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(ocaml_temporal_core_v1_runtime_dispose(&runtime) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(runtime == NULL);
  assert(runtime == NULL);
  assert(ocaml_temporal_core_v1_runtime_free(&runtime) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);

  assert(ocaml_temporal_core_v1_check_abi_version(
             OCAML_TEMPORAL_CORE_ABI_VERSION, NULL) ==
         OCAML_TEMPORAL_CORE_STATUS_INVALID_ARGUMENT);
  assert(ocaml_temporal_core_v1_result_free(NULL) ==
         OCAML_TEMPORAL_CORE_STATUS_INVALID_ARGUMENT);
  assert(ocaml_temporal_core_v1_runtime_new(NULL, &result) ==
         OCAML_TEMPORAL_CORE_STATUS_INVALID_ARGUMENT);
  assert(ocaml_temporal_core_v1_result_free(&result) ==
         OCAML_TEMPORAL_CORE_STATUS_OK);
  assert(ocaml_temporal_core_v1_runtime_free(NULL) ==
         OCAML_TEMPORAL_CORE_STATUS_INVALID_ARGUMENT);

  return 0;
}
