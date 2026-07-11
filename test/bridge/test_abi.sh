#!/bin/sh
set -eu

target_dir=${CARGO_TARGET_DIR:-_build/rust}
archive="$target_dir/debug/libocaml_temporal_core_bridge.a"
output_dir=_build/test/bridge
binary="$output_dir/abi_harness"

if [ ! -s "$archive" ]; then
  echo "missing Rust bridge archive: $archive" >&2
  exit 1
fi

mkdir -p "$output_dir"

cc \
  -std=c11 \
  -Wall \
  -Wextra \
  -Werror \
  -pedantic \
  -fsanitize=address,undefined \
  -fno-omit-frame-pointer \
  -Irust/core-bridge/include \
  test/bridge/abi_harness.c \
  "$archive" \
  -ldl \
  -lpthread \
  -lm \
  -o "$binary"

case "$(uname -s)" in
  Darwin) ASAN_OPTIONS=detect_leaks=0 "$binary" ;;
  *) ASAN_OPTIONS=detect_leaks=1 "$binary" ;;
esac
