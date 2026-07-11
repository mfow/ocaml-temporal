#!/bin/sh
set -eu

expected_rust_version=1.94.1

actual_rust_version=$(rustc --version | awk '{ print $2 }')
if [ "$actual_rust_version" != "$expected_rust_version" ]; then
  echo "expected rustc $expected_rust_version, got $actual_rust_version" >&2
  exit 1
fi

cargo --version >/dev/null
cargo build --manifest-path rust/Cargo.toml --locked

archive=_build/rust/debug/libocaml_temporal_core_bridge.a
if [ ! -s "$archive" ]; then
  echo "expected non-empty Rust static library at $archive" >&2
  exit 1
fi
