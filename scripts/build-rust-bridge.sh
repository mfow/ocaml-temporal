#!/bin/sh
set -eu

workspace_root=$1
static_output=$2
dynamic_output=$3
target_root=${CARGO_TARGET_DIR:-"$workspace_root/rust/target"}

cargo build \
  --manifest-path "$workspace_root/rust/Cargo.toml" \
  --package ocaml-temporal-core-bridge \
  --locked

"$workspace_root/scripts/copy-rust-bridge-artifacts.sh" \
  "$target_root/debug" "$static_output" "$dynamic_output"
