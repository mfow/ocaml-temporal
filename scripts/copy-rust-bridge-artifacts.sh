#!/bin/sh
set -eu

# Dune supplies explicit destinations for the Rust static and dynamic bridge
# artifacts. Cargo names the dynamic library differently on each host, so
# resolve that source here while keeping the caller-owned output paths stable.
# The separate copies also preserve the static archive and shared library as
# distinct build products for the platform-specific link step.
target_dir=$1
static_output=$2
dynamic_output=$3

case "$(uname -s)" in
  Darwin) dynamic_source="$target_dir/libocaml_temporal_core_bridge.dylib" ;;
  MINGW* | MSYS* | CYGWIN*) dynamic_source="$target_dir/ocaml_temporal_core_bridge.dll" ;;
  *) dynamic_source="$target_dir/libocaml_temporal_core_bridge.so" ;;
esac

cp "$target_dir/libocaml_temporal_core_bridge.a" "$static_output"
cp "$dynamic_source" "$dynamic_output"
