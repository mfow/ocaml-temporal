#!/bin/sh
set -eu

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
