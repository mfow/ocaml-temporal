#!/bin/sh
set -eu

root=$(pwd)
install_root="$root/_build/install/default/lib"
package_root="$install_root/temporal"
output_dir="$root/_build/test/install-smoke"

opam exec -- dune build @install

test -s "$package_root/internal_core_bridge/libocaml_temporal_core_bridge.a"
test ! -e "$package_root/internal_core_bridge/ocaml_temporal_core.h"
test ! -e "$package_root/internal_core_bridge/abi.rs"

mkdir -p "$output_dir"
cp "$root/test/fixtures/install-consumer/dune-project" "$output_dir/"
cp "$root/test/fixtures/install-consumer/dune" "$output_dir/"
cp "$root/test/fixtures/install-consumer/main.ml" "$output_dir/"
OCAMLPATH="$install_root${OCAMLPATH:+:$OCAMLPATH}" opam exec -- \
  dune build --root "$output_dir" ./main.exe
"$output_dir/_build/default/main.exe"
