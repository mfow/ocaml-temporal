#!/bin/sh
set -eu

root=$(pwd)
install_root="$root/_build/install/default/lib"
package_root="$install_root/temporal-sdk"
output_dir="$root/_build/test/install-smoke"
dune_install_root=$install_root
dune_output_dir=$output_dir
path_separator=:

case "$(uname -s)" in
  MINGW* | MSYS* | CYGWIN*)
    # opam supplies a native Windows Dune executable even though this script
    # runs under Cygwin. Convert paths at that process boundary only.
    dune_install_root=$(cygpath -w "$install_root")
    dune_output_dir=$(cygpath -w "$output_dir")
    path_separator=';'
    ;;
esac

opam exec -- dune build @install

test -s "$package_root/internal_core_bridge/libocaml_temporal_core_bridge.a"
test ! -e "$package_root/internal_core_bridge/ocaml_temporal_core.h"
test ! -e "$package_root/internal_core_bridge/abi.rs"
if find "$package_root" -iname '*mailbox*' -print | grep -q .; then
  echo "private mailbox processor leaked into the install tree" >&2
  exit 1
fi

mkdir -p "$output_dir"
cp "$root/test/fixtures/install-consumer/dune-project" "$output_dir/"
cp "$root/test/fixtures/install-consumer/dune" "$output_dir/"
cp "$root/test/fixtures/install-consumer/main.ml" "$output_dir/"
consumer_ocamlpath=$dune_install_root
if [ -n "${OCAMLPATH:-}" ]; then
  consumer_ocamlpath="${dune_install_root}${path_separator}${OCAMLPATH}"
fi
OCAMLPATH="$consumer_ocamlpath" opam exec -- \
  dune build --root "$dune_output_dir" ./main.exe
"$output_dir/_build/default/main.exe"
