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
# Dune's `(package temporal-sdk)` mechanism is the supported way for a public
# library to link a private implementation library. It installs that library
# only below `temporal-sdk/__private__/`; this is package-private linkage, not a
# top-level findlib package or a module in the public `Temporal` signature.
# Reject any mailbox/supervisor artifact outside that reserved subtree so a
# future stanza cannot accidentally publish the implementation as a dependency.
if find "$package_root" -path "$package_root/__private__" -prune \
  -o -iname '*mailbox*' -print | grep -q .; then
  echo "private mailbox processor leaked outside Dune's package-private tree" >&2
  exit 1
fi
if find "$package_root" -path "$package_root/__private__" -prune \
  -o -iname '*supervisor*' -print | grep -q .; then
  echo "private SDK supervisor leaked outside Dune's package-private tree" >&2
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
