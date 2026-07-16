#!/bin/sh
set -eu

root=$(pwd)
install_root="$root/_build/install/default/lib"
package_root="$install_root/temporal-sdk"
private_root="$package_root/__private__"
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

# The wrapped [Temporal] root is the installed package's explicit public
# module allow-list. Keep this check next to the consumer compilation so a
# newly exposed implementation module or an accidentally removed public
# module fails the package-boundary gate before publication. The comparison is
# intentionally source-based: the installed tree contains generated aliases,
# while the checked-in root is the maintainer-owned list.
expected_public_modules='Activity Child_workflow Client Codec Condition Duration Error Future Interaction Payload Query Result_syntax Runtime_info Scope Time Signal Update Worker Workflow Workflow_context'
actual_public_modules=$(sed -n 's/^module \([A-Za-z0-9_]*\) = .*/\1/p' \
  "$root/lib/public/temporal.ml" | tr '\n' ' ' | sed 's/[[:space:]]*$//')
if [ "$actual_public_modules" != "$expected_public_modules" ]; then
  echo "public Temporal module allow-list changed unexpectedly" >&2
  printf 'expected: %s\nactual:   %s\n' \
    "$expected_public_modules" "$actual_public_modules" >&2
  exit 1
fi

# Dune's `(package temporal-sdk)` mechanism installs implementation libraries
# only below `temporal-sdk/__private__/`. This is package-private linkage, not a
# top-level findlib package or a module in the public `Temporal` signature.
test -s "$private_root/temporal_core_bridge/libocaml_temporal_core_bridge.a"
test ! -e "$private_root/temporal_core_bridge/ocaml_temporal_core.h"
test ! -e "$private_root/temporal_core_bridge/abi.rs"
for private_library in \
  temporal_base \
  temporal_core_bridge \
  temporal_future_kernel \
  temporal_mailbox_processor \
  temporal_protocol \
  temporal_runtime \
  temporal_sdk_supervisor; do
  test -d "$private_root/$private_library"
  test ! -e "$package_root/$private_library"
done

# The artifact layout check above protects against accidental installation in
# the wrong directory. These separate consumer compilations protect the more
# important API contract: a normal `(libraries temporal-sdk)` consumer cannot
# import the mailbox, supervisor, or C/Rust bridge module by name.
if find "$package_root" -path "$private_root" -prune \
  -o -iname '*mailbox*' -print | grep -q .; then
  echo "private mailbox processor leaked outside Dune's package-private tree" >&2
  exit 1
fi
if find "$package_root" -path "$private_root" -prune \
  -o -iname '*supervisor*' -print | grep -q .; then
  echo "private SDK supervisor leaked outside Dune's package-private tree" >&2
  exit 1
fi
if find "$package_root" -path "$private_root" -prune \
  -o -iname '*core_bridge*' -print | grep -q .; then
  echo "private C/Rust bridge leaked outside Dune's package-private tree" >&2
  exit 1
fi

mkdir -p "$output_dir"
cp "$root/test/fixtures/install-consumer/dune-project" "$output_dir/"
cp "$root/test/fixtures/install-consumer/dune" "$output_dir/"
cp "$root/test/fixtures/install-consumer/main.ml" "$output_dir/"
cp "$root/test/fixtures/install-consumer/public_api.ml" "$output_dir/"
cp "$root/test/fixtures/install-consumer/negative-dune" "$output_dir/dune-negative"
cp "$root/test/fixtures/install-consumer/negative/forbidden_"*.ml "$output_dir/"
consumer_ocamlpath=$dune_install_root
if [ -n "${OCAMLPATH:-}" ]; then
  consumer_ocamlpath="${dune_install_root}${path_separator}${OCAMLPATH}"
fi
OCAMLPATH="$consumer_ocamlpath" opam exec -- \
  dune build --root "$dune_output_dir" ./main.exe
"$output_dir/_build/default/main.exe"

# Activate the negative stanzas only after the positive consumer has built. The
# source tree keeps them in a non-Dune template so the repository's normal
# `dune build` does not intentionally fail while compiling these fixtures.
cp "$output_dir/dune-negative" "$output_dir/dune"
for forbidden_target in \
  forbidden_mailbox \
  forbidden_supervisor \
  forbidden_bridge \
  forbidden_base \
  forbidden_runtime \
  forbidden_protocol \
  forbidden_native_worker \
  forbidden_backend \
  forbidden_future_kernel; do
  if OCAMLPATH="$consumer_ocamlpath" opam exec -- \
    dune build --root "$dune_output_dir" \
      "./$forbidden_target.exe" >"$output_dir/$forbidden_target.log" 2>&1; then
    echo "$forbidden_target unexpectedly compiled from the public package" >&2
    cat "$output_dir/$forbidden_target.log" >&2
    exit 1
  fi
done
