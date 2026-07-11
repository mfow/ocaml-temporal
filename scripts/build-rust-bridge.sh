#!/bin/sh
set -eu

workspace_root=$1
static_output=$2
dynamic_output=$3
link_flags_output=$4
target_root=${CARGO_TARGET_DIR:-"$workspace_root/rust/target"}
artifact_root=$target_root

case "$(uname -s)" in
  MINGW* | MSYS* | CYGWIN*)
    if command -v cygpath >/dev/null 2>&1; then
      artifact_root=$(cygpath -u "$target_root")
    fi
    ;;
esac

cargo build \
  --manifest-path "$workspace_root/rust/Cargo.toml" \
  --package ocaml-temporal-core-bridge \
  --locked

native_link_output=$(mktemp)
trap 'rm -f "$native_link_output"' EXIT HUP INT TERM

if ! CARGO_TERM_COLOR=never cargo rustc \
  --manifest-path "$workspace_root/rust/Cargo.toml" \
  --package ocaml-temporal-core-bridge \
  --locked \
  --lib \
  --crate-type staticlib \
  -- \
  --print=native-static-libs \
  2>"$native_link_output"
then
  cat "$native_link_output" >&2
  exit 1
fi

native_link_flags=$(sed -n 's/^note: native-static-libs: //p' "$native_link_output" | tail -n 1)
if [ -z "$native_link_flags" ]; then
  cat "$native_link_output" >&2
  echo "rustc did not report native static-library link flags" >&2
  exit 1
fi

# rustc owns the platform-specific library list and its ordering. On Windows,
# also preserve the Cargo build-script search path needed to resolve winapi's
# bundled MinGW import archives from OCaml's foreign linker.
sh "$workspace_root/scripts/render-rust-link-flags.sh" \
  "$(uname -s)" \
  "$artifact_root" \
  "$link_flags_output" \
  "$native_link_flags"

"$workspace_root/scripts/copy-rust-bridge-artifacts.sh" \
  "$artifact_root/debug" "$static_output" "$dynamic_output"
