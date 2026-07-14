#!/bin/sh
set -eu

# Build temporary opam manifests to verify the repository's narrow license
# allow-list. The fixtures cover ordinary approval, missing/prohibited
# licenses, and the two version-scoped OCaml linking exceptions without
# mutating the checked-in dependency inventory.
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

printf '%s\n' 'opam-version: "2.0"' 'name: "bad"' 'license: "GPL-3.0-only"' >"$tmp/bad.opam"
if scripts/check-licenses.sh "$tmp/bad.opam"; then
  echo "GPL fixture was incorrectly accepted" >&2
  exit 1
fi

printf '%s\n' 'opam-version: "2.0"' 'name: "good"' 'license: "MIT"' >"$tmp/good.opam"
scripts/check-licenses.sh "$tmp/good.opam"

printf '%s\n' 'opam-version: "2.0"' 'name: "missing"' >"$tmp/missing.opam"
if scripts/check-licenses.sh "$tmp/missing.opam"; then
  echo "missing license was incorrectly accepted" >&2
  exit 1
fi

printf '%s\n' \
  'opam-version: "2.0"' \
  'name: "unreviewed"' \
  'license: "LGPL-2.1-or-later WITH OCaml-LGPL-linking-exception"' \
  >"$tmp/unreviewed.opam"
if scripts/check-licenses.sh "$tmp/unreviewed.opam"; then
  echo "unreviewed linking exception was incorrectly accepted" >&2
  exit 1
fi

printf '%s\n' \
  'opam-version: "2.0"' \
  'name: "ocamlbuild"' \
  'version: "0.16.1"' \
  'license: "LGPL-2.0-or-later WITH OCaml-LGPL-linking-exception"' \
  >"$tmp/ocamlbuild.opam"
scripts/check-licenses.sh "$tmp/ocamlbuild.opam"

printf '%s\n' \
  'opam-version: "2.0"' \
  'name: "ocamlbuild"' \
  'version: "0.16.0"' \
  'license: "LGPL-2.0-or-later WITH OCaml-LGPL-linking-exception"' \
  >"$tmp/old-ocamlbuild.opam"
if scripts/check-licenses.sh "$tmp/old-ocamlbuild.opam"; then
  echo "unreviewed ocamlbuild version was incorrectly accepted" >&2
  exit 1
fi

printf '%s\n' \
  'opam-version: "2.0"' \
  'name: "mixed"' \
  'license: [' \
  '  "MIT"' \
  '  "GPL-3.0-only"' \
  ']' \
  >"$tmp/mixed.opam"
if scripts/check-licenses.sh "$tmp/mixed.opam"; then
  echo "mixed prohibited license was incorrectly accepted" >&2
  exit 1
fi
