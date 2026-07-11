#!/bin/sh
set -eu

failed=0

allowed_license() {
  package=$1
  version=$2
  license=$3

  case "$license" in
    MIT|Apache-2.0|BSD-2-Clause|BSD-3-Clause|ISC|Zlib|PostgreSQL)
      return 0
      ;;
    'LGPL-2.1-or-later WITH OCaml-LGPL-linking-exception')
      case "$package" in
        ocaml|ocaml-base-compiler|ocaml-compiler-libs) return 0 ;;
      esac
      ;;
    'CC0-1.0+')
      [ "$package" = "ocaml-options-vanilla" ] && [ "$version" = "1" ] && return 0
      ;;
  esac
  return 1
}

check_license_value() {
  package=$1
  version=$2
  value=$3

  if [ -z "$value" ]; then
    case "$package:$version" in
      base-bigarray:base|base-domains:base|base-nnp:base|base-threads:base|base-unix:base)
        echo "ALLOW $package $version compiler-virtual-package"
        return
        ;;
    esac
    echo "DENY  $package $version missing-license" >&2
    failed=1
    return
  fi

  expressions=$(printf '%s\n' "$value" | awk -F '"' '{ for (i = 2; i <= NF; i += 2) print $i }')
  if [ -z "$expressions" ]; then
    expressions=$(printf '%s\n' "$value" | awk -F ',' '{ for (i = 1; i <= NF; i++) { gsub(/^[[:space:]]+|[[:space:]]+$/, "", $i); print $i } }')
  fi
  old_ifs=$IFS
  IFS='
'
  for license in $expressions; do
    IFS=$old_ifs
    if allowed_license "$package" "$version" "$license"; then
      echo "ALLOW $package $version $license"
    else
      echo "DENY  $package $version $license" >&2
      failed=1
    fi
    IFS='
'
  done
  IFS=$old_ifs
}

check_opam_file() {
  file=$1
  case "$file" in
    */*) opam_file=$file ;;
    *) opam_file=./$file ;;
  esac
  package=$(opam show --just-file "$opam_file" --field=name 2>/dev/null || true)
  version=$(opam show --just-file "$opam_file" --field=version 2>/dev/null || true)
  [ -n "$package" ] || package=$(basename "$file" .opam)
  [ -n "$version" ] || version=fixture
  value=$(opam show --just-file "$opam_file" --field=license 2>/dev/null || true)
  check_license_value "$package" "$version" "$value"
}

if [ "$#" -gt 0 ]; then
  for file in "$@"; do
    check_opam_file "$file"
  done
else
  check_opam_file temporal.opam
  dependencies=$(sed -n 's/^  "\([^"]*\)" {= "\([^"]*\)".*/\1 \2/p' temporal.opam.locked)
  while read -r package version; do
    [ -n "$package" ] || continue
    value=$(opam show "$package.$version" --field=license 2>/dev/null || true)
    check_license_value "$package" "$version" "$value"
  done <<EOF
$dependencies
EOF
fi

exit "$failed"
