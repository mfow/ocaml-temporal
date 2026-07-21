#!/bin/sh
set -eu

# A release tag is an immutable claim about the package inputs. Keep this
# check independent of opam, Dune, Docker, and Rust so it can run before any
# expensive build and fail before a mismatched archive is published.
root=${1:-.}
tag=${2:-${RELEASE_TAG:-}}
cd "$root"

fail() {
  echo "release tag check: $*" >&2
  exit 1
}

[ -n "$tag" ] || fail "provide a tag as the second argument or RELEASE_TAG"
case "$tag" in
  v*) ;;
  *) fail "tag must start with v (got $tag)" ;;
esac

# Accept three numeric components plus an optional SemVer-style prerelease
# suffix. The suffix is useful for tags such as v1.0.0-beta.1 while keeping
# floating, malformed, and development tags out of the release path.
version=${tag#v}
core=$version
prerelease=
case "$version" in
  *-*)
    core=${version%%-*}
    prerelease=${version#*-}
    case "$prerelease" in
      '' | .* | *. | *..* | *[!A-Za-z0-9.-]*)
        fail "tag has an invalid prerelease suffix (got $tag)" ;;
    esac
    ;;
esac
case "$core" in
  *[!0-9.]* | .* | *. | *..*) fail "tag must be vMAJOR.MINOR.PATCH (got $tag)" ;;
esac
old_ifs=$IFS
IFS=.
set -- $core
IFS=$old_ifs
[ "$#" -eq 3 ] || fail "tag must be vMAJOR.MINOR.PATCH (got $tag)"
for component in "$@"; do
  [ -n "$component" ] || fail "tag contains an empty version component"
done

[ -f .release-version ] || fail "missing .release-version"
release_version=$(sed 's/\r$//' .release-version)
[ "$release_version" = "$version" ] ||
  fail ".release-version is $release_version, but tag is $version"
[ "$release_version" != "~dev" ] || fail "development version cannot be tagged"

[ -f temporal-sdk.opam ] || fail "missing temporal-sdk.opam"
[ -f temporal-sdk.opam.locked ] || fail "missing temporal-sdk.opam.locked"

opam_field() {
  sed -n "s/^$1:[[:space:]]*\"\([^\"]*\)\"[[:space:]]*$/\1/p" \
    temporal-sdk.opam | head -n 1
}
locked_field() {
  sed -n "s/^$1:[[:space:]]*\"\([^\"]*\)\"[[:space:]]*$/\1/p" \
    temporal-sdk.opam.locked | head -n 1
}
[ "$(opam_field name)" = temporal-sdk ] || fail "opam package name differs"
[ "$(opam_field version)" = "$version" ] || fail "opam version differs"
[ "$(locked_field name)" = temporal-sdk ] || fail "locked package name differs"
[ "$(locked_field version)" = "$version" ] || fail "locked package version differs"

printf '%s\n' "release tag check: ok ($tag -> temporal-sdk $version)"
