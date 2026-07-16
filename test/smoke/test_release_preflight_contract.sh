#!/bin/sh
set -eu

# The release checker itself is the source-only contract.  This wrapper keeps
# its Makefile invocation explicit and verifies the script remains portable
# POSIX shell before CI uses it on a clean checkout.
root=${1:-.}
cd "$root"
sh -n scripts/check-release-preflight.sh
if [ -n "$(git status --porcelain --untracked-files=all)" ]; then
  echo "release preflight contract requires a clean checkout" >&2
  exit 1
fi
sh scripts/check-release-preflight.sh .
