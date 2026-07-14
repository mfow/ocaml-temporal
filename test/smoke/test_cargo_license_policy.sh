#!/bin/sh
set -eu

# Exercise both sides of the Cargo license policy with the same scanner used
# by CI. The denied fixture must name every prohibited or malformed package;
# checking those diagnostics prevents a parser branch from becoming a silent
# allow or an unreviewed rejection.
scanner=scripts/check-cargo-licenses.py
fixtures=test/fixtures/cargo-licenses
python=${PYTHON:-python3}

"$python" "$scanner" --metadata "$fixtures/allowed.json" >/dev/null

if output=$("$python" "$scanner" --metadata "$fixtures/denied.json" 2>&1); then
  echo "expected prohibited Cargo license fixture to fail" >&2
  exit 1
fi

for package in gpl lgpl agpl mpl missing unknown bad-expression; do
  if ! printf '%s\n' "$output" | grep -q " $package "; then
    echo "expected rejection output for $package" >&2
    exit 1
  fi
done
