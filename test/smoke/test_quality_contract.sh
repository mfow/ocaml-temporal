#!/bin/sh
set -eu

# Dune intentionally ignores hidden source directories, so this small native
# contract test covers the GitHub workflow that the OCaml repository test
# cannot safely read from its sandbox.
source_root=${1:-.}
workflow=$source_root/.github/workflows/build.yml

# Anchor the job declaration at the workflow's two-space indentation so a
# future matrix step named "quality" cannot satisfy this contract by accident.
grep -Fq '  quality:' "$workflow"
grep -Fq 'name: Quality and security scans' "$workflow"
grep -Fq \
  'taiki-e/install-action@2ca9b94c269419b7b0c711c09d0b21c4e1d51145' \
  "$workflow"
grep -Fq \
  'cargo-deny@0.20.2,cargo-machete@0.9.2,typos@1.48.0' \
  "$workflow"
grep -Fq 'run: make quality' "$workflow"
