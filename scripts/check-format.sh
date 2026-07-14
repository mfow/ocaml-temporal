#!/bin/sh
set -eu

# This is the repository-wide whitespace gate used before the formatter and
# CI quality jobs. Git grep intentionally searches tracked text only, so an
# ignored build artifact cannot hide a source failure or make this gate noisy.
# A match is reported with its path and line number before the script fails.
if git grep -nI -E '[[:blank:]]$' -- .; then
  echo "tracked files contain trailing whitespace" >&2
  exit 1
fi
