#!/bin/sh
set -eu

# Exercises the source-contract helper independently of the large Temporal
# fixture. The adversarial binding below is deliberately wrong while a later,
# unrelated binding contains the requested tokens; a whole-file search would
# therefore pass incorrectly.
root=${1:-.}
helper="$root/test/smoke/source_contract_helpers.sh"

if [ ! -r "$helper" ]; then
  echo "source-contract helper test is missing: $helper" >&2
  exit 1
fi

tmp=$(mktemp -d "${TMPDIR:-/tmp}/ocaml-temporal-source-contract.XXXXXX")

# Removes only the private fixture directory created by this test. Traps cover
# ordinary completion, assertion failure, and interruption.
cleanup() {
  rm -rf "$tmp"
}
trap cleanup EXIT HUP INT TERM

fixture="$tmp/bindings.ml"
printf '%s\n' \
  'let target_policy =' \
  '  make' \
  '    ~maximum_attempts:1 ()' \
  '' \
  '(** The next binding must not satisfy the target policy contract. *)' \
  'let unrelated_policy =' \
  '  make' \
  '    ~maximum_attempts:2 ()' >"$fixture"

. "$helper"

# The positive path accepts formatter-introduced newlines inside the named
# binding while preserving token order.
require_ocaml_binding_tokens "$fixture" target_policy \
  'make ~maximum_attempts:1 ()' \
  'source-contract helper test'

# The expected two-attempt sequence exists in the file but only outside the
# target binding. Acceptance here would reintroduce the cross-policy false
# positive that this helper was created to prevent.
if (require_ocaml_binding_tokens "$fixture" target_policy \
  'make ~maximum_attempts:2 ()' 'source-contract helper test') \
  >/dev/null 2>&1; then
  echo "source-contract helper accepted tokens from an unrelated binding" >&2
  exit 1
fi

# A missing binding must fail even if the requested tokens occur elsewhere.
if (require_ocaml_binding_tokens "$fixture" missing_policy \
  'make ~maximum_attempts:2 ()' 'source-contract helper test') \
  >/dev/null 2>&1; then
  echo "source-contract helper accepted a missing binding" >&2
  exit 1
fi

echo "source-contract helper test: ok"
