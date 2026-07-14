#!/bin/sh
set -eu

# Dune intentionally ignores hidden source directories, so this small native
# contract test covers the GitHub workflow that the OCaml repository test
# cannot safely read from its sandbox.
source_root=${1:-.}
master_workflow=$source_root/.github/workflows/build.yml
pr_workflow=$source_root/.github/workflows/build-pr.yml

# Anchor the job declaration at the workflow's two-space indentation so a
# future matrix step named "quality" cannot satisfy this contract by accident.
grep -Fq '  quality:' "$master_workflow"
grep -Fq 'name: Quality and security scans' "$master_workflow"
grep -Fq \
  'taiki-e/install-action@2ca9b94c269419b7b0c711c09d0b21c4e1d51145' \
  "$master_workflow"
grep -Fq \
  'cargo-deny@0.20.2,cargo-machete@0.9.2,typos@1.48.0' \
  "$master_workflow"
grep -Fq 'run: make quality' "$master_workflow"

# PR quality uses the same pinned tools, but is conditional so Markdown-only
# changes can use the inexpensive independent license job.
pr_quality=$(sed -n '/^  quality:/,/^  license-audit:/p' "$pr_workflow")
printf '%s\n' "$pr_quality" | grep -Fqx "    if: needs.changes.outputs.code == 'true'"
printf '%s\n' "$pr_quality" | grep -Fqx '    name: Quality and security scans'
printf '%s\n' "$pr_quality" |
  grep -Fq 'taiki-e/install-action@2ca9b94c269419b7b0c711c09d0b21c4e1d51145'
printf '%s\n' "$pr_quality" |
  grep -Fq 'cargo-deny@0.20.2,cargo-machete@0.9.2,typos@1.48.0'
printf '%s\n' "$pr_quality" | grep -Fqx '        run: make quality'

# Master and scheduled builds are the exhaustive compatibility gate. Scope
# every assertion to verify so a comment or another job cannot satisfy it.
master_verify=$(sed -n '/^  verify:/,/^    steps:/p' "$master_workflow")
for version in 5.2 5.3 5.4 5.5; do
  printf '%s\n' "$master_verify" | grep -Fqx "          - \"$version\""
done
master_version_count=$(printf '%s\n' "$master_verify" | grep -Fc '          - "5.')
test "$master_version_count" -eq 4
printf '%s\n' "$master_verify" | grep -Fqx '          - ubuntu-24.04'
printf '%s\n' "$master_verify" | grep -Fqx '          - ubuntu-24.04-arm'
master_native=$(sed -n '/^  native:/,$p' "$master_workflow")
printf '%s\n' "$master_native" | grep -Fqx '          - label: Windows x64'
printf '%s\n' "$master_native" | grep -Fqx '          - label: macOS ARM64'

# The PR matrix is deliberately an explicit three-lane include list rather
# than a Cartesian product: oldest/current amd64 and current ARM64. This keeps
# the compatibility floor, current release, and ARM build covered before
# merge, while avoiding five redundant Linux runner allocations.
pr_verify=$(sed -n '/^  verify:/,/^    steps:/p' "$pr_workflow")
# Check the adjacent OCaml/runner fields rather than merely their presence.
# That preserves the intended floor/current pairing if the list is reordered
# or a future edit accidentally assigns the compatibility floor to ARM64.
printf '%s\n' "$pr_verify" | awk '
  $0 == "          - ocaml: \"5.2\"" {
    if ((getline runner) > 0 && runner == "            runner: ubuntu-24.04") {
      floor_amd64 = 1
    }
  }
  $0 == "          - ocaml: \"5.5\"" {
    if ((getline runner) > 0) {
      if (runner == "            runner: ubuntu-24.04") current_amd64 = 1
      if (runner == "            runner: ubuntu-24.04-arm") current_arm64 = 1
    }
  }
  END { exit !(floor_amd64 && current_amd64 && current_arm64) }
'
lane_count=$(printf '%s\n' "$pr_verify" | grep -Fc '          - ocaml:')
test "$lane_count" -eq 3

# The faster macOS native job runs for each code PR. Windows is intentionally
# conditional on bridge/build/workflow inputs, so preserve both the
# changed-path output and its separate job instead of accidentally turning it
# into a permanently skipped or always-expensive matrix cell.
grep -Fq '      native_windows:' "$pr_workflow"
grep -Fq '          native_windows=false' "$pr_workflow"
grep -Fq '                native_windows=true' "$pr_workflow"
grep -Fq '  native-macos:' "$pr_workflow"
grep -Fq '  native-windows:' "$pr_workflow"
grep -Fq "if: needs.changes.outputs.native_windows == 'true'" "$pr_workflow"

# The always-on macOS lane proves the representative desktop native link. The
# Windows lane is deliberately conditional, but when selected it must retain
# the matching compiler, architecture, and native verification command.
pr_native_macos=$(sed -n '/^  native-macos:/,/^  native-windows:/p' "$pr_workflow")
printf '%s\n' "$pr_native_macos" | grep -Fqx "    if: needs.changes.outputs.code == 'true'"
printf '%s\n' "$pr_native_macos" | grep -Fqx '    name: OCaml 5.5 / macOS ARM64'
printf '%s\n' "$pr_native_macos" | grep -Fqx '    runs-on: macos-15'
printf '%s\n' "$pr_native_macos" | grep -Fqx '      NATIVE_ARCH: arm64'
printf '%s\n' "$pr_native_macos" | grep -Fqx '        run: make native-verify'
pr_native_windows=$(sed -n '/^  native-windows:/,$p' "$pr_workflow")
printf '%s\n' "$pr_native_windows" |
  grep -Fqx "    if: needs.changes.outputs.native_windows == 'true'"
printf '%s\n' "$pr_native_windows" | grep -Fqx '    name: OCaml 5.5 / Windows x64'
printf '%s\n' "$pr_native_windows" | grep -Fqx '    runs-on: windows-latest'
printf '%s\n' "$pr_native_windows" | grep -Fqx '      NATIVE_ARCH: amd64'
printf '%s\n' "$pr_native_windows" | grep -Fqx '        run: make native-verify'

# License audit is intentionally unconditional. The live smoke is conditional
# on source or fixture changes and runs both acceptance scenarios at OCaml 5.5.
pr_license=$(sed -n '/^  license-audit:/,/^  temporal-integration:/p' "$pr_workflow")
printf '%s\n' "$pr_license" | grep -Fqx '    name: Dependency license audit'
if printf '%s\n' "$pr_license" | grep -Fq '    if:'; then
  exit 1
fi
printf '%s\n' "$pr_license" |
  grep -Fqx '        run: make license-check OCAML_VERSION=5.2'
pr_smoke=$(sed -n '/^  temporal-integration:/,/^  verify:/p' "$pr_workflow")
printf '%s\n' "$pr_smoke" | grep -Fqx "    if: needs.changes.outputs.smoke == 'true'"
printf '%s\n' "$pr_smoke" |
  grep -Fqx '    name: Temporal/PostgreSQL integration smoke (OCaml 5.5)'
printf '%s\n' "$pr_smoke" | grep -Fqx '      OCAML_VERSION: "5.5"'
printf '%s\n' "$pr_smoke" | grep -Fqx '          make test-temporal-integration'
printf '%s\n' "$pr_smoke" | grep -Fqx '          make test-temporal-worker-restart'

# The JSON schemas are protocol fixtures rather than prose. Preserve their
# code classification while keeping ordinary Markdown-only changes inexpensive.
grep -Fq '              docs/schemas/*)' "$pr_workflow"
grep -Fq '              *.md|*.markdown|LICENSE*|NOTICE*|docs/*)' "$pr_workflow"
