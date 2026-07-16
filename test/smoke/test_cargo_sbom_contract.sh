#!/bin/sh
set -eu

# Cargo includes the checkout directory in path-package IDs. This contract
# proves that two clones of the same package graph receive the same SPDXID,
# while distinct semantic packages remain distinguishable.
root=${1:-.}
cd "$root"
python3 - scripts/generate-cargo-sbom.py <<'PY'
import importlib.util
import sys

spec = importlib.util.spec_from_file_location("sbom", sys.argv[1])
assert spec and spec.loader
sbom = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sbom)

def metadata(root):
    return {
        "workspace_root": root + "/rust",
        "packages": [
            {
                "id": "path+file://" + root + "/rust#fixture-0.1.0",
                "name": "fixture",
                "version": "0.1.0",
                "manifest_path": root + "/rust/core-bridge/Cargo.toml",
                "source": None,
            }
        ],
    }

left = sbom.make_document(metadata("/tmp/first-clone"))
right = sbom.make_document(metadata("/home/runner/work/ocaml-temporal"))
assert left["packages"] == right["packages"]
other = dict(metadata("/home/runner/work/ocaml-temporal")["packages"][0], name="other")
assert sbom.package_spdx_id(other, "/home/runner/work/ocaml-temporal/rust") != \
    right["packages"][0]["SPDXID"]
PY
