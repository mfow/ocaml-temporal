#!/usr/bin/env python3
"""Generate and validate a deterministic SPDX document from Cargo metadata.

The script intentionally uses only the Python standard library. CI supplies
Cargo metadata from the locked graph and runs this script inside the pinned
official Python image; no package manager or network access is needed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any


NAMESPACE = "https://github.com/mfow/ocaml-temporal/sbom/cargo"


def package_spdx_id(package_id: str) -> str:
    """Return a stable SPDX identifier derived from Cargo's package ID."""

    digest = hashlib.sha256(package_id.encode("utf-8")).hexdigest()[:16]
    return f"SPDXRef-Package-{digest}"


def load_json(path: Path) -> dict[str, Any]:
    """Load a JSON object and turn malformed input into a concise CLI error."""

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"JSON root in {path} must be an object")
    return value


def make_document(metadata: dict[str, Any]) -> dict[str, Any]:
    """Convert `cargo metadata --format-version 1` into SPDX 2.3 JSON."""

    packages = metadata.get("packages")
    if not isinstance(packages, list) or not packages:
        raise ValueError("Cargo metadata contains no packages")
    normalized: list[dict[str, str]] = []
    for package in packages:
        if not isinstance(package, dict):
            raise ValueError("Cargo metadata package is not an object")
        package_id = package.get("id")
        name = package.get("name")
        version = package.get("version")
        if not all(isinstance(value, str) and value for value in (package_id, name, version)):
            raise ValueError("Cargo package is missing id, name, or version")
        normalized.append(
            {
                "SPDXID": package_spdx_id(package_id),
                "name": name,
                "versionInfo": version,
                "licenseConcluded": package.get("license") or "NOASSERTION",
                "downloadLocation": package.get("source") or "NOASSERTION",
            }
        )
    normalized.sort(key=lambda item: (item["name"], item["versionInfo"], item["SPDXID"]))
    return {
        "spdxVersion": "SPDX-2.3",
        "SPDXID": "SPDXRef-DOCUMENT",
        "name": "ocaml-temporal Cargo dependency graph",
        "documentNamespace": NAMESPACE,
        "creationInfo": {
            "created": "1970-01-01T00:00:00Z",
            "creators": ["Tool: ocaml-temporal-generate-cargo-sbom"],
        },
        "packages": normalized,
    }


def audit_document(document: dict[str, Any]) -> None:
    """Validate the deterministic SPDX fields required by this project."""

    if document.get("spdxVersion") != "SPDX-2.3":
        raise ValueError("SBOM is not SPDX-2.3")
    if document.get("SPDXID") != "SPDXRef-DOCUMENT":
        raise ValueError("SBOM document identifier is invalid")
    if document.get("documentNamespace") != NAMESPACE:
        raise ValueError("SBOM namespace is invalid")
    packages = document.get("packages")
    if not isinstance(packages, list) or not packages:
        raise ValueError("SBOM contains no packages")
    ids: set[str] = set()
    sort_keys: list[tuple[str, str, str]] = []
    for package in packages:
        if not isinstance(package, dict):
            raise ValueError("SBOM package is not an object")
        identifier = package.get("SPDXID")
        name = package.get("name")
        version = package.get("versionInfo")
        if not all(isinstance(value, str) and value for value in (identifier, name, version)):
            raise ValueError("SBOM package is missing SPDXID, name, or version")
        if identifier in ids:
            raise ValueError(f"duplicate SBOM package identifier: {identifier}")
        ids.add(identifier)
        sort_keys.append((name, version, identifier))
    if sort_keys != sorted(sort_keys):
        raise ValueError("SBOM packages are not deterministically sorted")


def main() -> int:
    """Run generation or validation selected by the command-line arguments."""

    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--metadata", type=Path, help="Cargo metadata JSON input")
    group.add_argument("--audit", type=Path, help="SPDX JSON document to validate")
    args = parser.parse_args()
    try:
        if args.metadata is not None:
            document = make_document(load_json(args.metadata))
            json.dump(document, sys.stdout, indent=2, sort_keys=True)
            sys.stdout.write("\n")
        else:
            audit_document(load_json(args.audit))
            print("cargo SBOM audit: ok")
    except ValueError as exc:
        print(f"cargo SBOM audit: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
