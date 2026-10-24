#!/usr/bin/env python3
"""Verify the retained CI artifact before the generated homes can be removed."""
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import zipfile


def gh(path):
    return subprocess.run(["gh", "api", path], check=True, capture_output=True).stdout


def main():
    artifact = os.environ["ARTIFACT_ID"]
    repository = os.environ["GITHUB_REPOSITORY"]
    prefix = f"repos/{repository}/actions/artifacts/{artifact}"
    raw = gh(prefix + "/zip")
    expected = os.environ["EXPECTED_SHA"].removeprefix("sha256:")
    actual = hashlib.sha256(raw).hexdigest()
    if actual != expected:
        raise SystemExit("Retained artifact checksum differs.")
    metadata = json.loads(gh(prefix))
    if metadata["expired"] or not metadata.get("expires_at"):
        raise SystemExit("Retained artifact has no valid expiry.")
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        if not any(name.endswith("product-source.txt") for name in archive.namelist()):
            raise SystemExit("Artifact lacks its product source revision.")
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as summary:
        summary.write(f"\nVerified artifact: {os.environ['DOWNLOAD_URL']}\n\n")
        summary.write(f"SHA-256: {actual}\n\nExpires: {metadata['expires_at']}\n")


if __name__ == "__main__":
    main()
