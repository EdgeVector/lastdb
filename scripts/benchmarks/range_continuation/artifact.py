#!/usr/bin/env python3
"""Verify the uploaded artifact metadata before the runner removes raw evidence."""

import argparse
import json
import os
from pathlib import Path
import re

from node import file_hash


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--archive", type=Path, required=True)
    args = parser.parse_args()
    if args.metadata.stat().st_size > 1024 ** 2:
        raise RuntimeError("The uploaded artifact metadata exceeds oneMiB")
    value = json.loads(args.metadata.read_text())
    expected = os.environ["ARTIFACT_SHA256"].removeprefix("sha256:")
    if (str(value["id"]) != os.environ["ARTIFACT_ID"] or value.get("expired") is not False
            or not re.fullmatch("[0-9a-f]{64}", expected)
            or value.get("digest", "").removeprefix("sha256:") != expected
            or value.get("size_in_bytes", 0) <= 0 or not value.get("expires_at")):
        raise RuntimeError("The uploaded artifact identity, digest, bytes, or expiry differs")
    repository = os.environ["GITHUB_REPOSITORY"]
    run = os.environ["GITHUB_RUN_ID"]
    run_url = "https://github.com/" + repository + "/actions/runs/" + run
    expected_url = run_url + "/artifacts/" + os.environ["ARTIFACT_ID"]
    if os.environ["ARTIFACT_URL"] != expected_url:
        raise RuntimeError("The upload URL differs from the current workflow run and artifact")
    if args.archive.stat().st_size > 8 * 1024 ** 3 or file_hash(args.archive) != expected:
        raise RuntimeError("The downloaded artifact differs from the verified archive SHA256")
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
        stream.write("\nVerified artifact: [download](" + expected_url + ")\n\nSHA256: `" + expected
            + "`\n\nCandidate: `" + os.environ["CANDIDATE_SHA"] + "`\n\nBaseline: `"
            + os.environ["BASELINE_SHA"] + "`\n\nExpires: " + value["expires_at"]
            + "\n\nThe downloaded archive SHA256 matches the upload and GitHub metadata.\n")


if __name__ == "__main__":
    main()
