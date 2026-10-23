#!/usr/bin/env python3
"""Verify the uploaded archive metadata and retain a compact download receipt."""

import argparse
import json
import os
from pathlib import Path
import re

from node import save


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    metadata = json.loads(args.metadata.read_text())
    digest = os.environ["ARTIFACT_SHA256"].removeprefix("sha256:")
    revision = os.environ["CANDIDATE_SHA"]
    if not re.fullmatch(r"[0-9a-f]{64}", digest) or not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("The artifact digest or source revision is invalid")
    if (metadata["id"] != int(os.environ["ARTIFACT_ID"]) or metadata["expired"]
            or metadata["size_in_bytes"] <= 0 or metadata.get("digest") != f"sha256:{digest}"):
        raise ValueError("The uploaded archive metadata differs from the upload receipt")
    receipt = {"url": os.environ["ARTIFACT_URL"], "sha256": digest,
               "source_revision": revision,
               "command": f"gh run rerun {os.environ['GITHUB_RUN_ID']} -R {os.environ['GITHUB_REPOSITORY']}",
               "expires_at": metadata["expires_at"],
               "note": "14-day workflow artifact; API confirms ID, nonzero size, live state, and SHA-256 digest"}
    args.output.mkdir(parents=True, exist_ok=False)
    save(args.output / "artifact.json", receipt)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as summary:
        summary.write(f"\n[Download raw benchmark results]({receipt['url']})\n\n"
                      f"SHA-256: `{digest}`  \nSource: `{revision}`  \nExpires: {receipt['expires_at']}\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    raise SystemExit(main())
