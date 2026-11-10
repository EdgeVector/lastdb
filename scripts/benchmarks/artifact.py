#!/usr/bin/env python3
"""Verify the uploaded archive metadata and retain a compact download receipt."""

import argparse
import json
import os
from pathlib import Path
import re
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from node import save


def uploaded_metadata():
    """Wait for just-uploaded metadata, without relaxing receipt validation."""
    artifact_id = os.environ["ARTIFACT_ID"]
    if not artifact_id.isdecimal():
        raise ValueError("The uploaded artifact ID is invalid")
    url = (f"https://api.github.com/repos/{os.environ['GITHUB_REPOSITORY']}"
           f"/actions/artifacts/{artifact_id}")
    request = Request(url, headers={
        "Authorization": f"Bearer {os.environ['GH_TOKEN']}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    })
    delays = (2, 4, 8, 16, 30)
    for attempt in range(len(delays) + 1):
        try:
            with urlopen(request, timeout=30) as response:
                return json.load(response)
        except HTTPError as error:
            if error.code not in (404, 429, 500, 502, 503, 504) or attempt == len(delays):
                raise
            reason = f"HTTP {error.code}"
            error.close()
        except (URLError, TimeoutError):
            if attempt == len(delays):
                raise
            reason = "transport unavailable"
        print(f"Artifact metadata {reason}; retrying in {delays[attempt]}s", file=sys.stderr)
        time.sleep(delays[attempt])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", type=Path, help="Use saved metadata instead of fetching it")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    metadata = json.loads(args.metadata.read_text()) if args.metadata else uploaded_metadata()
    digest = os.environ["ARTIFACT_SHA256"].removeprefix("sha256:")
    revision = os.environ["CANDIDATE_SHA"]
    if not re.fullmatch(r"[0-9a-f]{64}", digest) or not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("The artifact digest or source revision is invalid")
    if (metadata["id"] != int(os.environ["ARTIFACT_ID"]) or metadata["expired"]
            or metadata["workflow_run"]["id"] != int(os.environ["GITHUB_RUN_ID"])
            or metadata["size_in_bytes"] <= 0 or metadata.get("digest") != f"sha256:{digest}"):
        raise ValueError("The uploaded archive metadata differs from the upload receipt")
    receipt = {"url": os.environ["ARTIFACT_URL"], "sha256": digest,
               "source_revision": revision,
               "command": f"gh run rerun {os.environ['GITHUB_RUN_ID']} -R {os.environ['GITHUB_REPOSITORY']}",
               "expires_at": metadata["expires_at"],
               "note": "Workflow artifact; API confirms ID, nonzero size, live state, and SHA-256 digest"}
    args.output.mkdir(parents=True, exist_ok=False)
    save(args.output / "artifact.json", receipt)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as summary:
        summary.write(f"\n[Download CI artifact]({receipt['url']})\n\n"
                      f"SHA-256: `{digest}`  \nSource: `{revision}`  \nExpires: {receipt['expires_at']}\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    raise SystemExit(main())
