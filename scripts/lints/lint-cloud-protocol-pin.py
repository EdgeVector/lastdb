#!/usr/bin/env python3
"""Fail when this client would be rejected by the deployed storage service.

The service is the `cloud_backup_protocol` pinned by EdgeVector/exemem-infra
main's `fold` gitlink. PRs do not need cloud credentials. A new field is
allowed when serde skips it at its empty or default value and this client
never serializes any other value.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from protocol_pin.compare import incompatibilities, self_check
from protocol_pin.extract import load_protocol
from protocol_pin.usage import collect_usage

PIN_API = "https://api.github.com/repos/EdgeVector/exemem-infra/contents/fold?ref=main"
FIX = (
    "Land and deploy the exemem-infra pin bump first, or gate the client "
    "so it does not send the incompatible field, variant, or shape."
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--head-protocol", help="protocol tree to treat as the client")
    parser.add_argument("--pinned-protocol", help="protocol tree to treat as the deployed service")
    parser.add_argument("--dump", action="store_true", help="print the head wire summary and exit")
    args = parser.parse_args()
    self_check()
    repo = Path(args.repo_root).resolve()
    head_root = Path(args.head_protocol).resolve() if args.head_protocol else repo
    head = load_protocol(head_root)
    if args.dump:
        _dump(head)
        return
    if args.pinned_protocol:
        pinned = load_protocol(Path(args.pinned_protocol).resolve())
        sha = "local"
    else:
        sha = pin_sha()
        pinned_root = _materialize(repo, sha)
        pinned = load_protocol(pinned_root)
    usage = collect_usage(repo, head)
    errors = incompatibilities(pinned, head, usage)
    if errors:
        print(f"cloud_backup_protocol is incompatible with the storage service")
        print(f"pinned by exemem-infra main (fold @{sha}).")
        print()
        for error in errors:
            print(f"- {error}")
        print()
        print(FIX)
        sys.exit(1)
    print(f"cloud_backup_protocol matches the storage service pinned by exemem-infra main (fold @{sha}).")


def pin_sha():
    """Read exemem-infra main's fold gitlink. A token is optional."""
    tokens = []
    for name in ("GITHUB_TOKEN", "GH_TOKEN", "GH_PAT"):
        value = os.environ.get(name, "").strip()
        if value and value not in tokens:
            tokens.append(value)
    tokens.append("")
    last = "no response"
    for token in tokens:
        request = urllib.request.Request(
            PIN_API,
            headers={"Accept": "application/vnd.github+json", "User-Agent": "lastdb-protocol-pin"},
        )
        if token:
            request.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = json.load(response)
        except urllib.error.HTTPError as exc:
            last = f"HTTP {exc.code}"
            continue
        except urllib.error.URLError as exc:
            last = str(exc.reason)
            continue
        if payload.get("type") == "submodule" and payload.get("sha"):
            return payload["sha"]
        last = f"unexpected type {payload.get('type')}"
    raise SystemExit(
        "Could not read EdgeVector/exemem-infra main's fold gitlink "
        f"({last}). The storage-service pin is that submodule. "
        "This check fails closed until the pin can be read."
    )


def _materialize(repo, sha):
    dest = Path(tempfile.mkdtemp(prefix="protocol-pin-"))
    fetch = subprocess.run(
        ["git", "-C", str(repo), "fetch", "--depth", "1", "origin", sha],
        capture_output=True, text=True,
    )
    if fetch.returncode != 0:
        shutil.rmtree(dest, ignore_errors=True)
        raise SystemExit(f"could not fetch pinned lastdb commit {sha}: {fetch.stderr.strip()}")
    archive = subprocess.run(
        ["git", "-C", str(repo), "archive", sha, "cloud_backup_protocol"],
        capture_output=True,
    )
    if archive.returncode != 0:
        shutil.rmtree(dest, ignore_errors=True)
        raise SystemExit(f"pinned commit {sha} has no cloud_backup_protocol tree")
    subprocess.run(["tar", "-x", "-C", str(dest)], input=archive.stdout, check=True)
    return dest


def _dump(schema):
    for path in sorted(schema.types):
        typedef = schema.types[path]
        if typedef.kind == "alias":
            print(f"alias {path} -> {typedef.alias}")
            continue
        print(f"{typedef.kind} {path} repr={typedef.repr} deny={typedef.deny}")
        for item in typedef.fields:
            print(f"  {item.wire} {item.encoded} skip={item.skip} default={item.default}")
        for variant in typedef.variants:
            print(f"  variant {variant.name} -> {variant.wire} {variant.kind}")


if __name__ == "__main__":
    main()
