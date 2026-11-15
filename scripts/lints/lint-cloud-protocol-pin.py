#!/usr/bin/env python3
"""Fail when this client would be rejected by the deployed storage service.

The service is `cloud_backup_protocol` pinned by EdgeVector/exemem-infra main's
`fold` gitlink. When the protocol tree SHA matches the pin, exit immediately.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

PIN_API = "https://api.github.com/repos/EdgeVector/exemem-infra/contents/fold?ref=main"
FIX = (
    "Land and deploy the exemem-infra pin bump first, or gate the client "
    "so it does not send the incompatible field, variant, or shape."
)


def _git(repo: Path, *args: str, text: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=text)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root", default=".")
    p.add_argument("--head-protocol", help="protocol tree to treat as the client")
    p.add_argument("--pinned-protocol", help="protocol tree to treat as the deployed service")
    p.add_argument("--dump", action="store_true", help="print the head wire summary and exit")
    p.add_argument("--force-compare", action="store_true", help="compare AST shapes even when tree SHAs match")
    args = p.parse_args()
    repo = Path(args.repo_root).resolve()
    head_root = Path(args.head_protocol).resolve() if args.head_protocol else repo

    if args.dump:
        from protocol_pin.ast_scan import load_protocol
        _dump(load_protocol(head_root))
        return

    if args.pinned_protocol:
        from protocol_pin.ast_scan import collect_usage, load_protocol
        from protocol_pin.compare import incompatibilities
        head, pinned = load_protocol(head_root), load_protocol(Path(args.pinned_protocol).resolve())
        _report("local", incompatibilities(pinned, head, collect_usage(repo, head)))
        return

    sha = pin_sha()
    if not (re.fullmatch(r"[0-9a-f]{40}", sha) and _git(repo, "cat-file", "-e", f"{sha}^{{commit}}").returncode == 0):
        fetch = _git(repo, "fetch", "--depth", "1", "origin", sha)
        if fetch.returncode != 0:
            raise SystemExit(f"could not fetch pinned lastdb commit {sha}: {fetch.stderr.strip()}")
    pin_proc = _git(repo, "rev-parse", "--verify", f"{sha}:cloud_backup_protocol")
    if pin_proc.returncode != 0:
        raise SystemExit(f"pinned commit {sha} has no cloud_backup_protocol tree")
    pin_tree = pin_proc.stdout.strip()

    clean = _git(repo, "status", "--porcelain", "--", "cloud_backup_protocol")
    if not args.force_compare and not args.head_protocol and clean.returncode == 0 and not clean.stdout.strip():
        head_proc = _git(repo, "rev-parse", "--verify", "HEAD:cloud_backup_protocol")
        if head_proc.returncode == 0 and head_proc.stdout.strip() == pin_tree:
            print(f"cloud-protocol-pin: cloud_backup_protocol tree matches pinned {sha} ({pin_tree})")
            return

    from protocol_pin.ast_scan import collect_usage, load_protocol
    from protocol_pin.compare import incompatibilities

    head = load_protocol(head_root)
    with tempfile.TemporaryDirectory(prefix="protocol-pin-") as tmp:
        arc = _git(repo, "archive", sha, "cloud_backup_protocol", text=False)
        if arc.returncode != 0:
            raise SystemExit(f"pinned commit {sha} has no cloud_backup_protocol tree")
        subprocess.run(["tar", "-x", "-C", tmp], input=arc.stdout, check=True)
        pinned = load_protocol(Path(tmp))
    _report(sha, incompatibilities(pinned, head, collect_usage(repo, head)))


def _report(sha: str, errors: list[str]) -> None:
    if errors:
        print(f"cloud_backup_protocol is incompatible with the storage service\npinned by exemem-infra main (fold @{sha}).\n")
        for error in errors:
            print(f"- {error}")
        print(f"\n{FIX}")
        sys.exit(1)
    print(f"cloud_backup_protocol matches the storage service pinned by exemem-infra main (fold @{sha}).")


def pin_sha() -> str:
    tokens: list[str] = []
    for name in ("GITHUB_TOKEN", "GH_TOKEN", "GH_PAT"):
        val = os.environ.get(name, "").strip()
        if val and val not in tokens:
            tokens.append(val)
    tokens.append("")
    last = "no response"
    for token in tokens:
        req = urllib.request.Request(PIN_API, headers={"Accept": "application/vnd.github+json", "User-Agent": "lastdb-protocol-pin"})
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                payload = json.load(resp)
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
        f"Could not read EdgeVector/exemem-infra main's fold gitlink ({last}). "
        "The storage-service pin is that submodule. This check fails closed until the pin can be read."
    )


def _dump(schema) -> None:
    for path in sorted(schema.types):
        td = schema.types[path]
        if td.kind == "alias":
            print(f"alias {path} -> {td.alias}")
            continue
        print(f"{td.kind} {path} repr={td.repr} deny={td.deny}")
        for item in td.fields:
            print(f"  {item.wire} {item.encoded} skip={item.skip} default={item.default}")
        for variant in td.variants:
            print(f"  variant {variant.name} -> {variant.wire} {variant.kind}")


if __name__ == "__main__":
    main()
