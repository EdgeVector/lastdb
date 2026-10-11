#!/usr/bin/env python3
"""Write a bounded owner-only promotion receipt at each external action boundary."""
from __future__ import annotations

import argparse
import json
import os
import re
import stat
import sys
import tempfile
from pathlib import Path

MAX_BYTES = 1024 * 1024
STAGES = ("prepared", "commit_requested", "release_requested", "release_created", "commit_created", "push_requested", "branch_pushed", "pr_requested", "pr_created", "auto_arm_requested", "auto_arm_returned", "auto_arm_verified", "dry_run_complete")


def read(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_size > MAX_BYTES:
        raise ValueError
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError
    return value


def write(path, value):
    encoded = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
    if len(encoded) > MAX_BYTES:
        raise ValueError
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.exists() or path.is_symlink():
        read(path)
    descriptor, scratch = tempfile.mkstemp(prefix=".promotion-stage-", dir=str(path.parent))
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(scratch, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(scratch):
            os.unlink(scratch)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", required=True, type=Path)
    parser.add_argument("--init", action="store_true")
    parser.add_argument("--stage", required=True, choices=STAGES)
    parser.add_argument("--mode", choices=("publish", "dry-run"))
    parser.add_argument("--tag")
    parser.add_argument("--source-oid")
    parser.add_argument("--tap-base")
    parser.add_argument("--tap-root")
    parser.add_argument("--artifacts", type=Path)
    parser.add_argument("--bump-diff", type=Path)
    parser.add_argument("--sums", type=Path)
    parser.add_argument("--release-url")
    parser.add_argument("--pr-url")
    parser.add_argument("--branch")
    parser.add_argument("--head")
    parser.add_argument("--auto-arm-result", type=Path)
    args = parser.parse_args()
    if args.init:
        if args.path.exists() or args.path.is_symlink():
            raise ValueError
        if not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", args.tag or ""):
            raise ValueError
        if args.mode == "publish" and not re.fullmatch(r"[0-9a-f]{40}", args.tap_base or ""):
            raise ValueError
        if args.artifacts.stat().st_size > MAX_BYTES or args.bump_diff.stat().st_size > MAX_BYTES or args.sums.stat().st_size > MAX_BYTES:
            raise ValueError
        artifacts = json.loads(args.artifacts.read_text(encoding="utf-8"))
        if not isinstance(artifacts, list) or not artifacts:
            raise ValueError
        source_oid = args.source_oid or artifacts[0].get("source_git_oid", "")
        if not re.fullmatch(r"[0-9a-f]{40}", source_oid) or any(item.get("source_git_oid") != source_oid for item in artifacts):
            raise ValueError
        args.source_oid = source_oid
        args.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        descriptor = os.open(args.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(b"{}\n")
            stream.flush()
            os.fsync(stream.fileno())
        payload = {"schema": "lastdb.mini.forge_homebrew_promotion.v1", "publish": args.mode == "publish", "mode": args.mode, "version_tag": args.tag, "release_repo": "EdgeVector/homebrew-lastdb", "formula_venue": "github", "release_assets": {"files": sorted([item["tarball"] for item in artifacts] + [item["manifest"] for item in artifacts] + ["SHA256SUMS.txt"]), "sha256sums": args.sums.read_text(encoding="utf-8").splitlines()}, "validated_artifacts": artifacts, "homebrew_bump": {"formulas": ["Formula/lastdb.rb", "Formula/folddb.rb"], "changed_line_classes": ["version", "url", "sha256"], "diff_line_count": len(args.bump_diff.read_text(encoding="utf-8").splitlines())}, "live_outputs": {"release_url": "", "tap_pr_url": "", "forge_pr_number": ""}, "publication": {"source_git_oid": args.source_oid, "tap_base": args.tap_base or "", "tap_root": args.tap_root or ""}}
    else:
        payload = read(args.path)
    if args.release_url is not None:
        expected = "https://github.com/EdgeVector/homebrew-lastdb/releases/tag/" + payload["version_tag"]
        if args.release_url != expected:
            raise ValueError
        payload["live_outputs"]["release_url"] = args.release_url
    if args.pr_url is not None:
        if not re.fullmatch(r"https://github.com/EdgeVector/homebrew-lastdb/pull/[0-9]+", args.pr_url):
            raise ValueError
        payload["live_outputs"]["tap_pr_url"] = args.pr_url
    if args.branch is not None:
        if not re.fullmatch(r"auto-bump/v[0-9]+\.[0-9]+\.[0-9]+-[A-Za-z0-9-]+", args.branch):
            raise ValueError
        payload["publication"]["branch"] = args.branch
    if args.head is not None:
        if not re.fullmatch(r"[0-9a-f]{40}", args.head):
            raise ValueError
        payload["publication"]["head"] = args.head
    if args.auto_arm_result is not None:
        result = read(args.auto_arm_result)
        if result.get("url") != payload["live_outputs"]["tap_pr_url"] or result.get("headRefOid") != payload["publication"].get("head") or not result.get("autoMergeRequest"):
            raise ValueError
    if args.stage == "auto_arm_verified" and args.auto_arm_result is None:
        raise ValueError
    payload["publication"]["stage"] = args.stage
    payload["publication"]["complete"] = args.stage in ("auto_arm_verified", "dry_run_complete")
    write(args.path, payload)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        print("homebrew-promotion-receipt: bounded stage receipt refused; reconcile the existing exact receipt before retry", file=sys.stderr)
        raise SystemExit(64)
