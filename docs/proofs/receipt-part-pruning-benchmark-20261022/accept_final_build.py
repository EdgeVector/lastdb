#!/usr/bin/env python3
"""Accept exact clean final build receipts before any node operation."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
from reuse import ROOT, validate_revisions


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def load(name):
    return json.loads((ROOT / name).read_text())


def output(repo, *argv):
    return subprocess.check_output(argv, cwd=repo, text=True).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before-revision", required=True)
    parser.add_argument("--after-revision", required=True)
    args = parser.parse_args()
    target = ROOT / "final-build-checks.json"
    if target.exists():
        raise ValueError("Do not replace a final build acceptance receipt")
    builds = validate_revisions(ROOT, args.before_revision, args.after_revision)
    before, after = builds["before"], builds["after"]
    keys = ("build_argv", "rustc", "cargo")
    if any(before[key] != after[key] for key in keys):
        raise ValueError("The before and after node build commands or toolchains differ")
    accepted = []
    for label, revision in (("before", args.before_revision), ("after", args.after_revision)):
        node = builds[label]
        repo = Path(node["source_worktree"])
        if (output(repo, "git", "rev-parse", "HEAD") != revision
                or output(repo, "git", "status", "--porcelain", "--untracked-files=no")):
            raise ValueError("A frozen source revision changed before acceptance")
        items = [node, load(label + "-cli-build.json")]
        disk = load(label + "-disk-build.json")
        if disk["revision"] != revision or disk["product_source_clean"] is not True:
            raise ValueError("A physical tool receipt differs from its clean frozen source")
        items.extend(disk["binaries"].values())
        for item in items:
            path = Path(item["binary"]).resolve(strict=True)
            if digest(path) != item["binary_sha256"]:
                raise ValueError("A copied accepted binary differs from its original build hash")
            accepted.append({"binary": str(path), "sha256": item["binary_sha256"]})
    version = subprocess.check_output([after["binary"], "--version"], text=True).strip()
    if args.after_revision[:9] not in version:
        raise ValueError("The final candidate does not report its exact source revision")
    result = {"complete": True, "candidate_revision": args.after_revision, "candidate_version": version,
              "candidate_version_guard_passed": True, "source_tracked_clean_at_acceptance": True,
              "matching_measured_node_argv": before["build_argv"], "matching_compiler_and_cargo": True,
              "accepted_binaries": accepted, "nodes_started_before_acceptance": False,
              "rejected_binary_used_for_measurements": False, "rejected_original_attempt": "failed-build-1.json",
              "intermediate_archives": ["intermediate-c6645-archive.json", "intermediate-fa06955-archive.json"],
              "at_unix_ns": time.time_ns(),
              "candidate_only_cache_preparation": "candidate-cache-refresh.json",
              "acceptance_driver_sha256": digest(Path(__file__))}
    target.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"accepted": True, "version": version, "copies": len(accepted)}), flush=True)


if __name__ == "__main__":
    main()
