#!/usr/bin/env python3
"""Refresh only the stale node release stamp, then repeat the unchanged build."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
import build_variant


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--scratch", type=Path, required=True)
    args = parser.parse_args()
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise ValueError("Use the standard external host Cargo lock")
    repo = args.repo.resolve(strict=True)
    if repo.parent != build_variant.DEV or args.repo.is_symlink():
        raise ValueError("Use the exact candidate DEV worktree")
    build_variant.frozen(repo, args.revision)
    _, owner = build_variant.owner(args.scratch)
    command = ["cargo", "clean", "--release", "-p", "lastdb_node"]
    environment = dict(os.environ)
    environment["CARGO_TARGET_DIR"] = str(build_variant.ROOT.parents[2] / "target")
    started = time.time_ns()
    process = subprocess.run(command, cwd=repo, env=environment, check=False)
    receipt = {"argv": command, "exit_code": process.returncode,
               "start_unix_ns": started, "end_unix_ns": time.time_ns(),
               "source_revision": args.revision, "owner": owner,
               "CARGO_TARGET_DIR": environment["CARGO_TARGET_DIR"],
               "reason": "Shared-target build-script cache watched the baseline worktree's git state and retained its version stamp",
               "scope": "Candidate-only build-cache preparation; no product source, measured build argv, or baseline binary change",
               "driver_sha256": build_variant.digest(Path(__file__))}
    build_variant.save(build_variant.ROOT / "candidate-cache-refresh.json", receipt)
    process.check_returncode()
    build_variant.frozen(repo, args.revision)
    argv = [sys.executable, str(Path(build_variant.__file__)), "--variant", "after", "--repo", str(repo),
            "--revision", args.revision, "--scratch", str(args.scratch), "--disk-tools"]
    subprocess.run(argv, cwd=repo, env=environment, check=True)


if __name__ == "__main__":
    main()
