#!/usr/bin/env python3
"""Use the same node-package cache preparation before each new release build."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from build_variant import digest, frozen


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=("before", "after"), required=True)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--scratch", type=Path, required=True)
    args = parser.parse_args()
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise ValueError("Use the standard exclusive host Cargo lock")
    receipt = ROOT / (args.variant + "-cache-preparation.json")
    if receipt.exists() or (ROOT / (args.variant + "-build.json")).exists():
        raise ValueError("Do not replace an earlier preparation or build receipt")
    repo = args.repo.resolve(strict=True)
    if repo.parent != Path("/Users/tomtang/.fkanban/worktrees") or args.repo.is_symlink():
        raise ValueError("Use the exact clean source-only DEV worktree")
    frozen(repo, args.revision)
    environment = dict(os.environ, CARGO_TARGET_DIR=str(ROOT.parents[2] / "target"))
    argv = ["cargo", "clean", "--release", "-p", "lastdb_node"]
    started = time.time_ns()
    process = subprocess.run(argv, cwd=repo, env=environment, capture_output=True, text=True, check=False)
    value = {"complete": process.returncode == 0, "argv": argv, "revision": args.revision,
             "source_worktree": str(repo), "start_unix_ns": started, "end_unix_ns": time.time_ns(),
             "exit_code": process.returncode, "stdout": process.stdout, "stderr": process.stderr,
             "CARGO_TARGET_DIR": environment["CARGO_TARGET_DIR"], "driver_sha256": digest(Path(__file__)),
             "purpose": "Symmetric package-only cache preparation prevents stale source version stamps"}
    receipt.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    if process.returncode:
        raise RuntimeError("The node-package cache preparation failed")
    frozen(repo, args.revision)
    subprocess.run([sys.executable, str(ROOT / "build_variant.py"), "--variant", args.variant,
                    "--repo", str(repo), "--revision", args.revision,
                    "--scratch", str(args.scratch), "--disk-tools"], check=True)


if __name__ == "__main__":
    main()
