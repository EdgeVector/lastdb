#!/usr/bin/env python3
"""Keep one host-lock window for generated operations, pairs, and their analysis."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
from reuse import ROOT, helper_files, validate_revisions


def helpers():
    return {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in helper_files()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before-revision", required=True)
    parser.add_argument("--after-revision", required=True)
    parser.add_argument("--host-lock", required=True, type=Path)
    args = parser.parse_args()
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise ValueError("Use the standard external host Cargo lock")
    expected = Path.home() / ".cache" / "fold-db-perf-guard" / "host-cargo.lock"
    if args.host_lock.resolve(strict=True) != expected.resolve(strict=True):
        raise ValueError("Use the standard host lock without a fallback")
    target = ROOT / "review-runs.json"
    if target.exists():
        raise ValueError("Do not replace a preceding review window receipt")
    validate_revisions(ROOT, args.before_revision, args.after_revision)
    revisions = ["--before-revision", args.before_revision, "--after-revision", args.after_revision]
    commands = [[sys.executable, str(ROOT / "run_operational.py"), *revisions],
                [sys.executable, str(ROOT / "run_pairs.py"), *revisions, "--host-lock", str(args.host_lock)],
                [sys.executable, str(ROOT / "summarize.py"), *revisions]]
    frozen = helpers()
    receipt = {"complete": False, "start_unix_ns": time.time_ns(), "commands": [],
               "helpers_sha256": frozen, "lock_path": str(args.host_lock.resolve()),
               "before_revision": args.before_revision, "after_revision": args.after_revision,
               "lock_scope": "One standard exclusive host lock covers all operations, six runs, and analysis"}
    try:
        for argv in commands:
            if helpers() != frozen:
                raise RuntimeError("The fourteen measured helpers changed after the review window began")
            started = time.time_ns()
            process = subprocess.run(argv, stdin=subprocess.DEVNULL, check=False)
            receipt["commands"].append({"argv": argv, "exit_code": process.returncode,
                                        "start_unix_ns": started, "end_unix_ns": time.time_ns()})
            if process.returncode != 0:
                raise RuntimeError("The review stopped at a failed operation; preserve its original evidence")
        if helpers() != frozen:
            raise RuntimeError("A measured helper changed during the review window")
        receipt["complete"] = True
    except BaseException as failure:
        receipt["error"] = str(failure)
        raise
    finally:
        receipt["end_unix_ns"] = time.time_ns()
        target.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"review_complete": True, "operational_checks": 4, "independent_pairs": 3}), flush=True)


if __name__ == "__main__":
    main()
