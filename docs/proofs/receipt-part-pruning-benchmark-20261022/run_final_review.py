#!/usr/bin/env python3
"""Hold one standard lock for the final operational proof and matched pairs."""

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
from run_operational import disk_build, load, run_item


def hashes():
    paths = helper_files()
    paths += [ROOT / name for name in ("run_final_review.py", "run_operational.py", "auto_reclaim_capture.py",
              "retention_capture.py", "edge_capture.py", "payload_capture.py", "overflow_capture.py",
              "overflow_pages.py", "older_refusal.py", "orphan_repeat_capture.py")]
    return {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}


def execute(receipt, argv):
    started = time.time_ns()
    result = subprocess.run(argv, stdin=subprocess.DEVNULL, check=False)
    receipt["commands"].append({"argv": argv, "exit_code": result.returncode,
                                "start_unix_ns": started, "end_unix_ns": time.time_ns()})
    if result.returncode:
        raise RuntimeError("The final review stopped at a failed product operation; preserve its evidence")


def auto_check(builds, revision):
    manifest = load(ROOT / "operational-runs.json")
    if manifest.get("complete") is not True:
        raise ValueError("The preceding four operational checks do not complete")
    manifest["complete"] = False
    manifest["checks"] = 6
    (ROOT / "operational-runs.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    after = disk_build("after", revision)
    base = ["--binary", builds["after"]["binary"], "--source-revision", revision,
            "--disk-binary", after["lastdb-disk-home"], "--maintenance-binary", after["lastdb-pack-maintain"]]
    try:
        run_item("auto-reclaim-operational", "auto_reclaim_capture.py", base, [], manifest)
        run_item("orphan-repeat-operational", "orphan_repeat_capture.py", base, [], manifest)
        manifest["complete"] = True
    except BaseException as failure:
        manifest["error"] = str(failure)
        raise
    finally:
        manifest["end_unix_ns"] = time.time_ns()
        (ROOT / "operational-runs.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before-revision", required=True)
    parser.add_argument("--after-revision", required=True)
    parser.add_argument("--host-lock", type=Path, required=True)
    args = parser.parse_args()
    expected = Path.home() / ".cache/fold-db-perf-guard/host-cargo.lock"
    if (os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1"
            or args.host_lock.resolve(strict=True) != expected.resolve(strict=True)):
        raise ValueError("Use the standard exclusive external host lock")
    target = ROOT / "review-runs.json"
    if target.exists():
        raise ValueError("Do not replace a preceding final review receipt")
    builds = validate_revisions(ROOT, args.before_revision, args.after_revision)
    revisions = ["--before-revision", args.before_revision, "--after-revision", args.after_revision]
    frozen = hashes()
    receipt = {"complete": False, "start_unix_ns": time.time_ns(), "commands": [],
               "helpers_sha256": frozen, "before_revision": args.before_revision,
               "after_revision": args.after_revision, "operational_checks": 6,
               "lock_path": str(args.host_lock.resolve()),
               "lock_scope": "One exclusive standard host lock covers six checks, six runs, and analysis"}
    try:
        execute(receipt, [sys.executable, str(ROOT / "run_operational.py"), *revisions])
        auto_check(builds, args.after_revision)
        for argv in ([sys.executable, str(ROOT / "run_pairs.py"), *revisions, "--host-lock", str(args.host_lock)],
                     [sys.executable, str(ROOT / "summarize.py"), *revisions]):
            if hashes() != frozen:
                raise RuntimeError("A frozen operational or measured helper changed")
            execute(receipt, argv)
        if hashes() != frozen:
            raise RuntimeError("A frozen helper changed during the final review")
        receipt["complete"] = True
    except BaseException as failure:
        receipt["error"] = str(failure)
        raise
    finally:
        receipt["end_unix_ns"] = time.time_ns()
        target.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"review_complete": True, "operational_checks": 6, "independent_pairs": 3}), flush=True)


if __name__ == "__main__":
    main()
