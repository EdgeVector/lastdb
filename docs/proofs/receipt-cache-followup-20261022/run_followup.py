#!/usr/bin/env python3
"""Run accepted optimized diagnostics, controlled pairs, and separate AUTO cycles."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[2]
BEFORE = "08d8436ccce9e2493e187a6b0133dc93f47c410f"
AFTER = "e79b711517519c9ce27d0f6f1b405a364be3ba91"
LOCK = Path("/Users/tomtang/.cache/fold-db-perf-guard/host-cargo.lock")


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def main():
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") == "1":
        raise ValueError("This coordinator must release the standard lock between separate run families")
    target = ROOT / "followup-window.json"
    if target.exists():
        raise ValueError("Do not replace a preceding follow-up window receipt")
    wrapper = REPO / "scripts/ci/with-fold-host-cargo-lock.sh"
    revisions = ["--before-revision", BEFORE, "--after-revision", AFTER]
    steps = [("accept-builds", "accept_builds.py", revisions, True),
             ("optimized-profile", "run_profile.py", ["--variant", "after"], True),
             ("profile-provenance", "profile_provenance.py", ["--variant", "after"], False),
             ("six-operations-and-three-pairs", "run_final_review.py", revisions + ["--host-lock", str(LOCK)], True)]
    steps += [("default-auto-" + name, "run_auto.py", ["--variant", name], True)
              for name in ("before", "draft", "after")]
    result = {"complete": False, "start_unix_ns": time.time_ns(), "commands": [],
              "before_revision": BEFORE, "after_revision": AFTER,
              "lock_scope": "Separate exclusive lock acquisition for acceptance, profile, controlled family, and each AUTO cycle",
              "external_optimized_build_queue_wait": json.loads((ROOT / "optimized-build-queue-wait.json").read_text()),
              "driver_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    save(target, result)
    try:
        for name, helper, arguments, locked in steps:
            argv = [sys.executable, str(ROOT / helper), *arguments]
            if locked:
                argv = [str(wrapper), "--", *argv]
            row = {"step": name, "argv": argv, "start_unix_ns": time.time_ns()}
            result["commands"].append(row)
            save(target, result)
            print(json.dumps({"followup_step": name, "state": "start"}), flush=True)
            process = subprocess.run(argv, cwd=REPO, stdin=subprocess.DEVNULL, check=False)
            row.update(exit_code=process.returncode, end_unix_ns=time.time_ns())
            save(target, result)
            if process.returncode:
                raise RuntimeError("Stop the follow-up window at its first failed stage: " + name)
            print(json.dumps({"followup_step": name, "state": "complete"}), flush=True)
        result["complete"] = True
    except BaseException as failure:
        result["error"] = str(failure)
        raise
    finally:
        result["end_unix_ns"] = time.time_ns()
        save(target, result)


if __name__ == "__main__":
    main()
