#!/usr/bin/env python3
"""Run isolated writer gates, fresh diagnostics, matched work, and AUTO audits."""

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


def plan():
    revisions = ["--before-revision", BEFORE, "--after-revision", AFTER]
    steps = [("isolated-source-and-real-writer-gates", "verify_isolated.py", revisions, True)]
    for variant in ("before", "after"):
        steps += [(variant + "-fresh-profile", "run_profile.py", ["--variant", variant], True),
                  (variant + "-profile-provenance", "profile_provenance.py", ["--variant", variant], False),
                  (variant + "-stack-audit", "aggregate_native_sample.py",
                   ["--profile", str(ROOT / (variant + "-profile-1")), "--output",
                    str(ROOT / (variant + "-profile-stack-analysis.json"))], True)]
    steps += [("six-operations-and-three-pairs", "run_final_review.py", revisions + ["--host-lock", str(LOCK)], True),
              ("matched-numeric-audit", "analyze_matched.py", ["--root", str(ROOT), "--output",
               str(ROOT / "matched-numeric-audit.json")], True)]
    for variant in ("before", "draft", "after"):
        steps += [("default-auto-" + variant, "run_auto.py", ["--variant", variant], True),
                  ("auto-" + variant + "-numeric-audit", "analyze_auto.py", ["--root", str(ROOT),
                   "--variant", variant, "--output", str(ROOT / ("auto-" + variant + "-numeric-audit.json"))], True)]
    return steps


def main():
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") == "1":
        raise ValueError("Release the standard lock between separate measurement and audit stages")
    target = ROOT / "isolated-followup-window.json"
    builds = json.loads((ROOT / "isolated-builds.json").read_text())
    if target.exists() or builds.get("complete") is not True:
        raise ValueError("Require both completed fresh builds and an unused recovery window receipt")
    steps = plan()
    sources = [ROOT / helper for _, helper, _, _ in steps]
    sources += [ROOT / name for name in ("accept_isolated.py", "capability_capture.py", "analyze_auto.py",
                                        "matched_audit_metrics.py", "matched_audit_extra.py")]
    checksums = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}
    value = {"complete": False, "commands": [], "start_unix_ns": time.time_ns(), "helpers_sha256": checksums,
             "before_revision": BEFORE, "after_revision": AFTER,
             "rejected_attempt_archive": "rejected-build-1-archive.json",
             "preceding_dep_info_reader_failure": "source-guard-attempt-1-window.json",
             "genuine_historical_baseline_profile": "historical-before-profile-archive.json",
             "no_timed_requests_during_numeric_audit": True,
             "driver_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    try:
        for name, helper, arguments, locked in steps:
            if any(hashlib.sha256(Path(path).read_bytes()).hexdigest() != checksum for path, checksum in checksums.items()):
                raise ValueError("A frozen recovery-window stage helper changed")
            argv = [sys.executable, str(ROOT / helper), *arguments]
            if locked:
                argv = [str(REPO / "scripts/ci/with-fold-host-cargo-lock.sh"), "--", *argv]
            row = {"step": name, "argv": argv, "start_unix_ns": time.time_ns()}
            value["commands"].append(row)
            save(target, value)
            print(json.dumps({"isolated_followup_step": name, "state": "start"}), flush=True)
            process = subprocess.run(argv, cwd=REPO, env=environment, stdin=subprocess.DEVNULL, check=False)
            row.update(exit_code=process.returncode, end_unix_ns=time.time_ns())
            save(target, value)
            if process.returncode:
                raise RuntimeError("Stop at the first failed product or audit stage: " + name)
            print(json.dumps({"isolated_followup_step": name, "state": "complete"}), flush=True)
        value["complete"] = True
    except BaseException as failure:
        value["error"] = str(failure)
        raise
    finally:
        value["end_unix_ns"] = time.time_ns()
        save(target, value)


if __name__ == "__main__":
    main()
