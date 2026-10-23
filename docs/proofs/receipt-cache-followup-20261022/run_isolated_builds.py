#!/usr/bin/env python3
"""Hold one standard lock for the two fresh per-variant source builds."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent
SOURCES = {"before": ("/Users/tomtang/.fkanban/worktrees/lastdb-receipt-cache-baseline-20261022",
                       "08d8436ccce9e2493e187a6b0133dc93f47c410f"),
           "after": ("/Users/tomtang/.fkanban/worktrees/lastdb-receipt-cache-isolated-optimized-20261022",
                      "e79b711517519c9ce27d0f6f1b405a364be3ba91")}


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def main():
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise ValueError("Use one standard exclusive host lock for both isolated source builds")
    target = ROOT / "isolated-builds.json"
    if target.exists():
        raise ValueError("Do not replace an isolated build window receipt")
    driver = ROOT / "build_isolated.py"
    checksum = hashlib.sha256(driver.read_bytes()).hexdigest()
    result = {"complete": False, "start_unix_ns": time.time_ns(), "commands": [],
              "recovery_manifest": "rejected-build-1-archive.json", "build_driver_sha256": checksum,
              "driver_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    try:
        for variant, (source, revision) in SOURCES.items():
            if hashlib.sha256(driver.read_bytes()).hexdigest() != checksum:
                raise ValueError("The isolated build driver changed between variants")
            argv = [sys.executable, str(driver), "--variant", variant, "--repo", source, "--revision", revision]
            row = {"variant": variant, "argv": argv, "start_unix_ns": time.time_ns()}
            result["commands"].append(row)
            save(target, result)
            print(json.dumps({"isolated_build_start": variant}), flush=True)
            process = subprocess.run(argv, stdin=subprocess.DEVNULL, check=False)
            row.update(exit_code=process.returncode, end_unix_ns=time.time_ns())
            save(target, result)
            if process.returncode:
                raise RuntimeError("Stop at the first failed isolated source build")
        result["complete"] = True
    except BaseException as failure:
        result["error"] = str(failure)
        raise
    finally:
        result["end_unix_ns"] = time.time_ns()
        save(target, result)


if __name__ == "__main__":
    main()
