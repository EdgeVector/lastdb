#!/usr/bin/env python3
"""Preserve final derivatives, remove owned scratch, and package exact evidence."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[2]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise ValueError("Use the standard exclusive lock for final receipt work and cleanup")
    target = ROOT / "finalization-window.json"
    if target.exists():
        raise ValueError("Do not replace an earlier finalization receipt")
    if json.loads((ROOT / "isolated-followup-window.json").read_text()).get("complete") is not True:
        raise ValueError("All final measurements and frozen numeric audits must finish first")
    names = ("derive_auto_size.py", "cleanup_rejected_home.py", "cleanup_all_builds.py", "package_all.py")
    checksums = {name: digest(ROOT / name) for name in names}
    value = {"complete": False, "start_unix_ns": time.time_ns(), "commands": [],
             "driver_sha256": digest(Path(__file__)), "helpers_sha256": checksums}
    try:
        for name in names:
            if digest(ROOT / name) != checksums[name]:
                raise ValueError("A finalization helper changed after this window started")
            row = {"helper": name, "argv": [sys.executable, str(ROOT / name)], "start_unix_ns": time.time_ns()}
            value["commands"].append(row)
            target.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
            process = subprocess.run(row["argv"], cwd=REPO, stdin=subprocess.DEVNULL, check=False)
            row.update(exit_code=process.returncode, end_unix_ns=time.time_ns())
            if process.returncode:
                raise RuntimeError("Stop on the first final derivative, cleanup, or package failure")
        value["complete"] = True
    except BaseException as failure:
        value["error"] = str(failure)
        raise
    finally:
        value["end_unix_ns"] = time.time_ns()
        target.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"finalized": True, "helpers": len(names)}))


if __name__ == "__main__":
    main()
