#!/usr/bin/env python3
"""Package all completed scopes and reproduce numeric results from compact data."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[2]


def blob_bounds():
    relative = str(ROOT.relative_to(REPO))
    paths = subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "--", relative], cwd=REPO, text=True).splitlines()
    paths += subprocess.check_output(["git", "ls-files", "--cached", "--", relative], cwd=REPO, text=True).splitlines()
    if len(paths) > 5000:
        raise ValueError("The finite saved-evidence inventory exceeds five thousand candidate files")
    largest, total = 0, 0
    for name in set(paths):
        path = REPO / name
        if path.is_symlink() or not path.is_file() or not path.resolve(strict=True).is_relative_to(ROOT):
            raise ValueError("A committed evidence candidate is linked or leaves its exact proof root")
        size = path.stat().st_size
        if size > 2 * 1024 * 1024:
            raise ValueError("A candidate evidence blob exceeds two MiB: " + name)
        largest, total = max(largest, size), total + size
    return {"candidate_files": len(set(paths)), "largest_blob_bytes": largest, "candidate_bytes": total}


def main():
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise ValueError("Use the standard exclusive lock for evidence compression and numeric reproduction")
    target = ROOT / "packaging-window.json"
    if target.exists():
        raise ValueError("Do not replace an earlier saved-evidence packaging window")
    helpers = ("preserve_summaries.py", "package_receipts.py", "package_extra.py", "package_extended.py",
               "package_recovery.py", "verify_packaged_analysis.py")
    checksums = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in helpers}
    value = {"complete": False, "commands": [], "helpers_sha256": checksums, "start_unix_ns": time.time_ns()}
    try:
        for helper in helpers:
            if hashlib.sha256((ROOT / helper).read_bytes()).hexdigest() != checksums[helper]:
                raise ValueError("A packaging helper changed during its evidence transformation")
            argv = [sys.executable, str(ROOT / helper)]
            row = {"argv": argv, "start_unix_ns": time.time_ns()}
            value["commands"].append(row)
            process = subprocess.run(argv, cwd=REPO, stdin=subprocess.DEVNULL, check=False)
            row.update(exit_code=process.returncode, end_unix_ns=time.time_ns())
            if process.returncode:
                raise RuntimeError("Stop at the first packaging or numeric reproduction error")
        value.update(complete=True, blob_bounds=blob_bounds())
    except BaseException as failure:
        value["error"] = str(failure)
        raise
    finally:
        value["end_unix_ns"] = time.time_ns()
        target.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"all_evidence_packaged": True, **value["blob_bounds"]}))


if __name__ == "__main__":
    main()
