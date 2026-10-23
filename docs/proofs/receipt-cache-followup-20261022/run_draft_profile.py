#!/usr/bin/env python3
"""Copy the accepted draft, profile one owned generated node, then clean its home."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent
PRIOR = ROOT.parent / "receipt-part-pruning-benchmark-20261022"
sys.path.insert(0, str(PRIOR))
from reuse import original
from benchmark_support import file_hash


def main():
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise ValueError("Use the standard exclusive host Cargo lock")
    target = ROOT / "draft-profile-run.json"
    output = ROOT / "draft-profile-1"
    if target.exists() or output.exists():
        raise ValueError("Do not replace a preceding diagnostic profile or its run receipt")
    if not (ROOT / "draft-copy.json").exists():
        subprocess.run([sys.executable, str(ROOT / "prepare_draft.py")], check=True)
    build = json.loads((ROOT / "draft-build.json").read_text())
    binary = Path(build["binary"]).resolve(strict=True)
    if file_hash(binary) != build["binary_sha256"]:
        raise ValueError("The accepted exact draft copy changed")
    scratch = Path(tempfile.mkdtemp(prefix="rcprof.", dir=os.environ["TMPDIR"]))
    argv = [sys.executable, str(ROOT / "profile_batches.py"), "--binary", str(binary),
            "--source-revision", build["revision"], "--label", "draft-profile-1",
            "--scratch", str(scratch), "--output", str(output)]
    value = {"complete": False, "argv": argv, "start_unix_ns": time.time_ns(),
             "driver_sha256": file_hash(Path(__file__)), "scratch": str(scratch)}
    try:
        process = subprocess.run(argv, stdin=subprocess.DEVNULL, check=False)
        value["exit_code"] = process.returncode
        if process.returncode:
            raise RuntimeError("The real draft profile failed; preserve its original receipt and home")
        summary = json.loads((output / "summary.json").read_text())
        if (summary.get("complete") is not True or summary.get("profiler_stopped") is not True
                or summary.get("all_owned_processes_stopped") is not True):
            raise RuntimeError("The diagnostic profile or its process shutdown is incomplete")
        original("run_pairs").cleanup(output.resolve(strict=True))
        value.update(complete=True, generated_home_removed=not scratch.exists(),
                     sample_sha256=summary["sample_sha256"])
    except BaseException as failure:
        value["error"] = str(failure)
        raise
    finally:
        value["end_unix_ns"] = time.time_ns()
        target.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"draft_profile_complete": True, "home_removed": not scratch.exists()}), flush=True)


if __name__ == "__main__":
    main()
