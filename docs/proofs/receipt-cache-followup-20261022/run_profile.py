#!/usr/bin/env python3
"""Run the same fixed diagnostic on an exact before or optimized binary copy."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent / "receipt-part-pruning-benchmark-20261022"))
from reuse import original
from benchmark_support import file_hash

REVISIONS = {"before": "08d8436ccce9e2493e187a6b0133dc93f47c410f",
             "after": "e79b711517519c9ce27d0f6f1b405a364be3ba91"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=("before", "after"), required=True)
    args = parser.parse_args()
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise ValueError("Use the standard exclusive host Cargo lock")
    target = ROOT / (args.variant + "-profile-run.json")
    output = ROOT / (args.variant + "-profile-1")
    if target.exists() or output.exists():
        raise ValueError("Do not replace a preceding diagnostic profile or run receipt")
    build = json.loads((ROOT / (args.variant + "-build.json")).read_text())
    binary = Path(build["binary"]).resolve(strict=True)
    if (build["revision"] != REVISIONS[args.variant] or build["product_source_clean"] is not True
            or file_hash(binary) != build["binary_sha256"]):
        raise ValueError("The exact clean-source release build copy differs")
    scratch = Path(tempfile.mkdtemp(prefix="rcprof.", dir=os.environ["TMPDIR"]))
    argv = [sys.executable, str(ROOT / "profile_batches.py"), "--binary", str(binary),
            "--source-revision", build["revision"], "--label", args.variant + "-profile-1",
            "--scratch", str(scratch), "--output", str(output)]
    helpers = {str(ROOT / name): file_hash(ROOT / name) for name in
               ("profile_batches.py", "profile_sample.py", "run_profile.py")}
    value = {"complete": False, "argv": argv, "start_unix_ns": time.time_ns(),
             "helpers_sha256": helpers, "scratch": str(scratch)}
    try:
        process = subprocess.run(argv, stdin=subprocess.DEVNULL, check=False)
        value["exit_code"] = process.returncode
        if process.returncode:
            raise RuntimeError("The diagnostic profile failed; preserve its original receipts and home")
        summary = json.loads((output / "summary.json").read_text())
        if (summary.get("complete") is not True or summary.get("profiler_stopped") is not True
                or summary.get("all_owned_processes_stopped") is not True):
            raise RuntimeError("The diagnostic profile or process shutdown is incomplete")
        if any(file_hash(Path(path)) != checksum for path, checksum in helpers.items()):
            raise RuntimeError("A frozen profile helper changed during the diagnostic")
        original("run_pairs").cleanup(output.resolve(strict=True))
        value.update(complete=True, generated_home_removed=not scratch.exists(), sample_sha256=summary["sample_sha256"])
    except BaseException as failure:
        value["error"] = str(failure)
        raise
    finally:
        value["end_unix_ns"] = time.time_ns()
        target.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"profile_complete": True, "variant": args.variant, "home_removed": not scratch.exists()}), flush=True)


if __name__ == "__main__":
    main()
