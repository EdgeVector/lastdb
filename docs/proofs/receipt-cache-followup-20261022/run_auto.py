#!/usr/bin/env python3
"""Run one default-maintenance first cycle and clean only its owned home."""

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
             "draft": "a1941e3a8742d4da4743a02db99153119bdf034b",
             "after": "e79b711517519c9ce27d0f6f1b405a364be3ba91"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=("before", "draft", "after"), required=True)
    args = parser.parse_args()
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise ValueError("Use the standard exclusive host Cargo lock")
    name = "auto-" + args.variant + "-1"
    target, output = ROOT / (name + "-run.json"), ROOT / name
    if target.exists() or output.exists():
        raise ValueError("Do not replace a preceding active run or its receipt")
    build = json.loads((ROOT / (args.variant + "-build.json")).read_text())
    binary = Path(build["binary"]).resolve(strict=True)
    if (build["revision"] != REVISIONS[args.variant] or build["product_source_clean"] is not True
            or file_hash(binary) != build["binary_sha256"]):
        raise ValueError("The exact accepted release node copy differs")
    scratch = Path(tempfile.mkdtemp(prefix="rcauto.", dir=os.environ["TMPDIR"]))
    argv = [sys.executable, str(ROOT / "auto_retained.py"), "--binary", str(binary),
            "--source-revision", build["revision"], "--label", name,
            "--scratch", str(scratch), "--output", str(output)]
    helpers = {str(ROOT / file): file_hash(ROOT / file) for file in
               ("auto_retained.py", "long_sampling.py", "retained_inventory.py", "run_auto.py")}
    value = {"complete": False, "argv": argv, "helpers_sha256": helpers,
             "start_unix_ns": time.time_ns(), "scratch": str(scratch), "variant": args.variant}
    try:
        process = subprocess.run(argv, stdin=subprocess.DEVNULL, check=False)
        value["exit_code"] = process.returncode
        if process.returncode:
            raise RuntimeError("The active run failed; preserve its original receipts and generated home")
        summary = json.loads((output / "summary.json").read_text())
        if (summary.get("complete") is not True or summary.get("all_owned_processes_stopped") is not True
                or summary.get("fixed_requests") != 330 or summary.get("dropped_requests") != 0):
            raise RuntimeError("The active run or its exact request count does not complete")
        inputs = json.loads((output / "inputs.json").read_text())
        combined = {**helpers, **inputs["helpers_sha256"]}
        if any(file_hash(Path(path)) != checksum for path, checksum in combined.items()):
            raise RuntimeError("An active-run helper changed while the product ran")
        original("run_pairs").cleanup(output.resolve(strict=True))
        value.update(complete=True, generated_home_removed=not scratch.exists(),
                     ordered_input_sha256=summary["write_inputs"]["ordered_input_sha256"],
                     all_helpers_sha256=combined)
    except BaseException as failure:
        value["error"] = str(failure)
        raise
    finally:
        value["end_unix_ns"] = time.time_ns()
        target.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"active_cycle_complete": True, "variant": args.variant, "home_removed": not scratch.exists()}), flush=True)


if __name__ == "__main__":
    main()
