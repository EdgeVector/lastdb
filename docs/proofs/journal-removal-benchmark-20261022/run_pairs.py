#!/usr/bin/env python3
"""Run three ordered product pairs with one owned node at a time."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True
PROOF = Path(__file__).resolve().parent
sys.path.insert(0, str(PROOF.parent / "snapshot-commits-20261022"))
from cleanup import cleanup


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def load_build(label):
    value = json.loads((PROOF / f"{label}-build.json").read_text())
    path = Path(value["binary"])
    if not value["product_source_clean"] or not path.is_file():
        raise ValueError("The benchmark requires the recorded clean product source and binary")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    if digest.hexdigest() != value["binary_sha256"]:
        raise ValueError("The benchmark executable changed after its build receipt")
    return value


def run_pair_item(label, pair, build):
    name = f"{label}-{pair}"
    output = PROOF / name
    if output.exists():
        raise ValueError("A captured run already exists; do not overwrite its receipts")
    scratch = Path(tempfile.mkdtemp(prefix="jrun.", dir=os.environ["TMPDIR"]))
    args = [sys.executable, str(PROOF / "benchmark.py"), "--binary", build["binary"],
            "--scratch", str(scratch), "--output", str(output),
            "--source-revision", build["revision"], "--label", name]
    started = time.time_ns()
    result = subprocess.run(args, stdin=subprocess.DEVNULL, check=False)
    receipt = {"label": label, "pair": pair, "run": name, "argv": args,
               "start_unix_ns": started, "end_unix_ns": time.time_ns(),
               "exit_code": result.returncode, "host_load_after": os.getloadavg()}
    if result.returncode != 0:
        receipt["scratch_retained"] = str(scratch)
        return receipt
    summary = json.loads((output / "summary.json").read_text())
    if not summary["complete"] or not summary["all_owned_processes_stopped"]:
        raise RuntimeError("The product run does not confirm complete results and stopped nodes")
    cleanup(output.resolve(strict=True))
    receipt["scratch_removed"] = not scratch.exists()
    print(json.dumps({"completed_run": name, "scratch_removed": receipt["scratch_removed"]}), flush=True)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host-lock", required=True, type=Path)
    args = parser.parse_args()
    expected_lock = Path.home() / ".cache" / "fold-db-perf-guard" / "host-cargo.lock"
    if args.host_lock.resolve(strict=True) != expected_lock.resolve(strict=True):
        raise ValueError("Use the standard host Cargo lock, without a fallback")
    order = [("before", 1), ("after", 1), ("after", 2),
             ("before", 2), ("before", 3), ("after", 3)]
    builds = {label: load_build(label) for label in ("before", "after")}
    receipt = {"complete": False, "order": order, "builds": builds, "runs": [],
               "host": platform.platform(), "host_load_before": os.getloadavg(),
               "lock_path": str(args.host_lock.resolve()),
               "lock_scope": "External wrapper holds the host Cargo lock throughout this process",
               "scope": "Controlled local product comparison; automatic maintenance disabled",
               "cache": "Fresh application homes; operating system caches remain uncontrolled",
               "start_unix_ns": time.time_ns()}
    try:
        for label, pair in order:
            item = run_pair_item(label, pair, builds[label])
            receipt["runs"].append(item)
            save(PROOF / "pair-runs.json", receipt)
            if item["exit_code"] != 0:
                return 1
        receipt["complete"] = True
        return 0
    finally:
        receipt["end_unix_ns"] = time.time_ns()
        save(PROOF / "pair-runs.json", receipt)


if __name__ == "__main__":
    raise SystemExit(main())
