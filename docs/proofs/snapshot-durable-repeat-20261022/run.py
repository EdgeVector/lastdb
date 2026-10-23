#!/usr/bin/env python3
"""Repeat real daemon latency and forced-stop captures without product changes."""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import sys
import time

EXPECTED_BINARY = "0d62917132e752dd12df47f126ef6f9d04d755b84e4da73899b0b9d5b11cec7a"
SOURCE = "a590e2abab0db6a3916fb7de3f9fc155677ac727"


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def summarize(proof, plan):
    result = {}
    for workload in ("single", "compaction"):
        runs, values = [], []
        for row in plan:
            if row["workload"] != workload:
                continue
            output = proof / row["name"]
            writes = json.loads((output / "writes.json").read_text())
            recovery = json.loads((output / "recovery.json").read_text())
            shutdown = json.loads((output / "native-shutdown.json").read_text())
            process_exit = json.loads((output / "shutdown.json").read_text())["exit_code"]
            boundary = json.loads((output / "stopped-boundaries.json").read_text())
            receipts = [sample["receipt"].get("durability") for sample in writes["samples"]]
            if (not recovery["all_values_match"] or not recovery["clock_advanced"]
                    or not shutdown["flush_ok"] or process_exit != 0
                    or any(item != "durable" for item in receipts)
                    or boundary["journal_directory_exists"] or boundary["journal_tail_exists"]):
                raise RuntimeError("a complete durable capture or recovery check failed")
            run = {"name": row["name"], **writes["summary"],
                   "post_stop_records": recovery["actual"], "all_acks_durable": True,
                   "parts_before": writes["before"]["native_disk"]["parts"],
                   "parts_after": writes["after"]["native_disk"]["parts"]}
            if workload == "single":
                run["hundred_call_averages_ms"] = [statistics.mean(
                    sample["latency_ms"] for sample in writes["samples"][first:first + 100])
                    for first in range(0, len(writes["samples"]), 100)]
            if workload == "compaction":
                compact = json.loads((output / "compaction.json").read_text())
                run["compaction_status"] = compact["status"]
                run["writes_complete_during_compaction_request"] = sum(
                    compact["start_unix_us"] <= item["end_unix_us"] <= compact["end_unix_us"]
                    for item in writes["samples"])
                retry = output / "compaction-after-writes.json"
                if retry.exists():
                    run["compaction_retry_status"] = json.loads(retry.read_text())["status"]
            values.extend(sample["latency_ms"] for sample in writes["samples"])
            runs.append(run)
        values.sort()
        result[workload] = {"runs": runs, "count": len(values),
            "pooled_average_ms": statistics.mean(values), "pooled_median_ms": statistics.median(values),
            "pooled_p95_ms": values[math.ceil(.95 * len(values)) - 1],
            "pooled_p99_ms": values[math.ceil(.99 * len(values)) - 1], "max_ms": values[-1],
            "average_range_ms": [min(r["average_ms"] for r in runs), max(r["average_ms"] for r in runs)],
            "p99_range_ms": [min(r["p99_ms"] for r in runs), max(r["p99_ms"] for r in runs)],
            "errors": sum(r["errors"] for r in runs), "all_acks_durable": True,
            "recovered_records": sum(r["post_stop_records"] for r in runs)}
    save(proof / "summary.json", result)
    print(json.dumps({key: {k: v for k, v in value.items() if k != "runs"}
                      for key, value in result.items()}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    args = parser.parse_args()
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise RuntimeError("use the host Cargo exclusion for the full capture")
    proof = Path(__file__).resolve().parent
    root = proof.parents[2]
    scratch = args.scratch.resolve()
    if scratch.stat().st_uid != os.getuid() or any(p.name != "candidate" for p in scratch.iterdir()):
        raise RuntimeError("use a fresh owned scratch directory with only the pinned binary")
    binary = scratch / "candidate"
    if not binary.exists():
        shutil.copyfile(root / "target/dev-release/lastdbd", binary)
        binary.chmod(0o755)
    actual = hashlib.sha256(binary.read_bytes()).hexdigest()
    if actual != EXPECTED_BINARY:
        raise RuntimeError("the daemon differs from the previously measured binary")
    subprocess.run(["git", "diff", "--exit-code", SOURCE, "HEAD", "--", "fold_db", "vendor",
                    "lastdb_node", "Cargo.toml", "Cargo.lock"], cwd=root, check=True)
    plan = [{"name": name, "workload": kind, "home": str(scratch / ("h" + str(number)))}
            for number, (name, kind) in enumerate([
                ("single-1", "single"), ("compaction-1", "compaction"),
                ("compaction-2", "compaction"), ("single-2", "single"),
                ("single-3", "single"), ("compaction-3", "compaction")])]
    save(proof / "plan.json", {"product_source": SOURCE, "binary_sha256": actual,
        "capture_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
        "binary": str(binary), "scratch": str(scratch), "uid": os.getuid(),
        "platform": platform.platform(), "load_before": os.getloadavg(), "runs": plan,
        "schemas": 64, "value_bytes": 64, "uds_workers": 8, "dependency_high_water": 4096,
        "durability_parameter": "omitted; durable receipt required", "product_changes": False,
        "compiler_exclusion": True, "other_host_work_controlled": False})
    capture = proof.parent / "snapshot-durable-rebase-20261022/capture.py"
    outcomes = []
    for row in plan:
        compact = row["workload"] == "compaction"
        command = [sys.executable, str(capture), "--binary", str(binary), "--home", row["home"],
                   "--output", str(proof / row["name"]), "--count", "256" if compact else "1000",
                   "--clients", "4" if compact else "1", "--batch-size", "8" if compact else "1"]
        if compact:
            command.append("--compact")
        print(json.dumps({"starting": row["name"]}), flush=True)
        began = time.monotonic()
        outcome = subprocess.run(command, cwd=root)
        outcomes.append({"run": row["name"], "exit_code": outcome.returncode,
                         "seconds": time.monotonic() - began, "load_after": os.getloadavg()})
        save(proof / "outcomes.json", outcomes)
        if outcome.returncode:
            raise RuntimeError("a real-daemon capture failed; preserve its receipts")
        shutil.copyfile(scratch / (Path(row["home"]).name + ".log"), proof / (row["name"] + ".daemon.txt"))
    summarize(proof, plan)


if __name__ == "__main__":
    main()
