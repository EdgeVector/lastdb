#!/usr/bin/env python3
"""Measure two exact LastDB revisions on generated homes, without local builds."""

import argparse
import json
import os
from pathlib import Path
import platform
import re
import signal
import subprocess
import time

from node import Node, file_hash, save
from workload import PROFILES, Workload


def measure(binary, revision, recipe, output):
    node, error = None, None
    started = time.monotonic()
    try:
        node = Node(binary, revision, output)
        node.start()
        Workload(node, recipe, output).run()
    except BaseException as failure:
        error = f"{type(failure).__name__}: {failure}"
    finally:
        if node is not None:
            try:
                node.close()
            except BaseException as failure:
                error = f"{error or ''}; cleanup: {type(failure).__name__}: {failure}"
    result = {"complete": error is None, "error": error, "revision": revision,
              "elapsed_seconds": time.monotonic() - started}
    save(output / "result.json", result)
    return result


def host_details():
    cpu = Path("/proc/cpuinfo")
    model = next((line.split(":", 1)[1].strip() for line in cpu.read_text().splitlines()
                  if line.startswith("model name")), None) if cpu.exists() else platform.processor()
    return {"platform": platform.platform(), "cpu_model": model, "cpu_count": os.cpu_count(),
            "runner_image": os.environ.get("ImageVersion"), "python": platform.python_version()}


def interrupted(_signum, _frame):
    raise KeyboardInterrupt("Benchmark received an interruption")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for label in ("baseline", "candidate"):
        parser.add_argument(f"--{label}", type=Path, required=True)
        parser.add_argument(f"--{label}-revision", required=True)
    parser.add_argument("--profile", choices=PROFILES, default="standard")
    parser.add_argument("--driver-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.driver_revision):
        parser.error("The driver revision must be a complete commit SHA")
    signal.signal(signal.SIGTERM, interrupted)
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    manifest_path = args.output / "benchmark.json"
    if manifest_path.exists():
        parser.error("The output already contains a benchmark; select a fresh output directory")
    binaries = {}
    for label in ("baseline", "candidate"):
        revision = getattr(args, f"{label}_revision")
        if not re.fullmatch(r"[0-9a-f]{40}", revision):
            parser.error("Each revision must be a complete commit SHA")
        binary = getattr(args, label).resolve(strict=True)
        version = subprocess.check_output([str(binary), "--version"], text=True, timeout=10).strip()
        if f"bench-{revision}" not in version:
            parser.error(f"The {label} executable version differs from the selected revision")
        binaries[label] = {"path": str(binary), "sha256": file_hash(binary),
                           "revision": revision, "version": version}
    recipe = {**PROFILES[args.profile], "body_bytes": 1024, "fields": 4,
              "read_batches": [1, 32], "write_batches": [1, 128],
              "clients": [1] if PROFILES[args.profile].get("insert_only") else [1, 4, 16, 32],
              "writes": "durable acknowledgements; product default storage and maintenance",
              "reads": "known exact hash/range keys; storage receipts on every measured read",
              "cold": "purged warm records after reopen; disjoint first keys; positive disk payload receipts; OS pages uncontrolled",
              "timing": "RPC through response bytes; payload construction and response validation excluded",
              "throughput": "closed-loop; includes Python client, validation, and scheduler overhead",
              "memory": "daemon memory sampled at 200 ms; seed and startup excluded",
              "disk": "sum of file lengths in the generated home; allocated blocks not measured"}
    manifest = {"format_version": 1, "profile": args.profile, "recipe": recipe,
                "driver_revision": args.driver_revision,
                "host": host_details(), "binaries": binaries, "runs": [], "complete": False,
                "drivers": {name: file_hash(Path(__file__).parent / name)
                            for name in ("run.py", "node.py", "workload.py", "report.py", "artifact.py", "build.py")}}
    save(manifest_path, manifest)
    for pair in range(recipe["pairs"]):
        order = ("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")
        for label in order:
            output = args.output / f"pair-{pair}-{label}"
            output.mkdir()
            print(json.dumps({"pair": pair, "version": label, "profile": args.profile}), flush=True)
            entry = {"pair": pair, "label": label, "directory": output.name,
                     **measure(Path(binaries[label]["path"]), binaries[label]["revision"], recipe, output)}
            manifest["runs"].append(entry)
            save(manifest_path, manifest)
            if not entry["complete"]:
                print(json.dumps(entry), flush=True)
                return 1
    manifest["complete"] = True
    save(manifest_path, manifest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
