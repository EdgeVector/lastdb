#!/usr/bin/env python3
"""Measure two exact LastDB revisions on generated homes, or repeat suspect suites once."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import platform
import re
import signal
import subprocess
import sys
import time

from disk_budget import DiskBudgetWorkload
from growth import PROFILES as GROWTH_PROFILES, GrowthWorkload
from node import Node, file_hash, save
from phases import collect_native_phases
from workload import PROFILES, Workload


def measure(binary, revision, recipe, output, workers):
    node, error = None, None
    started = time.monotonic()
    try:
        node = Node(binary, revision, output,
                    native_timing=recipe["native_timing"] or recipe.get("disk_budget", False), workers=workers,
                    environment={}, record_protocol=True)
        node.start()
        save(output / "home-format.json", {"format": node.home_format()})
        if recipe.get("disk_budget"):
            DiskBudgetWorkload(node, recipe, output).run()
            save(output / "disk-format.json", {"marker": (node.home / "data" / "laststore-pack-node-v1").read_text()})
        elif recipe.get("growth"):
            GrowthWorkload(node, recipe, output).run()
        else:
            Workload(node, recipe, output).run()
    except BaseException as failure:
        error = f"{type(failure).__name__}: {failure}"
    finally:
        if node is not None:
            try:
                node.close()
            except BaseException as failure:
                error = f"{error or ''}; cleanup: {type(failure).__name__}: {failure}"
    if error is None and recipe["native_timing"]:
        try:
            save(output / "native-phases.json", collect_native_phases(output))
        except Exception as failure:
            error = f"native phase capture: {type(failure).__name__}: {failure}"
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


def recipe_for(args):
    recipe = {**PROFILES[args.profile], "body_bytes": 1024, "fields": 4,
              "native_timing": args.native_timing, "read_batches": [1, 32], "write_batches": [1, 128],
              "clients": [1] if PROFILES[args.profile].get("insert_only") else [1, 4, 16, 32],
              "writes": "durable acknowledgements; product default storage and maintenance",
              "reads": "known exact hash/range keys; storage receipts on every measured read",
              "mixed": "each client alternates read/write; adjacent clients start with opposite kinds",
              "cold": "purged warm records after reopen; disjoint first keys; positive disk payload receipts; OS pages uncontrolled",
              "spread_records": 33 * PROFILES[args.profile]["samples"],
              "cold_spread": "distinct shuffled keys over 64 hashes; seed batches interleave the hashes, so several run files cover each key",
              "timing": "RPC through response bytes; payload construction and response validation excluded",
              "throughput": "closed-loop; includes Python client, validation, and scheduler overhead",
              "memory": "daemon memory sampled at 200 ms; seed and startup excluded",
              "phases": "Current request ring after each write case; exact request IDs; outside timed cases; coverage explicit",
              "disk": "sum of file lengths in the generated home; growth checkpoints also record allocated blocks"}
    if args.growth_only or args.disk_budget_only:
        recipe = {"pairs": recipe["pairs"], "body_bytes": 1024, "fields": 4,
                  "native_timing": False, "growth": GROWTH_PROFILES[args.profile],
                  "read_batches": [64], "write_batches": [128], "clients": [1],
                  "writes": "durable batches; explicit flush at every checkpoint, then three stable "
                            "seconds of v2 automatic run merges",
                  "reads": "all known exact keys; generated values and latest revisions verified",
                  "memory": "daemon memory at 200 ms; per-round idle median; seed and startup excluded from peak",
                  "disk": "file lengths and allocated blocks from one bounded walk before and after the v2 wait"}
        if args.disk_budget_only:
            recipe["growth"] = {**recipe["growth"], "delete_target_bytes": 10 * 2**20}
            recipe.update(disk_budget=True, pairs=1,
                          writes="durable batches; flush at each checkpoint; default automatic run merges",
                          disk="database data/ only for the 1.5x ratio; other home files and the whole home stay separate; three stable seconds",
                          logical="GET /api/db/schemas total_logical_bytes; the stored record values of the home")
    if args.pairs is not None:
        recipe["pairs"] = args.pairs
    recipe["uds_workers"] = {label: getattr(args, f"{label}_workers") for label in ("baseline", "candidate")}
    return recipe


def confirm_suite(parser, kind, output):
    from growth_report import compare as growth_compare
    from report import compare as speed_compare, load_comparison as load, suspects
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
        parser.error("Automatic benchmark confirmation requires a GitHub-hosted Actions runner")
    manifest, comparison = load(output, {"growth": growth_compare, "speed": speed_compare}[kind])
    initial = sorted(suspects(comparison, kind))
    save(output / "repeat-plan.json", {"kind": kind, "suspects": initial, "max_repeats": 1})
    if not initial:
        print("No suspect measures; skip the confirmation run.", flush=True)
        return 0
    print(f"Repeat {kind} once for {len(initial)} suspect case/measure combinations.", flush=True)
    command = [sys.executable, str(Path(__file__).resolve()),
               "--profile", manifest["profile"], "--pairs", str(manifest["recipe"]["pairs"]),
               "--driver-revision", manifest["driver_revision"], "--output", str(output / "confirmation")]
    for label, binary in manifest["binaries"].items():
        command.extend([f"--{label}", binary["path"], f"--{label}-revision", binary["revision"],
                        f"--{label}-workers", str(manifest["recipe"]["uds_workers"][label])])
    if kind == "growth":
        command.append("--growth-only")
    if manifest["recipe"]["native_timing"]:
        command.append("--native-timing")
    subprocess.run(command, check=True)
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--confirm", choices=("speed", "growth"))
    for label in ("baseline", "candidate"):
        parser.add_argument(f"--{label}", type=Path)
        parser.add_argument(f"--{label}-revision")
        parser.add_argument(f"--{label}-workers", type=int, choices=(0, 4, 8, 16, 32), default=0)
    parser.add_argument("--profile", choices=PROFILES, default="standard")
    parser.add_argument("--pairs", type=int, choices=range(1, 7))
    parser.add_argument("--native-timing", action="store_true")
    parser.add_argument("--growth-only", action="store_true")
    parser.add_argument("--disk-budget-only", action="store_true")
    parser.add_argument("--driver-revision")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.confirm:
        return confirm_suite(parser, args.confirm, args.output)
    if any(getattr(args, name) is None for name in (
            "baseline", "baseline_revision", "candidate", "candidate_revision", "driver_revision")):
        parser.error("--baseline, --baseline-revision, --candidate, --candidate-revision, and --driver-revision are required")
    if (args.native_timing or args.growth_only or args.disk_budget_only
            or args.baseline_workers or args.candidate_workers) and (
            os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"):
        parser.error("Diagnostic, growth, and worker measurements require a GitHub-hosted Actions runner")
    if args.disk_budget_only and (args.growth_only or args.native_timing):
        parser.error("Disk budget requires its own generated v2 workload")
    if args.disk_budget_only and args.pairs not in (None, 1):
        parser.error("The disk budget uses one baseline/candidate pair")
    if args.native_timing and args.growth_only:
        parser.error("Resource growth uses ordinary daemon memory settings without native diagnostics")
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
        binaries[label] = {"path": str(binary), "sha256": file_hash(binary), "revision": revision, "version": version}
    recipe = recipe_for(args)
    manifest = {"format_version": 1, "profile": args.profile, "recipe": recipe,
                "driver_revision": args.driver_revision, "host": host_details(),
                "binaries": binaries, "runs": [], "complete": False,
                "drivers": {name: file_hash(Path(__file__).parent / name)
                            for name in ("run.py", "node.py", "workload.py", "phases.py", "report.py",
                                         "artifact.py", "build.py", "growth.py", "growth_report.py",
                                         "disk_budget.py", "process_counters.py")}}
    save(manifest_path, manifest)
    for pair in range(recipe["pairs"]):
        for label in (("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")):
            output = args.output / f"pair-{pair}-{label}"
            output.mkdir()
            print(json.dumps({"pair": pair, "version": label, "profile": args.profile}), flush=True)
            entry = {"pair": pair, "label": label, "directory": output.name,
                     "uds_workers": recipe["uds_workers"][label],
                     **measure(Path(binaries[label]["path"]), binaries[label]["revision"], recipe, output,
                               recipe["uds_workers"][label])}
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
