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

from node import RUN_NODE_MARKER, Node, file_hash, save
from native_phases import collect_native_phases
from workload import PROFILES, Workload
from growth import PROFILES as GROWTH_PROFILES, GrowthWorkload
from disk_budget import DiskBudgetWorkload


def measure(binary, revision, recipe, output, workers, seed=None, minimum_retired=0):
    node, error = None, None
    started = time.monotonic()
    try:
        program = seed or (binary, revision, workers)
        environment = {}
        if recipe.get("typed_range_comparison"):
            environment.update(LASTDB_HASH_KEY_ENCODING="blind_v1", LASTDB_RANGE_KEY_ENCODING="ope_v1")
        node = Node(program[0], program[1], output, native_timing=recipe["native_timing"], workers=program[2],
                    environment=environment)
        node.start()
        if recipe.get("disk_budget"):
            marker = (node.home / "data" / "laststore-pack-node-v1").read_text()
            save(output / "disk-format.json", {"marker": marker})
            if marker != RUN_NODE_MARKER:
                raise ValueError("The disk budget requires a v2 run home")
            DiskBudgetWorkload(node, recipe, output).run()
        elif recipe.get("growth"):
            GrowthWorkload(node, recipe, output).run()
        else:
            target = (binary, revision, workers) if seed else None
            Workload(node, recipe, output, target, minimum_retired).run()
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
              "native_timing": args.native_timing,
              "read_batches": [1, 32], "write_batches": [1, 128],
              "clients": [1] if PROFILES[args.profile].get("insert_only") else [1, 4, 16, 32],
              "writes": "durable acknowledgements; product default storage and maintenance",
              "reads": "known exact hash/range keys; storage receipts on every measured read",
              "mixed": "each client alternates read/write; adjacent clients start with opposite kinds",
              "cold": "purged warm records after reopen; disjoint first keys; positive disk payload receipts; OS pages uncontrolled",
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
            recipe.update(disk_budget=True, disk_format=2,
                          writes="durable batches; flush at each checkpoint; default v2 automatic run merges",
                          disk="database data/ and blobs/ only for the 1.5x ratio; Search queue, other home, and whole home separate; three stable seconds",
                          logical="GET /api/db/schemas total_logical_bytes; plaintext serialized atoms")
    if args.pairs is not None:
        recipe["pairs"] = args.pairs
    if args.seed_with_baseline:
        recipe["seed_with_baseline"] = True
        recipe["minimum_retired"] = args.minimum_retired
    recipe["uds_workers"] = {label: getattr(args, f"{label}_workers") for label in ("baseline", "candidate")}
    if args.candidate_typed_ranges:
        recipe["typed_range_comparison"] = True
        recipe["typed_range_baseline"] = args.baseline_typed_ranges
        typed = "typed-v1 inside encrypted V2 blocks"
        recipe["range_formats"] = {"baseline": typed if args.baseline_typed_ranges else "legacy OPE-v1",
                                   "candidate": typed}
    return recipe


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for label in ("baseline", "candidate"):
        parser.add_argument(f"--{label}", type=Path, required=True)
        parser.add_argument(f"--{label}-revision", required=True)
        parser.add_argument(f"--{label}-workers", type=int, choices=(0, 4, 8, 16, 32), default=0)
    parser.add_argument("--profile", choices=PROFILES, default="standard")
    parser.add_argument("--pairs", type=int, choices=range(1, 7))
    parser.add_argument("--native-timing", action="store_true")
    parser.add_argument("--growth-only", action="store_true")
    parser.add_argument("--disk-budget-only", action="store_true")
    parser.add_argument("--seed-with-baseline", action="store_true",
                        help="Measure a real upgrade of an owned home created by the baseline daemon")
    parser.add_argument("--candidate-typed-ranges", action="store_true",
                        help="Convert only the candidate's stopped generated seed with its matching CLI")
    parser.add_argument("--baseline-typed-ranges", action="store_true",
                        help="Also convert the baseline's independent seed with its matching CLI")
    parser.add_argument("--minimum-retired", type=int, default=0)
    parser.add_argument("--driver-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.baseline_typed_ranges and not args.candidate_typed_ranges:
        parser.error("A typed baseline requires --candidate-typed-ranges")
    if args.candidate_typed_ranges and args.seed_with_baseline:
        parser.error("Typed range comparison requires independent generated seeds")
    if (args.native_timing or args.growth_only or args.disk_budget_only or args.seed_with_baseline
            or args.baseline_workers or args.candidate_workers or args.candidate_typed_ranges) and (
            os.environ.get("GITHUB_ACTIONS") != "true"
            or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"):
        parser.error("Diagnostic, growth, upgrade, and worker measurements require a GitHub-hosted Actions runner")
    if args.disk_budget_only and (args.growth_only or args.native_timing or args.seed_with_baseline):
        parser.error("Disk budget requires its own generated v2 workload")
    if args.native_timing and args.growth_only:
        parser.error("Resource growth uses ordinary daemon memory settings without native diagnostics")
    if args.seed_with_baseline and args.growth_only:
        parser.error("The upgrade measurement uses the ordinary request workload")
    if not 0 <= args.minimum_retired <= 1_000_000 or (args.minimum_retired and not args.seed_with_baseline):
        parser.error("A bounded legacy retirement requirement needs --seed-with-baseline")
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
    recipe = recipe_for(args)
    manifest = {"format_version": 1, "profile": args.profile, "recipe": recipe,
                "driver_revision": args.driver_revision,
                "host": host_details(), "binaries": binaries, "runs": [], "complete": False,
                "drivers": {name: file_hash(Path(__file__).parent / name)
                            for name in ("run.py", "node.py", "workload.py", "phases.py", "native_phases.py",
                                         "native_report.py", "report.py", "artifact.py", "build.py",
                                         "growth.py", "growth_report.py", "disk_usage.py", "disk_budget.py", "disk_budget_report.py",
                                         "tail_report.py", "mixed_report.py",
                                         "advisories.py", "confirm.py",
                                         "range_continuation/process_counters.py", "typed_ranges.py")}}
    save(manifest_path, manifest)
    for pair in range(recipe["pairs"]):
        order = ("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")
        for label in order:
            output = args.output / f"pair-{pair}-{label}"
            output.mkdir()
            print(json.dumps({"pair": pair, "version": label, "profile": args.profile}), flush=True)
            baseline = binaries["baseline"]
            seed = ((Path(baseline["path"]), baseline["revision"], recipe["uds_workers"]["baseline"])
                    if args.seed_with_baseline else None)
            entry = {"pair": pair, "label": label, "directory": output.name,
                     "seed_revision": baseline["revision"] if seed else binaries[label]["revision"],
                     "uds_workers": recipe["uds_workers"][label],
                     **measure(Path(binaries[label]["path"]), binaries[label]["revision"],
                               {**recipe, "typed_range_target": bool(args.candidate_typed_ranges
                                   and (label == "candidate" or args.baseline_typed_ranges))}, output,
                               recipe["uds_workers"][label], seed,
                               args.minimum_retired if label == "candidate" else 0)}
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
