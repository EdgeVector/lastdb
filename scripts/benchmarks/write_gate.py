#!/usr/bin/env python3
"""Compare 16 durable writers on current paired CI programs and fresh homes."""

import argparse
import json
import os
from pathlib import Path
import re
import signal

from disk_budget import require_search_off
from node import Node, file_hash, save
from run import host_details
from write_contention_workload import Writers, RECORDS
from write_contention_report import native_events, summarize


def diagnostics(directory, cases):
    native_events(directory, cases)
    events = []
    with (directory / "node.log").open() as source:
        for line in source:
            if "LASTDB_SNAPSHOT_TIMING " in line:
                events.append(json.loads(line.split("LASTDB_SNAPSHOT_TIMING ", 1)[1]))
    for case in cases:
        if not case["native_timing"]:
            continue
        first, last = case["case_started_unix_us"], case["case_ended_unix_us"]
        selected = [e for e in events if first <= e["started_us"] <= e["ended_us"] <= last]
        prepare = [e for e in selected if e["kind"] == "prepare"]
        if len(prepare) < case["requests"]:
            raise ValueError("Logical preparation records do not cover all measured calls")
        case["preparation"] = {"records": len(prepare), "phases": summarize(prepare),
            "counts": totals(prepare)}
        reuse = [e for e in selected if e["kind"] == "write_reads"]
        if reuse and len(reuse) < case["requests"]:
            raise ValueError("Private write reuse records are incomplete")
        case["write_reads"] = {"records": len(reuse), "counts": totals(reuse),
            "scope": "Absent instrumentation is unreported, not zero reuse"}
        save(directory / f"{case['name']}.json", case)


def totals(events):
    names = {name for event in events for name in event.get("counts", {})}
    return {name: {"reported": sum(name in e.get("counts", {}) for e in events),
                   "total": sum(e.get("counts", {}).get(name, 0) for e in events)}
            for name in sorted(names)}


def measure_home(args, label, pair):
    revision = getattr(args, f"{label}_revision")
    binary = getattr(args, label)
    directory = args.output / f"pair-{pair}-{label}"
    directory.mkdir()
    node = Node(binary, revision, directory, environment={"LASTDB_SEARCH_ENABLED": "0"})
    home = {"label": label, "pair": pair, "revision": revision,
            "directory": directory.name, "complete": False, "cases": []}
    try:
        node.start()
        require_search_off(node, directory, "before")
        writes = Writers(node, "shared")
        writes.seed()
        home["cases"].append(writes.measure("ordinary-16", 16))
        # Keep ordinary latency separate from the opt-in phase capture.
        if pair == 2:
            node.stop()
            node.native_timing = True
            node.environment["LASTDBD_STDIO_LOG_ROTATION"] = "0"
            node.start()
            writes.verify()
            home["cases"].append(writes.measure("native-16", 16))
        require_search_off(node, directory, "after")
        if node.call("/api/db/native-status")[0]["native_disk"].get("writer_failed") is not False:
            raise ValueError("The native writer is unhealthy")
    finally:
        node.close()
    diagnostics(directory, home["cases"])
    home["complete"] = True
    return home


def interrupt(_signum, _frame):
    raise KeyboardInterrupt("The paired writer comparison received an interruption")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for label in ("baseline", "candidate"):
        parser.add_argument(f"--{label}", type=Path, required=True)
        parser.add_argument(f"--{label}-revision", required=True)
    parser.add_argument("--driver-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
        parser.error("Writer comparisons require a GitHub-hosted Actions runner")
    for revision in (args.baseline_revision, args.candidate_revision, args.driver_revision):
        if not re.fullmatch(r"[0-9a-f]{40}", revision):
            parser.error("Every source revision must be a complete SHA")
    parent = Path(os.environ["BENCH_OUTPUT"]).resolve(strict=True)
    if args.output.exists() or args.output.is_symlink() or args.output.parent.resolve() != parent:
        parser.error("The comparison requires a fresh child of the external CI result directory")
    if parent.parent != Path(os.environ["RUNNER_TEMP"]).resolve(strict=True):
        parser.error("The result directory must be outside Git in RUNNER_TEMP")
    args.output.mkdir()
    manifest = {"complete": False, "driver_revision": args.driver_revision,
                "host": host_details(), "records": RECORDS, "pairs": 3,
                "requests_per_case": 2048, "clients": 16, "batch_records": 1,
                "schema": "GitHubBenchmark", "hash": "generated", "search_enabled": False,
                "programs": {label: {"revision": getattr(args, f"{label}_revision"),
                    "sha256": file_hash(getattr(args, label))} for label in ("baseline", "candidate")},
                "homes": [], "scope": "Three alternating pairs; separate final-pair native capture"}
    signal.signal(signal.SIGTERM, interrupt)
    try:
        for pair in range(3):
            order = ("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")
            for label in order:
                print(json.dumps({"pair": pair, "version": label}), flush=True)
                manifest["homes"].append(measure_home(args, label, pair))
                save(args.output / "write-gate.json", manifest)
        manifest["complete"] = True
    except BaseException as error:
        manifest["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        save(args.output / "write-gate.json", manifest)


if __name__ == "__main__":
    main()
