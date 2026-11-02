#!/usr/bin/env python3
"""CI-only attribution of settled v2 size after updates, idle, and reopen."""

import argparse
import json
import os
from pathlib import Path
import platform
import re
import signal
import stat
import time

from disk_budget import DiskBudgetWorkload
from node import Node, file_hash, save

COUNTS = (512, 16384)
ROUNDS = 3
PAIRS = 2
IDLE_SECONDS = 30


def files(home):
    """Bound the owned-home walk by depth, entries, and wall time."""
    stack, result = [(home, 0)], []
    deadline, entries = time.monotonic() + 15, 0
    while stack:
        parent, depth = stack.pop()
        if depth > 12 or entries > 100000 or time.monotonic() > deadline:
            raise ValueError("The diagnostic file walk exceeded its bound")
        try:
            children = list(parent.iterdir())
        except FileNotFoundError:
            continue
        for path in children:
            entries += 1
            if entries > 100000 or time.monotonic() > deadline:
                raise ValueError("The diagnostic file walk exceeded its bound")
            try:
                info = path.lstat()
            except FileNotFoundError:
                continue
            if stat.S_ISLNK(info.st_mode):
                raise ValueError("An owned diagnostic home contains a symlink")
            if stat.S_ISDIR(info.st_mode):
                stack.append((path, depth + 1))
            else:
                result.append({"path": str(path.relative_to(home)), "bytes": info.st_size,
                               "allocated_bytes": info.st_blocks * 512})
    return sorted(result, key=lambda item: item["path"])


def snapshot(workload, number):
    point = workload.checkpoint(number)
    point["files"] = files(workload.node.home)
    return point


def measure(binary, recipe, folder):
    folder.mkdir(parents=True, exist_ok=False)
    node, result = None, {"complete": False}
    try:
        node = Node(Path(binary["path"]), binary["revision"], folder,
                    environment={"LASTDB_DISK_FORMAT": "2"})
        node.start()
        workload = DiskBudgetWorkload(node, {"growth": recipe}, folder)
        workload.seed()
        result["fixed_read"] = workload.measure("fixed-read")
        result["seed_files"] = files(node.home)
        result["fixed_update"] = workload.measure("fixed-update")
        result["after_updates"] = snapshot(workload, ROUNDS)
        save(folder / "disk-excess.json", result)
        time.sleep(IDLE_SECONDS)
        result["after_idle"] = snapshot(workload, ROUNDS + 1)
        node.stop()
        save(folder / "reopen-shutdown.json", json.loads((folder / "shutdown.json").read_text()))
        node.start()
        result["after_reopen"] = snapshot(workload, ROUNDS + 2)
        # All benchmark observations above precede this owner report.
        inventory, _, _ = node.call("/api/db/inventory")
        result["inventory"] = inventory["inventory"]
        result["after_inventory"] = snapshot(workload, ROUNDS + 3)
        if (result["inventory"]["per_schema_records_total_bytes"]
                != result["after_reopen"]["logical_storage"]["total_logical_bytes"]):
            raise ValueError("The inventory and pre-inventory logical record totals differ")
        native, _, _ = node.call("/api/db/native-status")
        result["native_status"] = native
        if native["native_disk"].get("writer_failed") is not False:
            raise ValueError("The diagnostic writer is unhealthy")
        result["complete"] = True
    except BaseException as error:
        result["error"] = f"{type(error).__name__}: {error}"
    finally:
        if node is not None:
            try:
                node.close()
            except BaseException as error:
                result.update(complete=False, cleanup_error=f"{type(error).__name__}: {error}")
        save(folder / "disk-excess.json", result)
    if not result["complete"]:
        raise ValueError(f"Disk diagnostic incomplete: {result.get('error') or result.get('cleanup_error')}")
    return result


def render(manifest):
    lines = ["# V2 disk excess investigation", "",
             f"Driver: `{manifest['driver_revision']}`. Two alternating pairs; 512 and 16,384 seeded records.",
             "Three update rounds replace every title and body. The generated keys and values are verified after each write and reopen.", "",
             "| Pair | Version | Records | State | Logical MiB | Disk MiB | Disk / logical |",
             "|---:|---|---:|---|---:|---:|---:|"]
    for row in manifest["runs"]:
        for state in ("after_updates", "after_idle", "after_reopen", "after_inventory"):
            point = row["result"][state]
            logical = point["logical_storage"]["total_logical_bytes"]
            disk = max(point["disk"].values())
            lines.append(f"| {row['pair']} | {row['label']} | {row['records']} | {state} | "
                         f"{logical / 2**20:.3f} | {disk / 2**20:.3f} | {disk / logical:.3f}× |")
    lines.extend(["", "The idle interval is 30 seconds. Reopen uses the same owned generated home and verifies every live key.",
                  "The inventory occurs after the benchmark observations. It writes nothing; after_inventory shows the state after its walk.",
                  "The artifact retains the named live-key classes, logical reports, file lengths and allocated blocks.",
                  "Inventory byte counts describe decrypted live records, not physical file bytes. Classes can omit other namespaces.",
                  "This investigation does not change the 1.5× advisory ceiling, maintenance settings, or the primary.", ""])
    return "\n".join(lines)


def interrupted(_signum, _frame):
    raise KeyboardInterrupt("Disk investigation received an interruption")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for label in ("baseline", "candidate"):
        parser.add_argument(f"--{label}", type=Path, required=True)
        parser.add_argument(f"--{label}-revision", required=True)
    parser.add_argument("--driver-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if (os.environ.get("GITHUB_ACTIONS") != "true"
            or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"):
        parser.error("Disk excess investigation requires a GitHub-hosted Actions runner")
    if not re.fullmatch(r"[0-9a-f]{40}", args.driver_revision):
        parser.error("The driver revision must be a complete SHA")
    signal.signal(signal.SIGTERM, interrupted)
    args.output.mkdir(parents=True, exist_ok=False)
    binaries = {}
    for label in ("baseline", "candidate"):
        path, revision = getattr(args, label).resolve(strict=True), getattr(args, label + "_revision")
        if not re.fullmatch(r"[0-9a-f]{40}", revision):
            parser.error("Each product revision must be a complete SHA")
        binaries[label] = {"path": str(path), "revision": revision, "sha256": file_hash(path)}
    manifest = {"complete": False, "driver_revision": args.driver_revision, "binaries": binaries,
                "recipe": {"counts": COUNTS, "rounds": ROUNDS, "pairs": PAIRS, "idle_seconds": IDLE_SECONDS},
                "host": {"platform": platform.platform(), "cpus": os.cpu_count()}, "runs": [],
                "drivers": {name: file_hash(Path(__file__).with_name(name)) for name in
                            ("disk_excess.py", "disk_budget.py", "disk_deletes.py", "disk_usage.py", "growth.py", "node.py", "workload.py")}}
    try:
        for count in COUNTS:
            recipe = {"records": count, "rounds": ROUNDS, "idle_seconds": 1}
            for pair in range(PAIRS):
                order = ("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")
                for label in order:
                    directory = f"records-{count}-pair-{pair}-{label}"
                    result = measure(binaries[label], recipe, args.output / directory)
                    manifest["runs"].append({"records": count, "pair": pair, "label": label,
                                             "directory": directory, "result": result})
                    save(args.output / "investigation.json", manifest)
                    print(json.dumps({"records": count, "pair": pair, "label": label, "complete": True}), flush=True)
        manifest["complete"] = True
    finally:
        save(args.output / "investigation.json", manifest)
    report = render(manifest)
    (args.output / "summary.md").write_text(report)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
        stream.write(report)
    print(report)


if __name__ == "__main__":
    main()
