#!/usr/bin/env python3
"""Attribute the #708 memory result on owned, generated CI homes only."""
import argparse
import json
import os
from pathlib import Path
import re
import signal
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
from growth import GrowthWorkload, PROFILES  # noqa: E402
from node import Node, save  # noqa: E402

HEADER = re.compile(r"^([0-9a-f]+)-([0-9a-f]+)\s+(\S+)\s+\S+\s+\S+\s+\S+\s*(.*)$")


def numeric_fields(value, prefix=""):
    result = {}
    if isinstance(value, dict):
        for key, child in value.items():
            result.update(numeric_fields(child, f"{prefix}.{key}" if prefix else key))
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        result[prefix] = value
    return result


def mappings(raw):
    result, current = [], None
    for line in raw.splitlines():
        match = HEADER.match(line)
        if match:
            current = {"start": int(match[1], 16), "end": int(match[2], 16),
                       "permissions": match[3], "path": match[4]}
            result.append(current)
        elif current is not None and ":" in line:
            name, value = line.split(":", 1)
            if value.strip().endswith("kB"):
                current[name] = int(value.split()[0]) * 1024
    return result


def stack_pointers(proc):
    pointers, unavailable = [], 0
    threads = list((proc / "task").iterdir())
    if len(threads) > 512:
        raise RuntimeError("The owned process exceeds the diagnostic thread bound")
    for thread in threads:
        try:
            parts = (thread / "syscall").read_text().split()
            if len(parts) >= 3 and parts[-2].startswith("0x"):
                pointers.append(int(parts[-2], 16))
            else:
                unavailable += 1
        except (OSError, ValueError):
            unavailable += 1
    return pointers, unavailable, len(threads)


def snapshot(node, output, label):
    if node.process is None or node.process.poll() is not None:
        raise RuntimeError("The owned daemon is not active")
    proc = Path("/proc") / str(node.process.pid)
    raw = (proc / "smaps").read_text()
    (output / f"{label}-smaps.txt").write_text(raw)
    pointers, unavailable, thread_count = stack_pointers(proc)
    rows, totals = mappings(raw), {}
    for row in rows:
        stack = row["path"] == "[stack]" or any(row["start"] <= p < row["end"] for p in pointers)
        kind = "thread_stack" if stack else "file" if row["path"].startswith("/") else "anonymous"
        row["kind"] = kind
        totals[kind] = totals.get(kind, 0) + row.get("Rss", 0)
    # Numeric fields only: no identity, capability, configuration, or secret strings.
    status = numeric_fields(node.call("/api/status")[0])
    result = {"rss_by_kind": totals, "rss_bytes": sum(totals.values()), "thread_count": thread_count,
              "huge_page_bytes": sum(row.get("AnonHugePages", 0) for row in rows),
              "unavailable_stack_pointers": unavailable, "status_numbers": status,
              "largest_mappings": sorted(rows, key=lambda row: row.get("Rss", 0), reverse=True)[:24]}
    save(output / f"{label}.json", result)
    print(json.dumps({"snapshot": label, "rss_by_kind": totals,
                      "unavailable_stack_pointers": unavailable}), flush=True)
    return result


class AttributedGrowth(GrowthWorkload):
    def measure(self, case):
        self.current_case = case
        return super().measure(case)

    def checkpoint(self, number):
        point = super().checkpoint(number)
        point["attribution"] = snapshot(self.node, self.output, f"{self.current_case}-{number}")
        return point


def measure(binary, revision, output, disable_thp, allocation_profile=False):
    environment = {"LASTDB_SELF_METRICS_TO_DB": "false", "LASTDB_SEARCH_ENABLED": "0",
                   "EXEMEM_API_URL": "http://127.0.0.1:9",
                   "FOLD_SCHEMA_SERVICE_URL": "http://127.0.0.1:9"}
    if disable_thp:
        environment["MIMALLOC_ALLOW_THP"] = "0"
    if allocation_profile:
        environment["LASTDB_ALLOCATION_PROFILE"] = str(output / "allocation-profile.json")
    node = Node(binary, revision, output, temporary_directory=Path(os.environ["RUNNER_TEMP"]),
                environment=environment)
    try:
        node.start()
        snapshot(node, output, "boot")
        time.sleep(2)
        snapshot(node, output, "boot-idle")
        workload = AttributedGrowth(node, {"growth": PROFILES["standard"]}, output)
        workload.run()
    finally:
        node.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--programs", type=Path, required=True)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--disable-thp", action="store_true")
    parser.add_argument("--allocation-profile", action="store_true")
    args = parser.parse_args()
    root = Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)
    if (os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"
            or not args.output.resolve().is_relative_to(root) or args.output.exists()):
        raise RuntimeError("A fresh output under RUNNER_TEMP on a hosted CI runner is required")
    args.output.mkdir(mode=0o700)
    save(args.output / "recipe.json", {"disable_thp": args.disable_thp,
                                     "allocation_profile": args.allocation_profile,
                                     "source": os.environ.get("GITHUB_SHA")})
    for pair in range(2):
        for label in (("baseline", "candidate") if pair == 0 else ("candidate", "baseline")):
            revision = getattr(args, label)
            output = args.output / f"pair-{pair}-{label}"
            output.mkdir()
            measure(args.programs / revision / "lastdbd", revision, output,
                    args.disable_thp, args.allocation_profile)


def interrupted(_signal, _frame):
    raise KeyboardInterrupt("CI attribution interrupted")


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupted)
    main()
