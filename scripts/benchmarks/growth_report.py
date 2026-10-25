#!/usr/bin/env python3
"""Publish resource comparisons; growth regressions remain advisory."""

import argparse
import json
import math
import os
from pathlib import Path
import statistics

from advisories import confirm, lines as confirmation_lines, notices
from growth import CASES, slope
from node import save

MEASURES = ("peak_memory_bytes", "idle_memory_bytes", "memory_growth_bytes",
            "memory_growth_bytes_per_round", "disk_before_compaction_bytes", "compacted_disk_bytes",
            "disk_growth_bytes", "disk_growth_bytes_per_round", "compacted_disk_bytes_per_record")
ALLOCATED_MEASURES = ("allocated_disk_before_compaction_bytes", "compacted_allocated_disk_bytes",
                      "allocated_disk_growth_bytes", "allocated_disk_growth_bytes_per_round",
                      "compacted_allocated_disk_bytes_per_record")
PERCENT = 20
MEMORY_FLOOR = 16 * 2**20
DISK_FLOOR = 2**20


def validate(case, result, recipe):
    points = result["checkpoints"]
    if len(points) != recipe["rounds"] + 1:
        raise ValueError(f"{case}: missing growth checkpoints")
    for number, point in enumerate(points):
        expected = recipe["records"] * (1 + number if case == "insert" else 1)
        if point["round"] != number or point["live_records"] != expected:
            raise ValueError(f"{case}: invalid round or live record count")
        values = [point[k] for k in ("idle_memory_bytes", "disk_before_compaction_bytes", "compacted_disk_bytes")]
        idle = point["idle_memory_samples"]
        if len(idle) < 2 or any(not math.isfinite(x) or x <= 0 for x in values + idle):
            raise ValueError(f"{case}: invalid resource samples")
        receipt = point["compaction"]
        if (receipt.get("executed") is not True or receipt.get("dry_run") is not False
                or receipt.get("reclaim_deferred") is not False
                or receipt.get("reclaim_remaining") is not False or receipt.get("reclaim_error")):
            raise ValueError(f"{case}: incomplete compaction")
    metrics = result["metrics"]
    if metrics["live_records"] != points[-1]["live_records"]:
        raise ValueError(f"{case}: final record count differs")
    if any(not math.isfinite(metrics[name]) for name in MEASURES):
        raise ValueError(f"{case}: nonfinite resource result")
    if metrics["peak_memory_bytes"] < max(p["idle_memory_bytes"] for p in points):
        raise ValueError(f"{case}: memory peak is smaller than the retained memory")
    for name, source in (("memory_growth_bytes_per_round", "idle_memory_bytes"),
                         ("disk_growth_bytes_per_round", "compacted_disk_bytes")):
        if metrics[name] != slope(points, source):
            raise ValueError(f"{case}: invalid growth slope")
    allocated = ALLOCATED_MEASURES[:2]
    if any(name in metrics for name in ALLOCATED_MEASURES) or any(
            name in point for point in points for name in allocated):
        if any(name not in metrics for name in ALLOCATED_MEASURES) or any(
                name not in point for point in points for name in allocated):
            raise ValueError(f"{case}: incomplete allocated disk samples")
        if any(not math.isfinite(point[name]) or point[name] < 0 for point in points for name in allocated):
            raise ValueError(f"{case}: invalid allocated disk samples")
        if any(not math.isfinite(metrics[name]) for name in ALLOCATED_MEASURES):
            raise ValueError(f"{case}: nonfinite allocated disk result")
        expected = {name: points[-1][name] for name in allocated}
        source = "compacted_allocated_disk_bytes"
        expected.update(allocated_disk_growth_bytes=points[-1][source] - points[0][source],
                        allocated_disk_growth_bytes_per_round=slope(points, source),
                        compacted_allocated_disk_bytes_per_record=points[-1][source] / metrics["live_records"])
        if any(metrics[name] != value for name, value in expected.items()):
            raise ValueError(f"{case}: invalid allocated disk result")


def percent(base, candidate):
    return 100 * (candidate / base - 1) if base is not None and candidate is not None and base > 0 else None


def advisory(base, candidate, floor):
    # A zero or negative base growth can still regress. Require an absolute
    # excess as well as 20% of the nonnegative reference to suppress small noise.
    return candidate - base >= floor and candidate >= max(0, base) * (1 + PERCENT / 100)


def compare(output, manifest):
    values = {label: {} for label in ("baseline", "candidate")}
    for run in manifest["runs"]:
        if not run["complete"]:
            raise ValueError("A growth measurement did not complete")
        cases = json.loads((output / run["directory"] / "growth.json").read_text())
        if set(cases) != set(CASES):
            raise ValueError("The growth cases differ from the recipe")
        for case in CASES:
            validate(case, cases[case], manifest["recipe"]["growth"])
        if run["pair"] in values[run["label"]]:
            raise ValueError("The growth results contain a duplicate pair")
        values[run["label"]][run["pair"]] = cases
    pairs = set(range(manifest["recipe"]["pairs"]))
    if any(set(v) != pairs for v in values.values()):
        raise ValueError("The growth measurements lack a complete pair")
    comparison = {}
    measures = (*MEASURES, *ALLOCATED_MEASURES)
    for case in CASES:
        item = {}
        for label, runs in values.items():
            samples = [runs[pair][case]["metrics"] for pair in sorted(pairs)]
            item[label] = {name: statistics.median(s[name] for s in samples)
                           if all(s.get(name) is not None for s in samples) else None
                           for name in (*measures, "live_records")}
        base, candidate = item["baseline"], item["candidate"]
        item["changes_percent"] = {name: percent(base[name], candidate[name]) for name in measures}
        item["paired_changes_percent"] = {
            name: [percent(values["baseline"][p][case]["metrics"].get(name),
                           values["candidate"][p][case]["metrics"].get(name)) for p in sorted(pairs)]
            for name in measures}
        item["advisories"] = [name for name, floor in (
            ("peak_memory_bytes", MEMORY_FLOOR), ("idle_memory_bytes", MEMORY_FLOOR),
            ("memory_growth_bytes", MEMORY_FLOOR), ("compacted_disk_bytes", DISK_FLOOR),
            ("disk_growth_bytes", DISK_FLOOR)) if advisory(base[name], candidate[name], floor)]
        comparison[case] = item
    return comparison


def change(value):
    return f"{value:+.1f}%" if value is not None else "n/a (base ≤ 0)"


def render(manifest, comparison):
    recipe = manifest["recipe"]["growth"]
    lines = ["# LastDB disk and memory growth — advisory", "",
             f"Profile: **{manifest['profile']}**. Pairs: **{manifest['recipe']['pairs']}**. "
             f"Seed records: **{recipe['records']}**. Rounds: **{recipe['rounds']}**.", "",
             f"Base: `{manifest['binaries']['baseline']['revision']}`  ",
             f"Candidate: `{manifest['binaries']['candidate']['revision']}`  ",
             f"Driver: `{manifest['driver_revision']}`", "",
             "Values are medians across alternating pairs. Arrows show base → candidate.",
             "Growth is the final checkpoint minus the initial checkpoint. The slope uses all checkpoints.", "",
             "| Case | Live records | Peak memory MiB | Idle memory MiB | Idle change | Memory growth MiB | Memory MiB/round |",
             "|---|---:|---:|---:|---:|---:|---:|"]

    def pair(item, name, divisor=2**20):
        return " → ".join("unreported" if item[label].get(name) is None else f"{item[label][name] / divisor:.2f}"
                          for label in ("baseline", "candidate"))

    for case, item in comparison.items():
        lines.append(f"| {case} | {int(item['candidate']['live_records'])} | {pair(item, 'peak_memory_bytes')} | "
                     f"{pair(item, 'idle_memory_bytes')} | {change(item['changes_percent']['idle_memory_bytes'])} | "
                     f"{pair(item, 'memory_growth_bytes')} | {pair(item, 'memory_growth_bytes_per_round')} |")
    lines.extend(["", "| Case | Before compaction MiB | After compaction MiB | Disk change | Bytes/live record | Disk growth MiB | Disk MiB/round |",
                  "|---|---:|---:|---:|---:|---:|---:|"])
    for case, item in comparison.items():
        lines.append(f"| {case} | {pair(item, 'disk_before_compaction_bytes')} | {pair(item, 'compacted_disk_bytes')} | "
                     f"{change(item['changes_percent']['compacted_disk_bytes'])} | "
                     f"{pair(item, 'compacted_disk_bytes_per_record', 1)} | "
                     f"{pair(item, 'disk_growth_bytes')} | {pair(item, 'disk_growth_bytes_per_round')} |")
    lines.extend(["", "| Case | Allocated before MiB | Allocated after MiB | Allocated change | Allocated bytes/live record | Allocated growth MiB | Allocated MiB/round |",
                  "|---|---:|---:|---:|---:|---:|---:|"])
    for case, item in comparison.items():
        name = "compacted_allocated_disk_bytes"
        delta = change(item["changes_percent"].get(name)) if all(
            item[label].get(name) is not None for label in ("baseline", "candidate")) else "unreported"
        lines.append(f"| {case} | {pair(item, 'allocated_disk_before_compaction_bytes')} | "
                     f"{pair(item, name)} | {delta} | {pair(item, 'compacted_allocated_disk_bytes_per_record', 1)} | "
                     f"{pair(item, 'allocated_disk_growth_bytes')} | {pair(item, 'allocated_disk_growth_bytes_per_round')} |")
    lines.extend(["", "Fixed-read and fixed-update keep the record count constant. Inserts add one seed-sized batch per round.",
                  "Each checkpoint requires a durable flush, full compaction, and complete reclaim with zero grace.",
                  f"Idle memory is the median over {recipe['idle_seconds']} seconds after compaction.",
                  "Memory peaks include work, compaction, and idle time. Seed and process startup stay outside the peak.",
                  "The first disk section counts file lengths across the generated home.",
                  "Allocated disk size sums each file's st_blocks × 512 bytes during the same bounded walk at each checkpoint.",
                  "Allocated sizes do not measure unique shared blocks or device writes. Short-lived disk peaks are unmeasured.",
                  "Older artifacts without allocated samples show unreported. Allocated measures do not change the advisory policy.",
                  "Automatic maintenance stays enabled. Explicit compaction measures retained space, not normal retention policy.",
                  "A notice requires at least 20% more resource use and an excess of 16 MiB memory or 1 MiB disk.",
                  "The same case and measure must cross both limits again in a second complete comparison before a notice.",
                  "Zero or negative baseline growth uses zero as its percentage reference; the absolute excess still applies.",
                  "All changes remain visible. Resource regressions never fail the check. Missing or invalid measurements do.",
                  "CAUTION: Shared machines and memory allocators vary. A positive slope alone does not establish a leak.",
                  "The artifact retains every checkpoint, compaction receipt, 200 ms memory sample, and per-pair change."])
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--job-status", required=True)
    parser.add_argument("--confirmation-status", required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    try:
        manifest = json.loads((args.output / "benchmark.json").read_text())
        if not manifest["complete"] or args.job_status != "success":
            raise ValueError("The resource growth measurement did not complete")
        comparison = compare(args.output, manifest)
        save(args.output / "comparison.json", comparison)
        decision = confirm(args.output, manifest, comparison, compare, "growth", args.confirmation_status)
        report, complete = render(manifest, comparison) + confirmation_lines(decision), True
    except (OSError, ValueError, KeyError, TypeError, ZeroDivisionError) as failure:
        report = ("# LastDB disk and memory growth — incomplete\n\n"
                  f"{type(failure).__name__}: {failure}\n\n"
                  "This run supplies no valid growth comparison. Inspect the run artifact.\n")
        complete = False
    (args.output / "summary.md").write_text(report)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
            stream.write(report)
    print(report)
    if complete:
        notices(decision)
    return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())
