#!/usr/bin/env python3
"""Publish advisory resource growth comparisons and the required v2 disk-size budget."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import statistics

import disk_budget
from growth import CASES, slope
from node import home_format_line, publish_summary, save, validate_breakdown
from report import confirm, confirmation_lines, notices

MEASURES = ("peak_memory_bytes", "idle_memory_bytes", "memory_growth_bytes",
            "memory_growth_bytes_per_round", "disk_before_compaction_bytes", "compacted_disk_bytes",
            "disk_growth_bytes", "disk_growth_bytes_per_round", "compacted_disk_bytes_per_record",
            "whole_home_disk_bytes", "other_home_bytes")
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
        validate_breakdown(point["disk_breakdown_before"])
        validate_breakdown(point["disk_breakdown"])
        expected = recipe["records"] * (1 + number if case == "insert" else 1)
        if point["round"] != number or point["live_records"] != expected:
            raise ValueError(f"{case}: invalid round or live record count")
        values = [point[k] for k in ("idle_memory_bytes", "disk_before_compaction_bytes", "compacted_disk_bytes")]
        idle = point["idle_memory_samples"]
        if len(idle) < 2 or any(not math.isfinite(x) or x <= 0 for x in values + idle):
            raise ValueError(f"{case}: invalid resource samples")
        receipt = point["compaction"]
        settled = receipt.get("settled_seconds")
        if (receipt.get("format") != 2 or receipt.get("automatic_merges") is not True
                or type(settled) is not float or not math.isfinite(settled) or settled < 3):
            raise ValueError(f"{case}: the v2 disk size did not settle")
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
    if any(not math.isfinite(point[name]) or point[name] < 0 for point in points for name in allocated):
        raise ValueError(f"{case}: invalid allocated disk samples")
    if any(not math.isfinite(metrics[name]) for name in ALLOCATED_MEASURES):
        raise ValueError(f"{case}: nonfinite allocated disk result")
    source = "compacted_allocated_disk_bytes"
    expected = {
        **{name: points[-1][name] for name in allocated},
        "allocated_disk_growth_bytes": points[-1][source] - points[0][source],
        "allocated_disk_growth_bytes_per_round": slope(points, source),
        "compacted_allocated_disk_bytes_per_record": points[-1][source] / metrics["live_records"],
    }
    if any(metrics[name] != value for name, value in expected.items()):
        raise ValueError(f"{case}: invalid allocated disk result")


def _percent(base, candidate):
    return 100 * (candidate / base - 1) if base is not None and candidate is not None and base > 0 else None


def _advisory(base, candidate, floor):
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
    comparison, measures = {}, (*MEASURES, *ALLOCATED_MEASURES)
    for case in CASES:
        item = {}
        for label, runs in values.items():
            samples = [runs[pair][case]["metrics"] for pair in sorted(pairs)]
            item[label] = {name: statistics.median(s[name] for s in samples)
                           for name in (*measures, "live_records")}
        base, candidate = item["baseline"], item["candidate"]
        item["changes_percent"] = {name: _percent(base[name], candidate[name]) for name in measures}
        item["paired_changes_percent"] = {
            name: [_percent(values["baseline"][p][case]["metrics"][name],
                            values["candidate"][p][case]["metrics"][name]) for p in sorted(pairs)]
            for name in measures}
        item["advisories"] = [name for name, floor in (
            ("peak_memory_bytes", MEMORY_FLOOR), ("idle_memory_bytes", MEMORY_FLOOR),
            ("memory_growth_bytes", MEMORY_FLOOR), ("compacted_disk_bytes", DISK_FLOOR),
            ("disk_growth_bytes", DISK_FLOOR)) if _advisory(base[name], candidate[name], floor)]
        comparison[case] = item
    return comparison


def render(output, manifest, comparison):
    recipe = manifest["recipe"]["growth"]
    change = lambda value: f"{value:+.1f}%" if value is not None else "n/a (base ≤ 0)"
    pair = lambda item, name, divisor=2**20: " → ".join(
        f"{item[label][name] / divisor:.2f}" for label in ("baseline", "candidate"))
    lines = ["# LastDB disk and memory growth — advisory", "",
             f"Profile: **{manifest['profile']}**. Pairs: **{manifest['recipe']['pairs']}**. "
             f"Seed records: **{recipe['records']}**. Rounds: **{recipe['rounds']}**.", "",
             f"Base: `{manifest['binaries']['baseline']['revision']}`  ",
             f"Candidate: `{manifest['binaries']['candidate']['revision']}`  ",
             f"Driver: `{manifest['driver_revision']}`", "", home_format_line(output, manifest), "",
             "Values are medians across alternating pairs. Arrows show base → candidate.",
             "Growth is the final checkpoint minus the initial checkpoint. The slope uses all checkpoints.", "",
             "| Case | Live records | Peak memory MiB | Idle memory MiB | Idle change | Memory growth MiB | Memory MiB/round |",
             "|---|---:|---:|---:|---:|---:|---:|"]
    for case, item in comparison.items():
        lines.append(f"| {case} | {int(item['candidate']['live_records'])} | {pair(item, 'peak_memory_bytes')} | "
                     f"{pair(item, 'idle_memory_bytes')} | {change(item['changes_percent']['idle_memory_bytes'])} | "
                     f"{pair(item, 'memory_growth_bytes')} | {pair(item, 'memory_growth_bytes_per_round')} |")
    lines.extend(["", "| Case | Database before MiB | Database after MiB | Disk change | Bytes/live record | Disk growth MiB | Disk MiB/round |",
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
        delta = change(item["changes_percent"][name])
        lines.append(f"| {case} | {pair(item, 'allocated_disk_before_compaction_bytes')} | "
                     f"{pair(item, name)} | {delta} | {pair(item, 'compacted_allocated_disk_bytes_per_record', 1)} | "
                     f"{pair(item, 'allocated_disk_growth_bytes')} | {pair(item, 'allocated_disk_growth_bytes_per_round')} |")
    lines.extend(["", "| Case | Other home MiB, base → candidate | Whole home MiB, base → candidate |",
                  "|---|---:|---:|"])
    for case, item in comparison.items():
        lines.append(f"| {case} | {pair(item, 'other_home_bytes')} | {pair(item, 'whole_home_disk_bytes')} |")
    lines.extend(["", "Database figures include data/. Other home files and the whole home stay in their own columns.",
                  "Every checkpoint records file lengths and allocated blocks for each category and the whole home.",
                  "Fixed-read and fixed-update keep the record count constant. Inserts add one seed-sized batch per round.",
                  "Each checkpoint requires a durable flush. A v2 home has no explicit compaction.",
                  "Its checkpoint waits for its automatic run merges:",
                  "the disk size must stay constant for three seconds, within 90 seconds. That size is the after value.",
                  f"Idle memory is the median over {recipe['idle_seconds']} seconds after the v2 wait.",
                  "Memory peaks include work, the v2 wait, and idle time. Seed and process startup stay outside the peak.",
                  "The first disk section counts file lengths across the generated home.",
                  "Allocated disk size sums each file's st_blocks × 512 bytes during the same bounded walk at each checkpoint.",
                  "Allocated sizes do not measure unique shared blocks or device writes. Short-lived disk peaks are unmeasured.",
                  "Allocated measures do not change the advisory policy.",
                  "Automatic maintenance stays enabled. Explicit compaction measures retained space, not normal retention policy.",
                  "A notice requires at least 20% more resource use and an excess of 16 MiB memory or 1 MiB disk.",
                  "The same case and measure must cross both limits again in a second complete comparison before a notice.",
                  "Zero or negative baseline growth uses zero as its percentage reference; the absolute excess still applies.",
                  "All changes remain visible. Resource regressions never fail the check. Missing or invalid measurements do.",
                  "CAUTION: Shared machines and memory allocators vary. A positive slope alone does not establish a leak.",
                  "The artifact retains every checkpoint, compaction receipt, 200 ms memory sample, and per-pair change."])
    return "\n".join(lines) + "\n"


def compare_disk_budget(output, manifest):
    recipe = manifest["recipe"]
    if recipe.get("disk_budget") is not True or recipe.get("pairs") != 1:
        raise ValueError("The disk budget requires one pair")
    values = {label: {} for label in ("baseline", "candidate")}
    for run in manifest["runs"]:
        if not run["complete"]:
            raise ValueError("A disk budget measurement did not complete")
        directory = output / run["directory"]
        cases = json.loads((directory / "growth.json").read_text())
        if set(cases) != set(disk_budget.CASES):
            raise ValueError("The disk budget cases differ from the recipe")
        deleted = json.loads((directory / "delete/growth.json").read_text())
        cleanup = json.loads((directory / "delete/cleanup.json").read_text())
        if (deleted != {"delete": cases["delete"]}
                or cleanup.get("daemon_stopped") is not True or not cleanup.get("removed")):
            raise ValueError("The separate delete case lacks matching results and complete cleanup")
        for case in disk_budget.CASES:
            disk_budget.validate(case, cases[case], recipe["growth"])
        if run["pair"] in values[run["label"]]:
            raise ValueError("The disk budget contains a duplicate pair")
        values[run["label"]][run["pair"]] = cases
    if any(set(runs) != {0} for runs in values.values()):
        raise ValueError("The disk budget lacks its single complete pair")
    comparison = {}
    for case in disk_budget.CASES:
        item = {label: {name: runs[0][case]["metrics"][name] for name in disk_budget.MEASURES}
                for label, runs in values.items()}
        item["paired_changes_percent"] = {
            name: [100 * (item["candidate"][name] / item["baseline"][name] - 1) if item["baseline"][name] else None]
            for name in disk_budget.MEASURES}
        item["advisories"] = disk_budget.advisories(item["baseline"], item["candidate"])
        comparison[case] = item
    return comparison


def _headroom_lines(comparison):
    rows = ["", "## Room below the total 1.5× limit", "",
            "At each case's highest settled ratio. Negative room means excess.", "",
            "| Case | Maximum ratio | Room MiB | Room % of limit |", "|---|---:|---:|---:|"]
    for case, item in comparison.items():
        value = item["candidate"]
        rows.append(f"| {case} | {value['max_disk_to_logical_ratio']:.6f}× | "
                    f"{value['limit_headroom_bytes'] / 2**20:.3f} | {value['limit_headroom_percent']:.2f}% |")
    deleted = comparison["delete"]["candidate"]
    rows.extend(["", f"Delete corpus: {deleted['starting_stored_record_bytes'] / 2**20:.3f} MiB logical; "
                 f"deleted {deleted['deleted_records']} of {deleted['starting_live_records']} records; "
                 f"{deleted['stored_record_bytes'] / 2**20:.3f} MiB logical remains."])
    return "\n".join(rows) + "\n"


def _disk_budget_notices(comparison):
    for case, item in comparison.items():
        ratio = item["candidate"]["max_disk_to_logical_ratio"]
        if ratio > disk_budget.LIMIT:
            print(f"::error title=Required disk size limit exceeded::{case}: {ratio:.6f}x exceeds "
                  f"{disk_budget.LIMIT}x total. This single-pass result blocks merge.")
        for name in item["advisories"]:
            print(f"::warning title=Disk growth advisory::{case}: {name}; "
                  f"{item['baseline'][name]} -> {item['candidate'][name]} bytes. This does not block merge.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("growth", "disk-budget"), default="growth")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--job-status", required=True)
    parser.add_argument("--confirmation-status")
    args = parser.parse_args()
    if args.mode == "disk-budget":
        exceeded = False
        try:
            manifest = json.loads((args.output / "benchmark.json").read_text())
            if not manifest["complete"] or args.job_status != "success":
                raise ValueError("The disk budget measurement did not complete")
            comparison = compare_disk_budget(args.output, manifest)
            save(args.output / "comparison.json", comparison)
            exceeded = any(i["candidate"]["max_disk_to_logical_ratio"] > disk_budget.LIMIT for i in comparison.values())
            report = ("# LastDB v2 disk size budget — required 1.5× total limit\n\n"
                      f"Required limit: **{'FAIL' if exceeded else 'PASS'}**. One pair; no repeat.\n\n"
                      f"Profile: **{manifest['profile']}**.  \n"
                      f"Base: `{manifest['binaries']['baseline']['revision']}`  \n"
                      f"Candidate: `{manifest['binaries']['candidate']['revision']}`  \n"
                      f"Driver: `{manifest['driver_revision']}`  \n"
                      + home_format_line(args.output, manifest) + "\n"
                      + disk_budget.render(comparison) + _headroom_lines(comparison))
            complete = True
        except (OSError, ValueError, KeyError, TypeError, ZeroDivisionError) as failure:
            report = f"# LastDB v2 disk size budget — incomplete\n\n{type(failure).__name__}: {failure}\n"
            complete = False
        publish_summary(args.output, report)
        if complete:
            _disk_budget_notices(comparison)
        return 0 if complete and not exceeded else 1
    if args.confirmation_status is None:
        parser.error("--confirmation-status is required in growth mode")
    try:
        manifest = json.loads((args.output / "benchmark.json").read_text())
        if not manifest["complete"] or args.job_status != "success":
            raise ValueError("The resource growth measurement did not complete")
        comparison = compare(args.output, manifest)
        save(args.output / "comparison.json", comparison)
        decision = confirm(args.output, manifest, comparison, compare, "growth", args.confirmation_status)
        report, complete = render(args.output, manifest, comparison) + confirmation_lines(decision), True
    except (OSError, ValueError, KeyError, TypeError, ZeroDivisionError) as failure:
        report = ("# LastDB disk and memory growth — incomplete\n\n"
                  f"{type(failure).__name__}: {failure}\n\n"
                  "This run supplies no valid growth comparison. Inspect the run artifact.\n")
        complete = False
    publish_summary(args.output, report)
    if complete:
        notices(decision)
    return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())
