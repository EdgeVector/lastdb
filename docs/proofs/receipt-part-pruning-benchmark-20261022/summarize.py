#!/usr/bin/env python3
"""Reuse matched analysis and separately compare the fixed compaction probe sweep."""

import argparse
import json
import math
from pathlib import Path
import sys

sys.dont_write_bytecode = True
from reuse import ROOT, helper_files, original, validate_revisions
from focused import timing_stats
import erase_benchmark


def validate_probes(role, compact, dispatched):
    rows = role["requests_timeline"]
    if role["requests"] != 64 or [row["ordinal"] for row in rows] != list(range(64)):
        raise ValueError("The focused probe stream differs from 64 fixed requests")
    overlaps = []
    for row in rows:
        started, ended = row["started_monotonic_ns"], row["ended_monotonic_ns"]
        overlap = max(started, compact[0]) < min(ended, compact[1])
        nominal = dispatched + int((0.02 + 0.05 * row["ordinal"]) * 1e9)
        if (row["nominal_start_monotonic_ns"] != nominal or ended < started
                or overlap != row["request_intervals_overlap"]
                or not math.isclose(row["elapsed_seconds"], (ended - started) / 1e9, abs_tol=1e-12)
                or not math.isclose(row["start_lateness_seconds"],
                                    (started - row["nominal_start_monotonic_ns"]) / 1e9, abs_tol=1e-12)):
            raise ValueError("A focused probe has inconsistent request times or overlap")
        if overlap:
            overlaps.append(row["elapsed_seconds"])
    if role["overlap_count"] != len(overlaps):
        raise ValueError("The focused overlap count differs from actual request intervals")
    all_ack = timing_stats([row["elapsed_seconds"] for row in rows])
    late = timing_stats([row["start_lateness_seconds"] for row in rows])
    overlapping = timing_stats(overlaps) if overlaps else None
    if (role["all_ack"] != all_ack or role["overlap_ack"] != overlapping or role["start_lateness"] != late):
        raise ValueError("The focused statistics differ from raw request times")
    result = {"overlap_count": len(overlaps)}
    for prefix, values in (("all_ack", all_ack), ("lateness", late), ("overlap_ack", overlapping)):
        if values is not None:
            result.update({f"{prefix}_{key}": values[key] for key in
                           ("samples", "mean_ms", "median_ms", "p95_ms", "max_ms")})
    return result


def read_focus(root, label, pair):
    analyzer = original("summarize")
    folder = root / f"{label}-{pair}"
    summary = analyzer.load(folder / "summary.json")
    value = analyzer.load(folder / "focused-compaction.json")
    if (not summary.get("extra_focus", {}).get("matched_measurements_unchanged")
            or value.get("all_values_verified") is not True or value.get("graceful_reopen_verified") is not True):
        raise ValueError("The focused phase lacks complete values, reopen, or matched-phase separation")
    checkpoints = {row["label"]: row for row in analyzer.load(folder / "focused-checkpoints.json")}
    analyzer.validate_boundary(checkpoints["focused-compact-before"], checkpoints["focused-compact-after"], value)
    cost = analyzer.physical_cost(checkpoints["focused-compact-before"], checkpoints["focused-compact-after"], value)
    compact = (value["compact_started_monotonic_ns"], value["compact_ended_monotonic_ns"])
    if not math.isclose(value["compact_wall_seconds"], (compact[1] - compact[0]) / 1e9, abs_tol=1e-12):
        raise ValueError("The focused compaction wall time differs from its request interval")
    return {"write_inputs": value["write_inputs"], "expected_values_sha256": value["expected_values_sha256"],
            "compact_wall_seconds": value["compact_wall_seconds"], "physical_cost": cost,
            "roles": {role: validate_probes(probes, compact, value["dispatched_monotonic_ns"])
                      for role, probes in value["probes"].items()}}


def compare_focus(runs):
    paired = original("summarize").paired
    for number in (1, 2, 3):
        before, after = runs[f"before-{number}"], runs[f"after-{number}"]
        if (before["write_inputs"] != after["write_inputs"]
                or before["expected_values_sha256"] != after["expected_values_sha256"]):
            raise ValueError("The focused paired write inputs or final values differ")
    result = {"compact_wall_seconds": paired([(runs[f"before-{n}"]["compact_wall_seconds"],
                                               runs[f"after-{n}"]["compact_wall_seconds"]) for n in (1, 2, 3)]),
              "roles": {}}
    for role in ("warm_read", "durable_write"):
        values = [(runs[f"before-{n}"]["roles"][role], runs[f"after-{n}"]["roles"][role]) for n in (1, 2, 3)]
        keys = set.intersection(*(set(value) for pair in values for value in pair))
        result["roles"][role] = {key: paired([(before[key], after[key]) for before, after in values])
                                  for key in sorted(keys)}
    return result


def focus_report(comparison):
    formatted = original("summarize").formatted
    lines = ["", "Separate focused compaction probe sweep", "64 fixed requests per role; nominal starts are 20 ms + 50 ms × ordinal.",
             "Each role uses one sequential stream. A blocked request delays later starts; the recorded lateness includes this delay.",
             "Overlap refers to the compaction request interval. It does not locate the internal write gate.",
             "Per-run percentiles retain their sample counts; requests from different runs are not pooled."]
    for role, metrics in comparison["roles"].items():
        lines.append(role)
        for key in ("overlap_count", "all_ack_median_ms", "all_ack_p95_ms", "all_ack_max_ms",
                    "overlap_ack_median_ms", "overlap_ack_p95_ms", "overlap_ack_max_ms",
                    "lateness_median_ms", "lateness_p95_ms", "lateness_max_ms"):
            if key not in metrics:
                lines.append(f"  {key}: absent because at least one run has no overlapping request")
                continue
            value = metrics[key]
            lines.append(f'  {key}: before {formatted(value["before"], 3)}; after {formatted(value["after"], 3)}')
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--before-revision", required=True)
    parser.add_argument("--after-revision", required=True)
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    builds = validate_revisions(root, args.before_revision, args.after_revision)
    analyzer = original("summarize")
    manifest = analyzer.load(root / "pair-runs.json")
    for label in ("before", "after"):
        for key in ("revision", "binary_sha256", "product_source_clean"):
            if manifest["builds"][label][key] != builds[label][key]:
                raise ValueError("The pair manifest differs from the required source or build receipt")
    previous = sys.argv
    sys.argv = [str(ROOT / "summarize.py"), "--root", str(root)]
    try:
        analyzer.main()
    finally:
        sys.argv = previous
    report = root / "summary.txt"
    text = report.read_text()
    old_heading = "Controlled journal removal comparison: three fresh-home pairs; n=3 per variant.\n"
    if not text.startswith(old_heading) or text.count(old_heading) != 1:
        raise ValueError("The inherited analysis heading differs from its expected exact text")
    report.write_text(text.replace(old_heading, "Controlled receipt part pruning comparison: three fresh-home pairs; n=3 per variant.\n", 1))
    summary = analyzer.load(root / "summary.json")
    helpers = {str(path): analyzer.digest(path) for path in helper_files()}
    if any(run["helpers_sha256"] != helpers for run in summary["runs"].values()):
        raise ValueError("The six runs do not share the exact frozen original and receipt-pruning helpers")
    runs = {f"{label}-{number}": read_focus(root, label, number)
            for number in (1, 2, 3) for label in ("before", "after")}
    comparison = compare_focus(runs)
    summary.update(benchmark_round=3, focused_compaction={"comparisons": comparison, "runs": runs},
                   round3_driver_sha256=analyzer.digest(Path(__file__)),
                   erase_benchmark=erase_benchmark.summarize(root))
    (root / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    with (root / "summary.txt").open("a") as stream:
        stream.write(focus_report(comparison))
        stream.write(erase_benchmark.report(summary["erase_benchmark"]))
    print(json.dumps({"complete": True, "round": 3, "matched_phases": 6, "focused_phase_separate": True}))


if __name__ == "__main__":
    main()
