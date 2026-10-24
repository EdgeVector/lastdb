#!/usr/bin/env python3
"""Publish advisory performance changes. Never disguise an incomplete run."""

import argparse
import json
import os
from pathlib import Path
import statistics

from node import save
from phases import phase_lines

MEASURES = ("p50_ms", "p95_ms", "p99_ms", "requests_per_second", "records_per_second")


def compare(output, manifest):
    metrics = {label: {} for label in ("baseline", "candidate")}
    for run in manifest["runs"]:
        if not run["complete"]:
            raise ValueError("A measurement did not complete")
        metrics[run["label"]][run["pair"]] = json.loads((output / run["directory"] / "metrics.json").read_text())
    pairs = set(range(manifest["recipe"]["pairs"]))
    if any(set(results) != pairs for results in metrics.values()):
        raise ValueError("The measurements lack a complete pair")
    cases = set(metrics["baseline"][0])
    if not cases or any(set(run) != cases for results in metrics.values() for run in results.values()):
        raise ValueError("The measured cases differ across versions or pairs")
    comparison = {}
    for case in sorted(cases):
        item = {}
        for label, results in metrics.items():
            samples = [results[pair][case] for pair in sorted(pairs)]
            if any(s["errors"] or not s["requests"] or s["p95_ms"] is None for s in samples):
                raise ValueError(f"{case} contains an invalid measurement")
            item[label] = {name: statistics.median(s[name] for s in samples) for name in MEASURES}
            item[label].update(requests=sum(s["requests"] for s in samples), errors=0)
        item["changes_percent"] = {
            name: 100 * (item["candidate"][name] / item["baseline"][name] - 1) for name in MEASURES}
        item["paired_p95_changes_percent"] = [
            100 * (metrics["candidate"][p][case]["p95_ms"] / metrics["baseline"][p][case]["p95_ms"] - 1)
            for p in sorted(pairs)]
        comparison[case] = item
    return comparison


def resource_lines(output, manifest):
    lines = ["", "| Run | Sampled daemon memory MiB | Final home MiB | Measured disk growth MiB |",
             "|---|---:|---:|---:|"]
    for run in manifest["runs"]:
        path = output / run["directory"] / "metrics.json"
        if not path.exists():
            continue
        metrics = list(json.loads(path.read_text()).values())
        primary = sorted((m for m in metrics if "disk_after_bytes" in m), key=lambda m: m["sequence"])
        memory = [m["sampled_peak_memory_bytes"] for m in primary if m["sampled_peak_memory_bytes"] is not None]
        if primary:
            peak = f"{max(memory) / 2**20:.1f}" if memory else "unavailable"
            growth = sum(m["disk_growth_bytes"] for m in primary) / 2**20
            lines.append(f"| {run['directory']} | {peak} | {primary[-1]['disk_after_bytes'] / 2**20:.1f} | {growth:.1f} |")
    return lines


def render(output, manifest, comparison):
    lines = ["# LastDB benchmarks — advisory", "",
             f"Profile: **{manifest['profile']}**. Pairs: **{manifest['recipe']['pairs']}**.", "",
             f"Base: `{manifest['binaries']['baseline']['revision']}`  ",
             f"Candidate: `{manifest['binaries']['candidate']['revision']}`  ",
             f"Driver: `{manifest['driver_revision']}`", "",
             "Each number is the median of the per-run measures. Percent changes use the base commit.",
             "Positive response-time changes and negative throughput changes mean slower results.", "",
             "| Case | Base p95 ms | Candidate p50 / p95 / p99 ms | p95 change | Candidate requests/s | Rate change | Errors |",
             "|---|---:|---:|---:|---:|---:|---:|"]
    for case, item in comparison.items():
        base, candidate, change = item["baseline"], item["candidate"], item["changes_percent"]
        latency = " / ".join(f"{candidate[name]:.3f}" for name in ("p50_ms", "p95_ms", "p99_ms"))
        lines.append(f"| {case} | {base['p95_ms']:.3f} | {latency} | {change['p95_ms']:+.1f}% | "
                     f"{candidate['requests_per_second']:.1f} | {change['requests_per_second']:+.1f}% | 0 |")
        if change["p95_ms"] >= 20 or change["requests_per_second"] <= -20:
            print(f"::warning title=Advisory performance change::{case}: p95 {change['p95_ms']:+.1f}%, "
                  f"request rate {change['requests_per_second']:+.1f}%. This does not block merge.")
    lines.extend(resource_lines(output, manifest))
    lines.extend(phase_lines(output, manifest))
    if manifest["recipe"].get("seed_with_baseline"):
        lines.extend(["", "## Actual baseline-home upgrade", "",
                      "The baseline daemon creates every seed record. The measured program reopens that same owned home.",
                      "| Run | Old receipts retired | Known keys read after upgrade | Same receipt on retry |",
                      "|---|---:|---:|---|"])
        for run in manifest["runs"]:
            if run["label"] != "candidate":
                continue
            result = json.loads((output / run["directory"] / "upgrade-complete.json").read_text())
            lines.append(f"| {run['directory']} | {result['retry_cache'].get('legacy_retired', 'unavailable')} | "
                         f"{result['known_keys_read']} | {result['same_receipt_on_retry']} |")
    lines.extend(["", "CAUTION: Shared GitHub machines vary. These results establish no fixed speed guarantee.",
                  "Cold reads miss the LastDB warm set. The operating system file cache stays uncontrolled.",
                  "The throughput includes the client cost. The memory measure excludes seed and startup.",
                  "A 20% change emits an advisory notice. No slowdown causes a failed check.",
                  "Raw samples, receipts, per-pair changes, resource measures, and build logs are in the run artifact.",
                  "Generated homes and identity files stay out of the artifact and leave the runner after shutdown."])
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--job-status", default="success")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    manifest_path = args.output / "benchmark.json"
    try:
        manifest = json.loads(manifest_path.read_text())
        if not manifest["complete"] or args.job_status != "success":
            raise ValueError("The build or measurement did not complete successfully")
        comparison = compare(args.output, manifest)
        save(args.output / "comparison.json", comparison)
        report = render(args.output, manifest, comparison)
        complete = True
    except (OSError, ValueError, KeyError, TypeError, ZeroDivisionError) as failure:
        report = ("# LastDB benchmarks — incomplete\n\n"
                  f"{type(failure).__name__}: {failure}\n\n"
                  "This run supplies no valid speed comparison. Inspect the build logs, result files, and raw samples.\n"
                  "Slowdowns are advisory. A build failure or incomplete measurement remains an error.\n")
        complete = False
    (args.output / "summary.md").write_text(report)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
            stream.write(report)
    print(report)
    return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())
