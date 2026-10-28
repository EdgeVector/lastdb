#!/usr/bin/env python3
"""Report paired writer speed and separate preparation work without a speed gate."""

import argparse
import json
import os
from pathlib import Path
from statistics import median


def render(output):
    data = json.loads((output / "write-gate.json").read_text())
    if not data["complete"] or len(data["homes"]) != 6 or not all(h["complete"] for h in data["homes"]):
        raise ValueError(f"Incomplete writer comparison: {data.get('error', 'missing homes')}")
    lines = ["# Paired 16-writer comparison", "",
             f"Driver: `{data['driver_revision']}`. Each fresh home has {data['records']} generated records.",
             "Six fresh homes form three alternating base/candidate pairs on this runner.",
             "Every client owns different keys in one schema and one hash. Search is off.",
             "Each case completes 2,048 single-record durable RPCs and verifies the final values.",
             "The final pair also records native phases after a restart. Native latency is separate from the ordinary comparison.",
             "All phase averages cover complete foreground storage work; nested preparation stages overlap.",
             "Extra metadata scopes remain explicit. Missing instrumentation is unreported, not zero.", "",
             "| Home / case | P95 ms | Records/s | Daemon CPU cores | Logical prepare mean ms | Reused disk records |",
             "|---|---:|---:|---:|---:|---:|"]
    ordinary = {label: [] for label in ("baseline", "candidate")}
    detail = []
    for home in data["homes"]:
        directory = output / home["directory"]
        cleanup = json.loads((directory / "cleanup.json").read_text())
        if not cleanup["daemon_stopped"] or not cleanup["removed"]:
            raise ValueError("A generated writer home lacks complete cleanup")
        for case in home["cases"]:
            metrics = case["metrics"]
            if case["name"] == "ordinary-16":
                ordinary[home["label"]].append(metrics)
            phases = case.get("native", {}).get("scope_phases", {})
            prepare = phases.get("logical_prepare_us", {}).get("mean_us")
            reuse = case.get("write_reads", {}).get("counts", {}).get("reused_records", {}).get("total")
            prep_text = f"{prepare / 1000:.3f}" if prepare is not None else "unreported"
            lines.append(f"| {home['directory']} / {case['name']} | {metrics['p95_ms']:.3f} | "
                         f"{metrics['records_per_second']:.2f} | {case['process']['daemon']['average_cpu_cores']:.3f} | "
                         f"{prep_text} | {reuse if reuse is not None else 'unreported'} |")
            if "preparation" in case:
                detail.extend(["", f"## {home['directory']} preparation", "", "```json",
                               json.dumps({"native": case["native"], "preparation": case["preparation"],
                                           "write_reads": case["write_reads"]}, indent=2, sort_keys=True), "```"])
    if any(len(values) != 3 for values in ordinary.values()):
        raise ValueError("Ordinary measurements lack three complete pairs")
    lines.extend(["", "| Ordinary median of three cases | Base | Candidate | Change |",
                  "|---|---:|---:|---:|"])
    for field in ("p95_ms", "records_per_second"):
        base, candidate = (median(row[field] for row in ordinary[label]) for label in ("baseline", "candidate"))
        lines.append(f"| {field} | {base:.3f} | {candidate:.3f} | {(candidate/base-1)*100:+.1f}% |")
    lines.extend(["", "Speed changes remain advisory. Completion and verified uploads remain required.",
                  "Fresh homes and the focused history do not replay the full nightly workload."])
    return "\n".join(lines + detail) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        report, code = render(args.output), 0
    except (OSError, ValueError, KeyError, TypeError) as error:
        report, code = f"# Paired writer comparison incomplete\n\n{type(error).__name__}: {error}\n", 1
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "summary.md").write_text(report)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as source:
        source.write(report)
    print(report)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
