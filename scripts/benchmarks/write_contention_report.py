#!/usr/bin/env python3
"""Retain native wait/commit clocks and render the CI writer investigation."""

import argparse
from bisect import bisect_left
import json
import os
from pathlib import Path
from statistics import mean, median

from node import save


def summarize(events):
    names = {name for event in events for name in event.get("phases_us", {})}
    return {name: {"reported": sum(name in e.get("phases_us", {}) for e in events),
                   "mean_us": mean(e["phases_us"][name] for e in events if name in e.get("phases_us", {})),
                   "median_us": median(e["phases_us"][name] for e in events if name in e.get("phases_us", {}))}
            for name in sorted(names)}


def native_events(directory, cases):
    path = directory / "node.log"
    if path.stat().st_size > 256 * 2**20:
        raise ValueError("The investigation log exceeds its 256 MiB bound")
    events = []
    with path.open() as source:
        for line in source:
            if "LASTDB_SNAPSHOT_TIMING " in line:
                events.append(json.loads(line.split("LASTDB_SNAPSHOT_TIMING ", 1)[1]))
    for case in cases:
        if not case["native_timing"]:
            continue
        first, last = case["case_started_unix_us"], case["case_ended_unix_us"]
        selected = [e for e in events if first <= e["started_us"] <= e["ended_us"] <= last]
        scopes = [e for e in selected if e["kind"] == "scope" and e["background"] is False]
        sequences = sorted(e["sequence"] for e in scopes)
        if len(scopes) != case["requests"] or len(set(sequences)) != len(sequences):
            raise ValueError("Native scopes do not cover all actual measured write requests")
        commits = []
        for event in selected:
            if event["kind"] != "commit" or not event["ok"]:
                continue
            position = bisect_left(sequences, event["first_sequence"])
            if position < len(sequences) and sequences[position] <= event["sequence"]:
                commits.append(event)
        if any(sum(c["first_sequence"] <= seq <= c["sequence"] for c in commits) != 1 for seq in sequences):
            raise ValueError("A write scope lacks one distinct durable publication")
        case["native"] = {"scopes": len(scopes), "commits": len(commits),
                          "requests_per_commit": mean(c["requests"] for c in commits),
                          "scope_phases": summarize(scopes), "commit_phases": summarize(commits),
                          "commit_mean_us": mean(c["total_us"] for c in commits),
                          "commit_slots_include_background": True,
                          "scope_phase_totals_overlap": False}
        save(directory / f"{case['name']}.json", case)


def milliseconds(phases, name):
    value = phases.get(name, {}).get("mean_us")
    return f"{value / 1000:.3f}" if value is not None else "unreported"


def render(output):
    data = json.loads((output / "investigation.json").read_text())
    if not data["complete"] or len(data["homes"]) != 7 or not all(h["complete"] for h in data["homes"]):
        raise ValueError(f"Incomplete writer investigation: {data.get('error', 'missing homes')}")
    lines = ["# Writer CPU and wait investigation", "",
             f"Controller: `{data['controller_revision']}`. Historical driver: `{data['driver_revision']}`.",
             f"Each fresh home has {data['records']} generated records. CPU capacity: {data['host_cpus']} cores.",
             "A and B are the exact archived programs from the reported warning. The final A/shared home repeats the same executable.",
             "Every client owns different keys. Separate schemas have disjoint canonical field identities.",
             "All requests require durable acknowledgements and verified final values. Search stays off.",
             "These focused homes do not replay the full nightly history. Native logging can affect timings.",
             "CPU counters describe one whole case, without adding overlapping per-request CPU intervals.",
             "Native admission includes the global preparation gate and waits on a sealed durable queue.",
             "Missing native phases or disabled kernel scheduler counters remain unreported. Host pressure is correlation only.", "",
             "| Home / case | Workers | Clients / batch | p95 ms | Records/s | Daemon CPU cores | Client CPU cores | Native admission ms | Logical preparation ms | Durable wait ms | Requests/commit |",
             "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    physical = ["", "| Home / case | Commits | Commit mean ms | Largest physical phases ms |", "|---|---:|---:|---|"]
    for home in data["homes"]:
        directory = output / home["directory"]
        cleanup = json.loads((directory / "cleanup.json").read_text())
        if not cleanup["daemon_stopped"] or not cleanup["removed"]:
            raise ValueError("A generated home lacks cleanup evidence")
        for case in home["cases"]:
            metrics = case["metrics"]
            native = case.get("native", {})
            phases = native.get("scope_phases", {})
            process = case["process"]
            label = f"{home['directory']} / {case['name']}"
            request_count = native.get("requests_per_commit")
            group_size = f"{request_count:.2f}" if request_count is not None else "unreported"
            lines.append(f"| {label} | {case['workers']} | {case['clients']} / {case['batch_records']} | "
                         f"{metrics['p95_ms']:.3f} | {metrics['records_per_second']:.2f} | "
                         f"{process['daemon']['average_cpu_cores']:.3f} | {process['client']['average_cpu_cores']:.3f} | "
                         f"{milliseconds(phases, 'admission_us')} | {milliseconds(phases, 'logical_prepare_us')} | "
                         f"{milliseconds(phases, 'durable_wait_us')} | {group_size} |")
            if native:
                largest = sorted(native["commit_phases"].items(), key=lambda pair: pair[1]["mean_us"], reverse=True)[:6]
                details = ", ".join(f"{n}={v['mean_us']/1000:.3f}" for n, v in largest)
                physical.append(f"| {label} | {native['commits']} | {native['commit_mean_us']/1000:.3f} | {details} |")
    return "\n".join(lines + physical) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        report, rc = render(args.output), 0
    except (OSError, ValueError, KeyError, TypeError) as error:
        report, rc = f"# Writer investigation — incomplete\n\n{type(error).__name__}: {error}\n", 1
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "summary.md").write_text(report)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as summary:
        summary.write(report)
    print(report)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
