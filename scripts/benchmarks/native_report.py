#!/usr/bin/env python3
"""Publish separate native diagnostics with explicit physical-commit counts."""

import argparse
import json
import os
from pathlib import Path


def largest(phases):
    values = sorted(phases.items(), key=lambda item: item[1]["mean"], reverse=True)[:5]
    return ", ".join(f"{name}={value['mean'] / 1000:.3f} ({value['reported']} records)"
                     for name, value in values) or "unreported"


def render(output):
    manifest = json.loads((output / "benchmark.json").read_text())
    if not manifest["complete"] or not manifest["recipe"]["native_timing"]:
        raise ValueError("The separate native diagnostic campaign is incomplete")
    lines = ["# Native write diagnostics — separate from ordinary latency", "",
             f"Driver: `{manifest['driver_revision']}`. Pairs: {manifest['recipe']['pairs']}.",
             "Native timing is enabled only for these generated CI homes.",
             "Instrumentation can change latency; use the ordinary campaign for the speed comparison.",
             "Scope means are per native request; physical means are per distinct commit.",
             "A physical commit can serve several requests, including background work.",
             "Nested phase values overlap. Do not add them or assign one shared commit to each request.", "",
             "| Run / case | Write scopes | Physical commits | Scope mean ms | Largest scope phases ms |",
             "|---|---:|---:|---:|---|"]
    physical_lines = ["", "| Run / case | Commit mean ms | Largest physical phases ms |",
                      "|---|---:|---|"]
    tails = ["", "## Exact slow batch-update requests", "",
             "Each request joins to an enclosed scope and one distinct physical commit by sequence and time.",
             "Shared physical commits retain their request count; their cost is not assigned to every request.", ""]
    for run in manifest["runs"]:
        if not run["complete"]:
            raise ValueError("A native diagnostic run did not complete")
        data = json.loads((output / run["directory"] / "native-phases.json").read_text())
        if not data["complete"]:
            raise ValueError("Native phase coverage is incomplete")
        for name in ("update/128", "concurrent-write/1", "concurrent-write/4"):
            case = data["cases"][name]
            label = f"{run['directory']} / {name}"
            lines.append(f"| {label} | {case['scopes']} | {case['distinct_physical_commits']} | "
                         f"{case['scope_mean_us'] / 1000:.3f} | {largest(case['scope_phases_us'])} |")
            physical_lines.append(f"| {label} | {case['physical_mean_us'] / 1000:.3f} | "
                                  f"{largest(case['physical_phases_us'])} |")
            for request in case.get("slowest_requests", []):
                tails.extend([f"### {label}: {request['request_id']}", "", "```json",
                              json.dumps(request, sort_keys=True), "```", ""])
    return "\n".join(lines + physical_lines + tails) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    try:
        report, rc = render(args.output), 0
    except (OSError, ValueError, KeyError, TypeError) as error:
        report, rc = f"# Native diagnostics — incomplete\n\n{type(error).__name__}: {error}\n", 1
    (args.output / "native-summary.md").write_text(report)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
            stream.write(report)
    print(report)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
