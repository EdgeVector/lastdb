#!/usr/bin/env python3
"""Publish separate native diagnostics with explicit physical-commit counts."""

import argparse
import json
import math
import os
from pathlib import Path


def largest(phases):
    values = sorted(phases.items(), key=lambda item: item[1]["mean"], reverse=True)[:5]
    return ", ".join(f"{name}={value['mean'] / 1000:.3f} ({value['reported']} records)"
                     for name, value in values) or "unreported"


def write_sizes(case):
    counts = case.get("physical_counts", {})

    def mean(name):
        item = counts.get(name)
        if item is None:
            return None
        value = item["mean"]
        if item["reported"] != case["distinct_physical_commits"] or not math.isfinite(value) or value < 0:
            raise ValueError(f"Incomplete or invalid physical byte counter: {name}")
        return value

    names = ("page_output_requested_write_bytes", "locator_parent_requested_write_bytes",
             "locator_parent_omitted_zero_bytes", "locator_parent_sparse_writes")
    values = [mean(name) for name in names]
    pages = mean("locator_parent_output_pages")
    parent_image = pages * 16384 if pages is not None else None
    if any(value is not None for value in values):
        if any(value is None for value in values) or parent_image is None:
            raise ValueError("The requested-write byte counters are incomplete")
        requested, parent, omitted, sparse = values
        if not math.isclose(parent + omitted, parent_image, rel_tol=1e-12, abs_tol=1e-6):
            raise ValueError("Parent requested bytes plus omitted zeros differ from logical page bytes")
        if parent > requested or sparse > pages:
            raise ValueError("The parent write counters exceed their output totals")
    else:
        requested = parent = omitted = sparse = None
    metadata, maps = mean("metadata_pages"), mean("block_map_output_pages")
    image = (metadata + maps) * 16384 if metadata is not None and maps is not None else None
    if requested is not None and image is not None and requested > image and not math.isclose(
            requested, image, rel_tol=1e-12, abs_tol=1e-6):
        raise ValueError("Requested write bytes exceed the six output classes' logical bytes")

    def number(value, divisor=2**20):
        return "unreported" if value is None else f"{value / divisor:.3f}"

    return " | ".join(number(value) for value in (image, requested, parent_image, parent, omitted)) + " | " + number(sparse, 1)


def render(output):
    manifest = json.loads((output / "benchmark.json").read_text())
    if not manifest["complete"] or not manifest["recipe"]["native_timing"]:
        raise ValueError("The separate native diagnostic campaign is incomplete")
    lines = ["# Native write diagnostics — separate from ordinary latency", "",
             f"Driver: `{manifest['driver_revision']}`. Pairs: {manifest['recipe']['pairs']}.",
             f"Requested socket workers (0 = product default): {manifest['recipe'].get('uds_workers', 'unreported')}",
             "Native timing is enabled only for these generated CI homes.",
             "Instrumentation can change latency; use the ordinary campaign for the speed comparison.",
             "Scope means are per native request; physical means are per distinct commit.",
             "A physical commit can serve several requests, including background work.",
             "Nested phase values overlap. Do not add them or assign one shared commit to each request.", "",
             "| Run / case | Write scopes | Physical commits | Scope mean ms | Largest scope phases ms |",
             "|---|---:|---:|---:|---|"]
    physical_lines = ["", "| Run / case | Commit mean ms | Largest physical phases ms |",
                      "|---|---:|---|"]
    byte_lines = ["", "## Encoded page writes per distinct physical commit", "",
                  "Logical image bytes count all 16,384 bytes per page. Requested bytes count bytes sent to successful writes.",
                  "The six output classes are locator leaves and parents, directory leaves and parents, key overflow pages, and block maps.",
                  "They exclude map-header rewrites, receipts, CURRENT, lifetime references, and payload frames.",
                  "These counters do not measure allocated disk blocks, filesystem metadata, or device writes. Missing old counters show unreported.",
                  "Parent requested bytes plus omitted zero bytes must equal parent logical image bytes.", "",
                  "| Run / case | Output image MiB | Output requested MiB | Parent image MiB | Parent requested MiB | Parent omitted zeros MiB | Sparse parent writes |",
                  "|---|---:|---:|---:|---:|---:|---:|"]
    tails = ["", "## Exact slow write requests", "",
             "Each request joins to an enclosed scope and one distinct physical commit by sequence and time.",
             "Shared physical commits retain their request count; their cost is not assigned to every request.", ""]
    for run in manifest["runs"]:
        if not run["complete"]:
            raise ValueError("A native diagnostic run did not complete")
        data = json.loads((output / run["directory"] / "native-phases.json").read_text())
        if not data["complete"]:
            raise ValueError("Native phase coverage is incomplete")
        for name in ("insert/1", "update/1", "update/128", "concurrent-write/1", "concurrent-write/4",
                     "concurrent-write/32", "concurrent-mixed/32"):
            case = data["cases"][name]
            label = f"{run['directory']} / {name}"
            lines.append(f"| {label} | {case['scopes']} | {case['distinct_physical_commits']} | "
                         f"{case['scope_mean_us'] / 1000:.3f} | {largest(case['scope_phases_us'])} |")
            physical_lines.append(f"| {label} | {case['physical_mean_us'] / 1000:.3f} | "
                                  f"{largest(case['physical_phases_us'])} |")
            byte_lines.append(f"| {label} | {write_sizes(case)} |")
            for request in case.get("slowest_requests", []):
                tails.extend([f"### {label}: {request['request_id']}", "", "```json",
                              json.dumps(request, sort_keys=True), "```", ""])
    return "\n".join(lines + physical_lines + byte_lines + tails) + "\n"


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
