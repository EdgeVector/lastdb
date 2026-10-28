"""CI-only first-case memory comparison and same-binary generated-home control."""

import argparse
import json
import os
from pathlib import Path
import re
import statistics

from growth import GrowthWorkload, PROFILES
from node import Node, file_hash, save

PAIRS = 4


def measure(binary, revision, output, condition, label, pair):
    output.mkdir()
    node = Node(binary, revision, output)
    result = {"complete": False, "condition": condition, "label": label,
              "pair": pair, "revision": revision, "binary_sha256": file_hash(binary)}
    try:
        node.start()
        work = GrowthWorkload(node, {"growth": PROFILES["standard"]}, output)
        # This is the unmodified first standard case. Stop before later cases;
        # seed, checkpoints, memory sampler, and maintenance remain unchanged.
        work.case_names = ("fixed-read",)
        work.run()
        data = work.cases["fixed-read"]
        rows = work.samples
        peak = max(rows, key=lambda row: row["memory_bytes"])
        result.update(metrics=data["metrics"], checkpoints=data["checkpoints"], peak_sample=peak,
                      phase_max_bytes={phase: max(row["memory_bytes"] for row in rows if row["phase"] == phase)
                                       for phase in sorted({row["phase"] for row in rows})})
        # The 512 known values are verified by the unchanged seed/checkpoint
        # requests, including all four durable checkpoint boundaries.
        result["verified_records"] = work.live_records
        result["complete"] = True
    except BaseException as error:
        result["error"] = str(error)
        raise
    finally:
        try:
            node.close()
        finally:
            save(output / "result.json", result)
    return result


def compare(runs, condition):
    result = {}
    chosen = [run for run in runs if run["condition"] == condition]
    for label in ("baseline", "candidate"):
        rows = [run for run in chosen if run["label"] == label]
        if len(rows) != PAIRS or any(not run["complete"] for run in rows):
            raise RuntimeError("The memory control lacks all four complete pairs")
        result[label] = {metric: statistics.median(row["metrics"][metric] for row in rows)
                         for metric in ("peak_memory_bytes", "idle_memory_bytes", "memory_growth_bytes")}
    result["peak_change_percent"] = 100 * (result["candidate"]["peak_memory_bytes"] / result["baseline"]["peak_memory_bytes"] - 1)
    result["peak_difference_mib"] = (result["candidate"]["peak_memory_bytes"] - result["baseline"]["peak_memory_bytes"]) / 2**20
    result["per_pair_peak_change_percent"] = []
    for pair in range(PAIRS):
        rows = {run["label"]: run["metrics"]["peak_memory_bytes"] for run in chosen if run["pair"] == pair}
        result["per_pair_peak_change_percent"].append(100 * (rows["candidate"] / rows["baseline"] - 1))
    return result


def summary(output, manifest):
    lines = ["# First standard fixed-read memory: fresh comparison and same-binary control", "",
             "Four alternating pairs per condition. Every run uses a fresh generated512-record home.",
             "The original first growth case, sampler, seed, checkpoints, and maintenance are unchanged.",
             "The same-binary control uses the exact baseline executable on both labelled sides.", "",
             "| Condition | Peak MiB base → candidate | Peak change | Idle MiB base → candidate |",
             "| --- | ---: | ---: | ---: |"]
    for condition, row in manifest["comparisons"].items():
        before, after = row["baseline"], row["candidate"]
        lines.append(f"| {condition} | {before['peak_memory_bytes']/2**20:.3f} → {after['peak_memory_bytes']/2**20:.3f} | "
                     f"{row['peak_change_percent']:+.1f}% | {before['idle_memory_bytes']/2**20:.3f} → {after['idle_memory_bytes']/2**20:.3f} |")
    lines += ["", "| Run | Actual source | Peak MiB / phase / seconds | Checkpoint idle MiB, rounds0–3 |",
              "| --- | --- | ---: | --- |"]
    for run in manifest["runs"]:
        peak = run["peak_sample"]
        points = ", ".join(f"{point['idle_memory_bytes']/2**20:.2f}" for point in run["checkpoints"])
        lines.append(f"| {run['condition']}/{run['pair']}/{run['label']} | {run['revision'][:12]} | "
                     f"{run['metrics']['peak_memory_bytes']/2**20:.2f} / {peak['phase']} / {peak['elapsed_seconds']:.2f} | {points} |")
    lines += ["", "The sampler starts immediately after the seed. Its first peak can retain memory from seed writes.",
              "This control preserves that original window. It removes no opening or high samples and changes no advisory policy.",
              "A same-binary difference cannot be caused by a source difference; it does not identify an allocator or operating-system cause.",
              "Each run verifies512 known records, durable flushes, normal shutdown, and owned-home cleanup.",
              "This diagnostic measures memory only; the ordinary latency, growth, disk, and range checks remain separate.",
              "All original PR measurements and their confirmed advisory remain retained.",
              "The existing workflow stays unchanged; its historical shared-seed step name differs from this diagnostic."]
    text = "\n".join(lines) + "\n"
    (output / "summary.md").write_text(text)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as stream:
            stream.write(text)


def main():
    parser = argparse.ArgumentParser()
    for label in ("baseline", "candidate"):
        parser.add_argument(f"--{label}-binary", type=Path, required=True)
        parser.add_argument(f"--{label}-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true":
        parser.error("Actual memory measurements run in CI only")
    if not args.output.resolve().is_relative_to(Path(os.environ["RUNNER_TEMP"]).resolve()):
        parser.error("Output must stay outside Git in RUNNER_TEMP")
    manifest = {"complete": False, "driver_revision": os.environ.get("DRIVER_SHA"), "runs": [],
                "recipe": {"pairs": PAIRS, "conditions": ["before-after", "same-binary-control"],
                           "growth": PROFILES["standard"], "case": "fixed-read", "all_opening_samples_retained": True}}
    for label in ("baseline", "candidate"):
        revision = getattr(args, label + "_revision")
        if not re.fullmatch("[0-9a-f]{40}", revision):
            raise ValueError("Full product source revisions are required")
        binary = getattr(args, label + "_binary").resolve()
        manifest[label] = {"revision": revision, "binary_sha256": file_hash(binary)}
    args.output.mkdir(parents=True)
    save(args.output / "benchmark.json", manifest)
    for pair in range(PAIRS):
        conditions = ("before-after", "same-binary-control") if pair % 2 == 0 else ("same-binary-control", "before-after")
        labels = ("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")
        for condition in conditions:
            for label in labels:
                source = label if condition == "before-after" else "baseline"
                print(json.dumps({"condition": condition, "pair": pair, "label": label, "source": source}), flush=True)
                run = measure(getattr(args, source + "_binary").resolve(), manifest[source]["revision"],
                              args.output / f"{condition}-{pair}-{label}", condition, label, pair)
                manifest["runs"].append(run)
                save(args.output / "benchmark.json", manifest)
    manifest["comparisons"] = {condition: compare(manifest["runs"], condition)
                               for condition in ("before-after", "same-binary-control")}
    manifest["complete"] = True
    save(args.output / "benchmark.json", manifest)
    summary(args.output, manifest)


if __name__ == "__main__":
    main()
