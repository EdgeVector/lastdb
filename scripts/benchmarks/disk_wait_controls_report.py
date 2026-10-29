#!/usr/bin/env python3
"""Audit archived-program measurements and compare source effects with controls."""

import argparse
from collections import defaultdict
import gzip
import json
import math
import os
from pathlib import Path
from statistics import median

from disk_wait_controls import BASE, CANDIDATE, DRIVER, SOURCE_ARTIFACT, SOURCE_DIGEST, schedule


def audit_origin(output, manifest):
    source = json.loads((output.parent / "source-artifact.json").read_text())
    transfer = json.loads((output.parent / "program-transfer.json").read_text())
    if (manifest["controller_revision"] != os.environ["EXPECTED_CONTROLLER_SHA"]
            or manifest["driver_revision"] != DRIVER or source["id"] != SOURCE_ARTIFACT
            or source["digest"] != SOURCE_DIGEST
            or transfer["bundle_sha256"] != "ff817f6bf5e14aa1dc1fdb01cf72c5848abb70c0424491d4862b03e32fd0d151"):
        raise ValueError("The archived campaign or original program identity differs")
    for label, name, revision in (("A", "baseline", BASE), ("B", "candidate", CANDIDATE)):
        program = manifest["programs"][label]
        verified = transfer["programs"][name]
        keys = ("sha256", "cli_sha256", "version", "cli_version", "context", "revision")
        if program["revision"] != revision or any(program[key] != verified[key] for key in keys):
            raise ValueError("A measured program differs from the original verified transfer")


def audit(directory):
    metrics = json.loads((directory / "metrics.json").read_text())
    if json.loads((directory / "result.json").read_text())["complete"] is not True:
        raise ValueError("The run did not complete its product checks and cleanup")
    samples, signatures = defaultdict(list), {}
    with gzip.open(directory / "samples.jsonl.gz", "rt") as source:
        for line in source:
            sample = json.loads(line)
            identity = sample["case"], sample["request_id"]
            if ("error" in sample or identity in signatures
                    or not math.isfinite(sample["elapsed_ms"]) or sample["elapsed_ms"] <= 0):
                raise ValueError("Invalid, rejected, or duplicate timed request")
            signatures[identity] = tuple(sample[key] for key in
                ("kind", "records", "client", "client_request", "request_sha256"))
            samples[sample["case"]].append(sample["elapsed_ms"])
            if sample["case"].startswith("concurrent-mixed/"):
                samples[f"{sample['case']}/{sample['kind']}"].append(sample["elapsed_ms"])
    if len(signatures) != 4768 or set(samples) != set(metrics):
        raise ValueError("The full original standard workload did not complete")
    for case, metric in metrics.items():
        ordered = sorted(samples[case])
        if metric["errors"] or metric["requests"] != len(ordered):
            raise ValueError("The metric request count differs from the raw calls")
        for key, fraction in (("p50_ms", .5), ("p95_ms", .95), ("p99_ms", .99)):
            expected = ordered[math.ceil(len(ordered) * fraction) - 1]
            if metric[key] != expected:
                raise ValueError("The reported percentile differs from the raw calls")
    return metrics, signatures


def change(first, second):
    return (second / first - 1) * 100


def contrasts(runs, case, key):
    blocks = defaultdict(dict)
    for entry, metrics in runs:
        blocks[entry["block"]][entry["label"]] = metrics[case][key]
    values = {"source": [], "same_A": [], "same_B": []}
    for block in blocks.values():
        values["source"].append(change(median([block["A1"], block["A2"]]),
                                       median([block["B1"], block["B2"]])))
        values["same_A"].append(change(block["A1"], block["A2"]))
        values["same_B"].append(change(block["B1"], block["B2"]))
    return {label: {"blocks_percent": numbers, "median_percent": median(numbers),
                    "min_percent": min(numbers), "max_percent": max(numbers)}
            for label, numbers in values.items()}


def describe(value):
    return f"{value['median_percent']:+.1f}% [{value['min_percent']:+.1f}, {value['max_percent']:+.1f}]"


def report(output):
    manifest = json.loads((output / "controls.json").read_text())
    audit_origin(output, manifest)
    if manifest["complete"] is not True or len(manifest["runs"]) != 20:
        raise ValueError("The fixed 20-run campaign is incomplete; no source verdict")
    planned = schedule(manifest["replica"])
    if any(any(entry[key] != expected[key] for key in
               ("mode", "block", "position", "label", "directory"))
           for entry, expected in zip(manifest["runs"], planned)):
        raise ValueError("The actual run order differs from the predeclared controls")
    runs, signature = [], None
    for entry in manifest["runs"]:
        metrics, observed = audit(output / entry["directory"])
        if signature is not None and signature != observed:
            raise ValueError("The request bodies or client schedules differ between programs")
        signature = observed
        if entry["mode"] == "ordinary":
            runs.append((entry, metrics))
    # The initial controller used short descriptive names for the two mixed primary cases.
    primary = {name.replace("mixed/", "concurrent-mixed/", 1) if name.startswith("mixed/") else name
               for name in manifest["primary_cases"]}
    lines = ["# PR #702 disk-wait controls", "", f"Runner: {manifest['runner']}",
        f"CPU: {manifest['host']['cpu_model']}", "",
        "A1/A2 use the same archived baseline binary; B1/B2 use the same archived candidate binary.",
        "Each label occupies each position once across four ordinary blocks. No automatic repeat.",
        "All 20 runs use the original standard 10,000-record workload: 95,360 verified timed requests.",
        "Native runs are separate diagnostics and do not enter ordinary percentiles.",
        "Percent changes show the median [minimum, maximum] of four within-block comparisons.",
        "Same-program controls measure variability without a source change. They do not prove its cause.",
        "No pooled-request confidence interval or automatic regression verdict is claimed.", "",
        "| Case | A P95 ms | B P95 ms | Source P95 change | Same A control | Same B control |",
        "|---|---:|---:|---|---|---|"]
    results = {}
    for case in sorted(runs[0][1]):
        p95 = contrasts(runs, case, "p95_ms")
        before = median(metrics[case]["p95_ms"] for entry, metrics in runs if entry["label"][0] == "A")
        after = median(metrics[case]["p95_ms"] for entry, metrics in runs if entry["label"][0] == "B")
        results[case] = {"baseline_p95_ms": before, "candidate_p95_ms": after,
                         "p95": p95, "throughput": contrasts(runs, case, "requests_per_second")}
        marker = " *" if case in primary else ""
        lines.append(f"| {case}{marker} | {before:.3f} | {after:.3f} | {describe(p95['source'])} | "
                     f"{describe(p95['same_A'])} | {describe(p95['same_B'])} |")
    lines.extend(["", "* marks the four previously confirmed write-tail warnings.", "",
                  "## Separate native disk-phase diagnostics", "",
                  "| Run | Case | Physical commits | Mean commit ms | Three largest mean physical phases (us) |",
                  "|---|---|---:|---:|---|"])
    for entry in manifest["runs"]:
        if entry["mode"] != "native":
            continue
        native = json.loads((output / entry["directory"] / "native-phases.json").read_text())
        if native["complete"] is not True:
            raise ValueError("Native timing coverage is incomplete")
        for case in ("concurrent-write/1", "concurrent-write/4", "concurrent-mixed/1",
                     "concurrent-mixed/4", "update/128"):
            value = native["cases"][case]
            phases = sorted(value["physical_phases_us"].items(), key=lambda item: item[1]["mean"], reverse=True)[:3]
            text = "; ".join(f"{name}={phase['mean']:.1f}" for name, phase in phases)
            lines.append(f"| {entry['label']} | {case} | {value['distinct_physical_commits']} | "
                         f"{value['physical_mean_us'] / 1000:.3f} | {text} |")
    (output / "controls-summary.json").write_text(json.dumps(results, indent=2) + "\n")
    text = "\n".join(lines) + "\n"
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as summary:
        summary.write(text)
    print(text)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    report(parser.parse_args().output)


if __name__ == "__main__":
    main()
