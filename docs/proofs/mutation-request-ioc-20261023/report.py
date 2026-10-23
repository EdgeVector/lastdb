#!/usr/bin/env python3
"""Summarize the single-mutation benchmark without raw samples in Git."""

import argparse
import json
import os
from pathlib import Path


def report(args):
    lines = ["# Single mutation latency comparison", "",
             f"Before: `{args.before}`. After: `{args.after}`.", ""]
    comparison = args.output / "comparison.json"
    if not comparison.exists():
        return lines + ["The comparison did not complete. No latency conclusion is available."]
    results = json.loads(comparison.read_text())
    runs = json.loads((args.output / "runs.json").read_text())
    if len(runs) != 8 or not all(run["complete"] for run in runs):
        raise RuntimeError("The comparison requires eight complete measurements")
    inputs = json.loads((args.output / "inputs.json").read_text())
    for label, revision in (("before", args.before), ("after", args.after)):
        if f"bench-{revision}" not in inputs["binaries"][label]["version"]:
            raise RuntimeError(f"The {label} binary differs from the requested revision")
    lines += ["Each value is the median of four run percentiles. All values are milliseconds.", "",
              "| Operation | Before p50 | After p50 | Change | Before p95 | After p95 | Before p99 | After p99 |",
              "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for phase, metrics in results.items():
        med, p95, p99 = (metrics[name] for name in ("p50_ms", "p95_ms", "p99_ms"))
        lines.append(f"| {phase} | {med['before']:.3f} | {med['after']:.3f} | "
                     f"{med['change_percent']:+.1f}% | {p95['before']:.3f} | {p95['after']:.3f} | "
                     f"{p99['before']:.3f} | {p99['after']:.3f} |")
    lines += ["", "Method: four alternating pairs on one GitHub runner, with separate optimized builds.",
              "Each run starts with generated data, 1 KiB values, durable writes, and 128 warm-up writes.",
              "Each side measures 5,200 mutations and 1,200 control reads in total.",
              "The write cases use `/api/mutation`. The driver checks the final values before and after restart.",
              "The timer covers a new Unix socket connection through the response bytes.",
              "Schema setup, JSON conversion, and value checks remain outside the timer.",
              "Limits: one shared GitHub machine, four pairs, cloud off, one schema, and no prefix or aggregate case.",
              "Small differences and variable tail latency do not establish a general speed change.", "",
              "Driver: `python3 docs/proofs/mutation-request-ioc-20261023/measure.py` with `--rounds 4`.",
              "Build: `cargo build --locked --profile dev-release -p lastdb_node --bin lastdbd`.",
              f"Driver SHA-256: `{inputs['driver_sha256']}`.", ""]
    for label in ("before", "after"):
        lines.append(f"{label.title()} executable SHA-256: `{inputs['binaries'][label]['sha256']}`.")
    lines += ["", "Per-pair p50 changes:", "",
              "| Pair | Create | Update | Conditional update | Four callers | Control read |",
              "|---|---:|---:|---:|---:|---:|"]
    for pair in range(1, 5):
        sides = {run["label"]: run for run in runs if run["name"].startswith(f"{pair}-")}
        changes = [100 * (sides["after"]["phases"][phase]["p50_ms"] /
                         sides["before"]["phases"][phase]["p50_ms"] - 1) for phase in results]
        lines.append(f"| {pair} | " + " | ".join(f"{change:+.1f}%" for change in changes) + " |")
    return lines


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--before", required=True)
    parser.add_argument("--after", required=True)
    parser.add_argument("--artifact-metadata", type=Path)
    args = parser.parse_args()
    lines = report(args)
    if args.artifact_metadata:
        meta = json.loads(args.artifact_metadata.read_text())
        digest = os.environ["ARTIFACT_SHA256"]
        if (meta["expired"] or meta["size_in_bytes"] <= 0 or
                meta.get("digest") != f"sha256:{digest}"):
            raise RuntimeError("The archive is absent, empty, expired, or has a different digest")
        receipt = {"url": os.environ["ARTIFACT_URL"], "sha256": digest,
                   "before": args.before, "after": args.after,
                   "driver_revision": os.environ["GITHUB_SHA"], "retention_days": 14,
                   "expires_at": meta["expires_at"]}
        (args.output / "artifact.json").write_text(json.dumps(receipt, indent=2) + "\n")
        lines += ["", f"[Raw evidence archive]({receipt['url']}). Retention: 14 days.",
                  f"Archive SHA-256: `{digest}`. Expiration: {meta['expires_at']}.",
                  f"CI driver revision: `{receipt['driver_revision']}`."]
    content = "\n".join(lines) + "\n"
    (args.output / "summary.md").write_text(content)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as stream:
            stream.write(content)
    print(content)


if __name__ == "__main__":
    main()
