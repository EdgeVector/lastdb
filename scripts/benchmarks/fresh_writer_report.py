#!/usr/bin/env python3
"""Audit the frozen fresh-home campaign; report source effects beside A/A variation."""

import argparse
from collections import Counter, defaultdict
import json
import math
import os
from pathlib import Path
from statistics import median

from fresh_writer_controls import ARTIFACT, BASE, CANDIDATE, DIGEST, DRIVER, HASHES, schedule
from fresh_writer_native import FLUSHES, stats, summarize


def audit(directory, case):
    rows = json.loads((directory / f"{case['name']}-samples.json").read_text())
    identities = {(r["client"], r["request_id"]): r["request_sha256"] for r in rows}
    if (len(rows) != 2048 or len(identities) != 2048 or len({r["request_id"] for r in rows}) != 2048
            or case["metrics"]["errors"]
            or case["metrics"]["requests"] != 2048
            or Counter(r["client"] for r in rows) != Counter({n: 128 for n in range(16)})
            or any(r.get("error") or r["records"] != 1 or not math.isfinite(r["elapsed_ms"])
                   or r["elapsed_ms"] <= 0 for r in rows)):
        raise ValueError("Invalid, missing, rejected, or duplicate timed call")
    ordered = sorted(r["elapsed_ms"] for r in rows)
    for key, fraction in (("p50_ms", .5), ("p95_ms", .95), ("p99_ms", .99)):
        if case["metrics"][key] != ordered[math.ceil(len(ordered)*fraction)-1]:
            raise ValueError("A reported percentile differs from its raw calls")
    elapsed = case["wall_ns"] / 1e9
    if not math.isclose(case["metrics"]["records_per_second"], len(rows)/elapsed, rel_tol=1e-9):
        raise ValueError("Throughput differs from completed calls and elapsed time")
    return identities


def audit_origin(output, data):
    source = json.loads((output.parent / "source-artifact.json").read_text())
    transfer = json.loads((output.parent / "program-transfer.json").read_text())
    if (source["id"] != ARTIFACT or source["digest"] != DIGEST
            or data["source_artifact"] != ARTIFACT or data["driver_revision"] != DRIVER
            or transfer["bundle_sha256"] != "cc92bb5bafbd5a8981a32691210c357e2834159f0631a216e979e8025862eced"):
        raise ValueError("Campaign source or program bundle differs")
    for label, role, revision in (("A", "baseline", BASE), ("B", "candidate", CANDIDATE)):
        program, received = data["programs"][label], transfer["programs"][role]
        if (program["revision"] != revision or program["sha256"] != HASHES[label]
                or any(program[key] != received[key] for key in
                       ("sha256", "cli_sha256", "version", "cli_version", "context", "revision"))):
            raise ValueError("A measured program differs from the exact archived transfer")


def percent(a, b):
    return (b/a-1)*100


def report(output):
    data = json.loads((output / "controls.json").read_text())
    audit_origin(output, data)
    if not data["complete"] or len(data["runs"]) != 20:
        raise ValueError("The fixed 20-home campaign is incomplete")
    blocks, signatures, native = defaultdict(dict), {}, []
    for entry, planned in zip(data["runs"], schedule(data["replica"])):
        if not entry["complete"] or any(entry[k] != planned[k] for k in
                                        ("mode", "block", "position", "label", "directory")):
            raise ValueError("The actual execution differs from the frozen schedule")
        directory = output / entry["directory"] / entry["home"]["directory"]
        cleanup = json.loads((directory / "cleanup.json").read_text())
        if not cleanup["daemon_stopped"] or not cleanup["removed"]:
            raise ValueError("An owned generated home lacks complete cleanup")
        for case in entry["home"]["cases"]:
            observed = audit(directory, case)
            expected = signatures.setdefault(case["name"], observed)
            if observed != expected:
                raise ValueError("Request bodies or per-client schedules differ between programs")
            if case["native_timing"]:
                detail = summarize(directory, case)
                (directory / "flush-tails.json").write_text(json.dumps(detail, indent=2)+"\n")
                native.append({"label": entry["label"], "metrics": case["metrics"], "detail": detail})
            elif entry["mode"] == "ordinary":
                blocks[entry["block"]][entry["label"]] = case["metrics"]
    if len(blocks) != 4 or any(set(b) != {"A1", "A2", "B1", "B2"} for b in blocks.values()) or len(native) != 4:
        raise ValueError("Required source and same-program controls are missing")
    lines = ["# Fresh 16-writer source and disk-flush controls", "",
             f"Runner {data['replica']}: {data['host']['cpu_model']}, {data['host']['cpu_count']} CPUs.",
             "Exact archived programs and original driver; no rebuild. Each home has 100,000 generated records.",
             "A1/A2 are one identical executable; B1/B2 are one identical executable.",
             "Every label occupies each position once. Runner 1 reverses the schedule. No automatic repeat.",
             "Sixteen ordinary homes enter the source comparison. Four additional homes run ordinary predecessors then separate native passes.",
             "All 49,152 timed calls/runner are audited; native calls do not enter ordinary percentiles.",
             "All calls require durable acknowledgments and verified final values. Search is off.", "",
             "| Block | A P95 ms | B P95 ms | Source P95 change | Same A change | Same B change | Source rate change |",
             "|---|---:|---:|---:|---:|---:|---:|"]
    results = []
    for number, values in sorted(blocks.items()):
        a, b = (median(values[label]["p95_ms"] for label in names) for names in (("A1", "A2"), ("B1", "B2")))
        ar, br = (median(values[label]["records_per_second"] for label in names) for names in (("A1", "A2"), ("B1", "B2")))
        row = {"block": number, "baseline_p95_ms": a, "candidate_p95_ms": b,
               "source_p95_percent": percent(a, b), "source_rate_percent": percent(ar, br),
               "same_A_percent": percent(values["A1"]["p95_ms"], values["A2"]["p95_ms"]),
               "same_B_percent": percent(values["B1"]["p95_ms"], values["B2"]["p95_ms"])}
        results.append(row)
        lines.append(f"| {number} | {a:.3f} | {b:.3f} | {row['source_p95_percent']:+.1f}% | "
                     f"{row['same_A_percent']:+.1f}% | {row['same_B_percent']:+.1f}% | {row['source_rate_percent']:+.1f}% |")
    for field in ("source_p95_percent", "same_A_percent", "same_B_percent", "source_rate_percent"):
        values = [r[field] for r in results]
        lines.append(f"\nMedian block {field}: {median(values):+.1f}% [{min(values):+.1f}, {max(values):+.1f}].")
    lines += native_lines(native)
    lines += ["", "## Filesystems and post-campaign probes", "", "```json",
              json.dumps(data["locations"], indent=2), "```",
              "Probes execute after every database case finishes, using only owned temporary directories.",
              "They cannot establish the cause of an earlier request delay."]
    for name, probe in data["io_probes"].items():
        values = {phase: stats(s[phase] for s in probe["samples"])
                  for phase in ("total_us", "file_flush_us", "rename_us", "directory_flush_us")}
        lines += ["", f"{name}: `{json.dumps(values, sort_keys=True)}`"]
    lines += ["", "Fresh layouts, OS cache state, and shared-host pressure remain uncontrolled.",
              "Same-program variation is evidence against a source-only explanation; it does not prove its cause.",
              "Source contrasts use within-block medians. No pooled-sample confidence or automatic regression verdict is claimed."]
    result = {"replica": data["replica"], "host": data["host"], "blocks": results, "native": native}
    (output / "comparison.json").write_text(json.dumps(result, indent=2)+"\n")
    text = "\n".join(lines)+"\n"
    (output / "summary.md").write_text(text)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
        stream.write(text)
    print(text)


def native_lines(cases):
    lines = ["", "## Separate native disk tails", "",
             "| Label | P95 ms | Commit P95 / max ms | Commits >50 / >100 ms | Largest flush phase max ms |",
             "|---|---:|---:|---:|---|"]
    for case in cases:
        d = case["detail"]
        top = sorted(((k, d["commit_phases"][k]["max_us"]) for k in FLUSHES
                      if k in d["commit_phases"]), key=lambda v: v[1], reverse=True)[:3]
        phases = "; ".join(f"{k}={v/1000:.3f}" for k, v in top)
        lines.append(f"| {case['label']} | {case['metrics']['p95_ms']:.3f} | "
                     f"{d['commit_us']['p95_us']/1000:.3f} / {d['commit_us']['max_us']/1000:.3f} | "
                     f"{d['slow_commit_counts']['50000']} / {d['slow_commit_counts']['100000']} | {phases} |")
    lines += ["", "Commit membership follows exact storage sequences, not HTTP IDs. Each physical commit is counted once.",
              "Nested phases and concurrent waits overlap. Do not add their percentiles. Raw top-commit details remain in the artifacts."]
    return lines


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    report(parser.parse_args().output)
