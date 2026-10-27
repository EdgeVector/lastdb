"""One-off CI conditional-write comparison; the workflow stays unchanged."""

import argparse
import gzip
import json
import math
import os
from pathlib import Path
import re
import statistics

from aggregate_requests import records, repeat, schemas, summary, rpc
from conditional_requests import exercise, read_records
from invalidation_requests import cross_targets, exercise_group
from node import Node, file_hash, save

PAIRS, SAMPLES, SIZES = 4, 128, (1, 32)


def metric(rows):
    elapsed = sorted(row["elapsed_ms"] for row in rows)
    return {"requests": len(rows), "errors": 0,
            **{f"p{p}_ms": elapsed[math.ceil(len(elapsed) * p / 100) - 1] for p in (50, 95, 99)}}


def measure(binary, revision, output, label, pair):
    output.mkdir()
    node = Node(binary, revision, output)
    raw = gzip.open(output / "samples.jsonl.gz", "wt")
    samples, result = {}, {"complete": False, "label": label, "pair": pair, "revision": revision}

    def timed(case, path, body, expected=(200,)):
        request_id = f"conditional-{label}-{pair}-{case.replace('/', '-')}-{len(samples.get(case, []))}"
        value, elapsed, status, digest = rpc(node, path, body, expected, request_id)
        row = {"case": case, "request_id": request_id, "elapsed_ms": elapsed,
               "status": status, "request_sha256": digest, "response_fields": sorted(value)}
        raw.write(json.dumps(row, sort_keys=True) + "\n")
        samples.setdefault(case, []).append(row)
        return value

    try:
        node.start()
        conditional_proof = exercise(node, SAMPLES, timed)
        schemas(node)
        groups = [exercise_group(node, size, 1, lambda case, path, body, expected=(200,): rpc(node, path, body, expected)[0]) for size in SIZES]
        groups = cross_targets(node, groups)
        result["proof"] = {group: proof for group, _, _, proof in groups}
        result["conditional_proof"] = conditional_proof
        result["operational_checks"] = ["delete/purge/tombstone refusal", "mixed-batch refusal", "two-target batch", "latest-source token", "restart"]
        node.stop()
        node.start()
        if read_records(node, conditional_proof["expected"]) != conditional_proof["values_sha256"]:
            raise RuntimeError("Conditional-write values differ after restart")
        for group, values, token, proof in groups:
            summary(node, group, values, 1, token)
            if records(node, group, values) != proof:
                raise RuntimeError("Source/member values differ after restart")
            repeat(node, group, token)
        result["metrics"] = {case: metric(rows) for case, rows in samples.items()}
        if len(samples) != 11 or any(len(rows) != SAMPLES for rows in samples.values()):
            raise RuntimeError("The benchmark lacks all scheduled opening samples")
        result["complete"] = True
    except BaseException as error:
        result["error"] = str(error)
        raise
    finally:
        raw.close()
        try:
            node.close()
        finally:
            save(output / "result.json", result)
    return result


def compare(runs):
    cases = sorted(runs[0]["metrics"])
    output = {}
    for case in cases:
        row = {}
        for label in ("baseline", "candidate"):
            selected = [run["metrics"][case] for run in runs if run["label"] == label]
            row[label] = {f"p{p}_ms": statistics.median(item[f"p{p}_ms"] for item in selected)
                          for p in (50, 95, 99)}
        row["p95_change_percent"] = (row["candidate"]["p95_ms"] / row["baseline"]["p95_ms"] - 1) * 100
        row["paired_p95_change_percent"] = []
        for pair in range(PAIRS):
            selected = {run["label"]: run["metrics"][case]["p95_ms"] for run in runs if run["pair"] == pair}
            row["paired_p95_change_percent"].append((selected["candidate"] / selected["baseline"] - 1) * 100)
        output[case] = row
    return output


def write_summary(output, result):
    lines = ["# Conditional writes: actual CI requests", "",
             f"Baseline `{result['baseline']['revision']}`; candidate `{result['candidate']['revision']}`.", "",
             "Four alternating AB/BA pairs; 128 opening samples per case per run; fresh generated homes.", "",
             "| Case | Before P50 ms | After P50 ms | Before P95 ms | After P95 ms | P95 change |",
             "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for case, row in result["comparison"].items():
        before, after = row["baseline"], row["candidate"]
        lines.append(f"| {case} | {before['p50_ms']:.3f} | {after['p50_ms']:.3f} | "
                     f"{before['p95_ms']:.3f} | {after['p95_ms']:.3f} | {row['p95_change_percent']:+.1f}% |")
    lines += ["", "All scheduled opening samples remain; no warmup exclusion or outlier removal.",
              "CAS value and content-hash success/refusal, live-field absent refusal, and absent-key create use the public API.",
              "Must-exist probes include a written-field hit, a new-field fallback hit, a missing key, and a 32-key guarded batch.",
              "A three-mutation same-key batch preserves the final value. Repeat-request calls are fresh local writes, not signed replay cache hits.",
              "The public API allocates a fresh author clock; actual duplicate-cache hit branches are source-reviewed, not claimed exercised.",
              "Known-key reads verify each outcome. Additional real operations cover delete/recreate, refusal precedence, aggregate source/member repair and finalize.",
              "A normal stop/restart preserves all final conditional and aggregate values. Every owned daemon stops durably and its generated home is removed.",
              "RPC latency excludes payload preparation and proof reads; proof reads affect cache and pacing. No throughput claim.",
              "No primary data, cloud, injected private state, missing-atom corruption, or interrupted-process recovery is part of this workload.",
              "The ordinary 10,000-record CI comparison is separate. This driver reuses the existing workflow unchanged; its historical step name differs."]
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
        raise RuntimeError("This driver runs in CI only")
    result = {"complete": False, "driver_revision": os.environ.get("DRIVER_SHA"), "runs": [],
              "recipe": {"pairs": PAIRS, "samples_per_case": SAMPLES, "operational_aggregate_members": SIZES, "timed_cases": 11,
                         "order": ["AB", "BA", "AB", "BA"], "default_daemon_settings": True}}
    for label in ("baseline", "candidate"):
        revision = getattr(args, label + "_revision")
        binary = getattr(args, label + "_binary").resolve()
        if not re.fullmatch("[0-9a-f]{40}", revision):
            raise ValueError("A full product revision is required")
        result[label] = {"revision": revision, "binary_sha256": file_hash(binary)}
    args.output.mkdir(parents=True)
    save(args.output / "benchmark.json", result)
    for pair in range(PAIRS):
        labels = ("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")
        for label in labels:
            print(f"pair={pair + 1} label={label}", flush=True)
            run = measure(getattr(args, label + "_binary").resolve(), result[label]["revision"],
                          args.output / f"pair-{pair + 1}-{label}", label, pair)
            result["runs"].append(run)
            save(args.output / "benchmark.json", result)
    if any(run["proof"] != result["runs"][0]["proof"] for run in result["runs"]):
        raise RuntimeError("The programs disagree on source/member results")
    if any(run["conditional_proof"] != result["runs"][0]["conditional_proof"] for run in result["runs"]):
        raise RuntimeError("The programs disagree on conditional-write results")
    result["comparison"] = compare(result["runs"])
    result["complete"] = True
    save(args.output / "benchmark.json", result)
    write_summary(args.output, result)


if __name__ == "__main__":
    main()
