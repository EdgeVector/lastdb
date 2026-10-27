"""One-off CI aggregate latency comparison; the workflow and product stay unchanged."""

import argparse
import gzip
import json
import math
import os
from pathlib import Path
import re
import statistics

from aggregate_requests import durable, exercise_group, records, repeat, rpc, schemas, summary
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
        request_id = f"aggregate-{label}-{pair}-{case.replace('/', '-')}-{len(samples.get(case, []))}"
        value, elapsed, status, digest = rpc(node, path, body, expected, request_id)
        row = {"case": case, "request_id": request_id, "elapsed_ms": elapsed,
               "status": status, "request_sha256": digest, "response_fields": sorted(value)}
        raw.write(json.dumps(row, sort_keys=True) + "\n")
        samples.setdefault(case, []).append(row)
        return value

    try:
        node.start()
        schemas(node)
        groups = [exercise_group(node, size, SAMPLES, timed) for size in SIZES]
        result["proof"] = {group: proof for group, _, _, proof, _ in groups}
        result["race_finalize_status"] = {group: status for group, _, _, _, status in groups}
        node.stop()
        node.start()
        for group, values, token, proof, _ in groups:
            summary(node, group, values, 1, token)
            if records(node, group, values) != proof:
                raise RuntimeError("Source/member values differ after restart")
            repeat(node, group, token)
        result["metrics"] = {case: metric(rows) for case, rows in samples.items()}
        if len(samples) != 4 * len(SIZES) or any(len(rows) != SAMPLES for rows in samples.values()):
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
    lines = ["# Aggregate repair and finalization: actual CI requests", "",
             f"Baseline `{result['baseline']['revision']}`; candidate `{result['candidate']['revision']}`.", "",
             "Four alternating AB/BA pairs; 128 opening samples per case per run; fresh generated homes.", "",
             "| Case | Before P50 ms | After P50 ms | Before P95 ms | After P95 ms | P95 change |",
             "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for case, row in result["comparison"].items():
        before, after = row["baseline"], row["candidate"]
        lines.append(f"| {case} | {before['p50_ms']:.3f} | {after['p50_ms']:.3f} | "
                     f"{before['p95_ms']:.3f} | {after['p95_ms']:.3f} | {row['p95_change_percent']:+.1f}% |")
    lines += ["", "All scheduled samples remain in the artifact, including opening calls. No warmup or outlier exclusion.",
              "Totals, known-key source/member identities, invalid-to-valid markers, stale tokens, association refusal,",
              "repeat-finalize durable empty receipts, a real source-write/finalize race, and restart reads pass.",
              "The race permits either legal order. Its status is recorded separately from stable source/member proof.",
              "Client RPC latency includes transport and durable response, excluding request preparation and proof reads.",
              "Proof reads affect cache and pacing. This study does not measure throughput or large aggregate hashes.",
              "The ordinary 10,000-record CI workload remains a separate measurement.",
              "Product-generated UUIDs, tokens, and clocks vary. The logical inputs and recipe remain the same.",
              "Injected corrupt members/private grants, scoped orphan cleanup, and interrupted-process recovery are outside this workload.",
              "The driver uses generated isolated homes only. It never uses a primary home or a live schema service.",
              "The existing workflow remains unchanged; its historical shared-seed step name does not describe this driver.",
              "Each owned daemon stops with a verified durable flush; its generated home is removed."]
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
              "recipe": {"pairs": PAIRS, "samples_per_case": SAMPLES, "members": SIZES,
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
    result["comparison"] = compare(result["runs"])
    result["complete"] = True
    save(args.output / "benchmark.json", result)
    write_summary(args.output, result)


if __name__ == "__main__":
    main()
