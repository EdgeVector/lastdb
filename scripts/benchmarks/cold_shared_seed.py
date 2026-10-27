#!/usr/bin/env python3
"""CI-only direct batch-query latency from copies of one generated seed."""

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import time

from node import Node, UnixConnection, file_hash, save
from report import compare
from workload import FIELDS, PROFILES, SCHEMA, Workload, describe, encoded, read_request, validate_read

PAIRS, SAMPLES, RECORDS = 4, 256, 1024
CASES = (
    ("batch/1x1", 1, 1, False, True),
    ("batch/16x1", 16, 1, False, True),
    ("batch/64x1", 64, 1, False, True),
    ("batch/16x32", 16, 32, False, True),
    ("batch/16x1-no-receipt", 16, 1, False, False),
    ("batch/mixed-valid-rejected", 8, 1, True, True),
)
SCOPE = (
    "Direct /api/queries/batch RPC through response bytes, one client. Six cases, "
    "four alternating pairs, no opening samples discarded. Every run starts from "
    "a byte-identical copy of one stopped generated seed, then warms all 1024 known keys. "
    "Valid values, item order, rejection kinds, and receipt presence are verified outside "
    "the RPC timer. All requests carry the owner unfiltered-read header; batch items "
    "must still reject the unkeyed query. Payload preparation, decode, validation, "
    "and gzip recording are outside each RPC timer but included in throughput. "
    "Startup metadata, OS cache, maintenance and scheduling remain uncontrolled. "
    "No cold-read, app-identity, concurrent-write snapshot, primary-data, total-memory, "
    "power-loss or read-scope performance claim."
)


def inventory(home):
    files, entries, started = {}, 0, time.monotonic()
    for parent, directories, names in os.walk(home, followlinks=False):
        for name in directories + names:
            path = Path(parent) / name
            entries += 1
            if entries > 100_000 or time.monotonic() - started > 30:
                raise RuntimeError("Owned seed inventory exceeded its bound")
            mode = path.lstat().st_mode
            if not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
                raise RuntimeError("Generated home contains a link or nonregular entry")
            if stat.S_ISREG(mode):
                files[str(path.relative_to(home))] = {"bytes": path.stat().st_size, "sha256": file_hash(path)}
    if not files:
        raise RuntimeError("Generated seed is empty")
    return files


def prepare_seed(node, output):
    work = None
    try:
        node.start()
        work = Workload(node, {**PROFILES["standard"], "records": RECORDS, "native_timing": False}, output)
        work.seed()
    finally:
        if work:
            work.raw.close()
        node.stop()
    whole = inventory(node.home)
    save(output / "inventory.json", whole)
    return whole


def request(case, number):
    _, items, keys, mixed, _ = case
    queries, expected = [], []
    rejected = (
        (None, "malformed_body", None, False),
        ({}, "missing_required_key", "schema_name", False),
        ({"schema_name": SCHEMA, "fields": FIELDS}, "full_schema_scan_not_allowed", None, True),
        ({**json.loads(read_request([0])), "limit": -1}, "invalid_value", "limit", False),
    )
    for item in range(items):
        if mixed and item % 2:
            query, kind, wire_key, receipt = rejected[item // 2]
            queries.append(query)
            expected.append({"status": 400, "kind": kind, "key": wire_key, "receipt": receipt})
        else:
            first = ((number * items + item) * keys) % (RECORDS - keys + 1)
            numbers = list(range(first, first + keys))
            queries.append(json.loads(read_request(numbers)))
            expected.append({"status": 200, "numbers": numbers})
    return encoded({"queries": queries}), expected


def call_batch(node, body, request_id, receipt):
    headers = {"Content-Type": "application/json", "X-LastDB-Client": "github-benchmark",
               "X-LastDB-Request-Id": request_id, "X-LastDB-Allow-Full-Scan": "1"}
    if receipt:
        headers["x-lastdb-read-receipt"] = "true"
    connection = UnixConnection(node.socket)
    started = time.perf_counter_ns()
    try:
        connection.request("POST", "/api/queries/batch", body, headers)
        response = connection.getresponse()
        raw = response.read()
        elapsed = (time.perf_counter_ns() - started) / 1e6
        if response.status != 200:
            raise RuntimeError(f"Batch returned HTTP {response.status}")
        return json.loads(raw), elapsed
    finally:
        connection.close()


def validate_batch(result, expected, receipt):
    entries = result.get("results", [])
    if result.get("ok") is not True or result.get("count") != len(expected) or len(entries) != len(expected):
        raise RuntimeError("Batch lacks a complete success envelope")
    records, costs, outcomes = 0, [], []
    for entry, want in zip(entries, expected):
        value, status = entry["response"], entry["status"]
        if status != want["status"]:
            raise RuntimeError("Item status or order differs from the requested flow")
        cost = entry.get("read_receipt")
        if status == 200:
            validate_read(value, want["numbers"])
            records += len(want["numbers"])
            if receipt and (not cost or cost["logical_warm_hits"] < len(FIELDS) * len(want["numbers"])):
                raise RuntimeError("A measured warm item lacks its warm-hit receipt")
            if not receipt and cost is not None:
                raise RuntimeError("An item ignored the receipt opt-out")
            outcomes.append({"status": status, "keys": want["numbers"]})
        else:
            if value.get("kind") != want["kind"] or value.get("key") != want["key"]:
                raise RuntimeError("A rejected item changed its refusal kind or key")
            if bool(cost) != want["receipt"]:
                raise RuntimeError("A rejected item changed its receipt boundary")
            outcomes.append({"status": status, "kind": value["kind"], "key": value.get("key")})
        costs.append(cost)
    return records, costs, hashlib.sha256(encoded(outcomes)).hexdigest()


def warm(node):
    for first in range(0, RECORDS, 64):
        numbers = list(range(first, first + 64))
        validate_read(node.call("/api/query", read_request(numbers), receipt=True)[0], numbers)
    # Re-read the full known set to require warm coverage before timed batches.
    for first in range(0, RECORDS, 64):
        numbers = list(range(first, first + 64))
        result, _, receipt = node.call("/api/query", read_request(numbers), receipt=True)
        validate_read(result, numbers)
        if not receipt or receipt["logical_warm_hits"] < len(FIELDS) * len(numbers):
            raise RuntimeError("The generated clone did not establish the warm boundary")


def measure_case(node, case, case_index, raw):
    rows, started = [], time.perf_counter()
    for number in range(SAMPLES):
        body, expected = request(case, number)
        request_id = f"query-batch-{case_index}-{number:06d}"
        result, elapsed = call_batch(node, body, request_id, case[4])
        records, receipts, outcome = validate_batch(result, expected, case[4])
        row = {"case": case[0], "kind": "read", "records": records, "client": 0,
               "client_request": number, "request_id": request_id,
               "request_sha256": hashlib.sha256(body).hexdigest(), "outcome_sha256": outcome,
               "elapsed_ms": elapsed, "read_receipts": receipts}
        rows.append(row)
        raw.write(json.dumps(row) + "\n")
    metric = describe(rows, time.perf_counter() - started)
    metric["verified_items"] = SAMPLES * case[1]
    metric["verified_records"] = sum(r["records"] for r in rows)
    print(json.dumps({"case": case[0], **metric}), flush=True)
    return metric, [(r["case"], r["client_request"], r["request_sha256"], r["outcome_sha256"]) for r in rows]


def measure(binary, revision, output, template, whole):
    output.mkdir()
    node = Node(binary, revision, output)
    error, metrics, observations = None, {}, []
    try:
        if template.process is not None or any(node.home.iterdir()):
            raise RuntimeError("The clone requires a stopped seed and an empty destination")
        shutil.copytree(template.home, node.home, dirs_exist_ok=True)
        if inventory(node.home) != whole:
            raise RuntimeError("The generated clone differs from the stopped seed")
        save(output / "clone.json", {"byte_identical": True, "files": len(whole)})
        node.start()
        warm(node)
        save(output / "warm-boundary.json", {"verified_keys": RECORDS, "fields": FIELDS})
        with gzip.open(output / "samples.jsonl.gz", "wt") as raw:
            for case_index, case in enumerate(CASES):
                metric, observations_for_case = measure_case(node, case, case_index, raw)
                metrics[case[0]] = metric
                observations.extend(observations_for_case)
                save(output / "metrics.json", metrics)
    except BaseException as failure:
        error = f"{type(failure).__name__}: {failure}"
    finally:
        try:
            node.close()
        except BaseException as failure:
            error = f"{error or ''}; cleanup: {type(failure).__name__}: {failure}"
    result = {"complete": error is None, "error": error}
    save(output / "result.json", result)
    if error:
        raise RuntimeError(error)
    return result, observations


def render(manifest, comparison):
    revisions = {k: v["revision"] for k, v in manifest["binaries"].items()}
    lines = ["## Temporary direct batch-query comparison", "",
             f"Baseline: `{revisions['baseline']}`. Candidate: `{revisions['candidate']}`.", "",
             "| Case | Base P50/P95/P99 ms | Candidate P50/P95/P99 ms | P95 change |",
             "|---|---:|---:|---:|"]
    for case, item in comparison.items():
        base = " / ".join(f"{item['baseline'][k]:.3f}" for k in ("p50_ms", "p95_ms", "p99_ms"))
        candidate = " / ".join(f"{item['candidate'][k]:.3f}" for k in ("p50_ms", "p95_ms", "p99_ms"))
        lines.append(f"| {case} | {base} | {candidate} | {item['changes_percent']['p95_ms']:+.2f}% |")
    lines.extend(["", "Medians of four per-run statistics, not pooled percentiles.", "", SCOPE])
    report = "\n".join(lines) + "\n"
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
        stream.write(report)
    print(report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for label in ("baseline", "candidate"):
        parser.add_argument(f"--{label}-binary", type=Path, required=True)
        parser.add_argument(f"--{label}-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    revisions = {label: getattr(args, f"{label}_revision") for label in ("baseline", "candidate")}
    if os.environ.get("GITHUB_ACTIONS") != "true" or any(
            not re.fullmatch(r"[0-9a-f]{40}", revision) for revision in revisions.values()):
        raise RuntimeError("Run only in GitHub Actions with exact product revision SHAs")
    binaries = {label: getattr(args, f"{label}_binary").resolve() for label in revisions}
    args.output.mkdir(parents=True, exist_ok=True)
    manifest = {"complete": False, "recipe": {"pairs": PAIRS, "samples": SAMPLES, "seed_records": RECORDS,
                "endpoint": "/api/queries/batch", "cases": CASES}, "runs": [],
                "driver_revision": os.environ["DRIVER_SHA"], "scope": SCOPE,
                "binaries": {label: {"revision": revisions[label], "sha256": file_hash(binary)}
                             for label, binary in binaries.items()}}
    save(args.output / "benchmark.json", manifest)
    seed_output = args.output / "seed"
    seed_output.mkdir()
    template = Node(binaries["baseline"], revisions["baseline"], seed_output)
    reference = None
    try:
        whole = prepare_seed(template, seed_output)
        manifest["seed_inventory_sha256"] = file_hash(seed_output / "inventory.json")
        for pair in range(PAIRS):
            for label in (("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")):
                directory = f"{pair}-{label}"
                result, observed = measure(binaries[label], revisions[label], args.output / directory,
                                           template, whole)
                if reference is None:
                    reference = observed
                if observed != reference:
                    raise RuntimeError("Input hashes or verified outcome facts differ across the program pair")
                manifest["runs"].append({"pair": pair, "label": label, "directory": directory, **result})
                save(args.output / "benchmark.json", manifest)
        if inventory(template.home) != whole:
            raise RuntimeError("The stopped generated seed changed")
        comparison = compare(args.output, manifest)
        save(args.output / "comparison.json", comparison)
        render(manifest, comparison)
    finally:
        template.close()
    manifest.update(complete=True, identical_input_requests=len(reference))
    save(args.output / "benchmark.json", manifest)


if __name__ == "__main__":
    main()
