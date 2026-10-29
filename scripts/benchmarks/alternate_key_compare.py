#!/usr/bin/env python3
"""CI comparison of equivalent two-key records: proteins versus named alternate keys."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import platform
import statistics
import sys
import time

from node import Node, save
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "schema_checks"))
from alternate_keys import task_definition
from node_ops import ENVIRONMENT, install, require

FIELDS = ["record_id", "group_id", "owner_id", "payload"]
PAYLOAD = "p" * 4096


def mutation(key, fields=None, operation="update"):
    return {"type": "mutation", "schema": "Task", "mutation_type": operation,
            "key_value": {"hash": key}, "fields_and_values": fields or {},
            "durability": "durable", "convergence": "sync"}


def values(key, group="g0"):
    return {"record_id": key, "group_id": group, "owner_id": "owner", "payload": PAYLOAD}


def declare(node, alternate):
    definition = task_definition("Task")
    if not alternate:
        definition.pop("alternate_keys")
    install(node, definition)
    if not alternate:
        for name, hash_field in (("ByGroup", "group_id"), ("ByOwner", "owner_id")):
            peer = dict(definition, name=name, descriptive_name=name, schema_type="HashRange",
                        key={"hash_field": hash_field, "range_field": "record_id"})
            install(node, peer)
    for offset in range(0, 256, 32):
        node.call("/api/mutations/batch",
                  [mutation(f"t{i:04}", values(f"t{i:04}"), "create") for i in range(offset, offset + 32)])


def query(alternate, name, predicate, limit=1000):
    body = {"schema_name": "Task" if alternate or name == "primary" else "ByGroup",
            "fields": FIELDS, "filter": predicate, "limit": limit}
    if alternate and name != "primary":
        body["key_name"] = "by_group"
    return body


def check_rows(reply, expected):
    rows = reply.get("results", [])
    require(len(rows) == len(expected), "measurement returned an incomplete record count")
    actual = {row["fields"]["record_id"]: row["fields"] for row in rows}
    require(actual == expected, "measurement returned different logical records")


def timed(node, label, pair, case, ordinal, path, body, records):
    reply, elapsed, receipt = node.call(path, body, receipt=True)
    records.append({"label": label, "pair": pair, "case": case, "ordinal": ordinal,
                    "latency_ms": elapsed, "receipt": receipt})
    if path != "/api/query":
        require(reply.get("durability") == "durable", "measurement lacks a durable acknowledgement")
    return reply


def measure_reads(node, alternate, label, pair, records):
    for case in ("primary_read", "alternate_exact", "alternate_range32"):
        for i in range(256):
            key = f"t{i:04}"
            if case == "primary_read":
                body = query(alternate, "primary", {"HashKey": key})
                expected = {key: values(key)}
            elif case == "alternate_exact":
                body = query(alternate, "by_group", {"HashRangeKey": {"hash": "g0", "range": key}})
                expected = {key: values(key)}
            else:
                body = query(alternate, "by_group", {"HashKey": "g0"}, limit=32)
                expected = {f"t{j:04}": values(f"t{j:04}") for j in range(32)}
            check_rows(timed(node, label, pair, case, i, "/api/query", body, records), expected)


def measure_writes(node, alternate, label, pair, records):
    for i in range(256):
        key = f"t{i:04}"
        timed(node, label, pair, "payload_update", i, "/api/mutation",
              mutation(key, {"payload": PAYLOAD + "u"}), records)
    for i in range(256):
        timed(node, label, pair, "key_move", i, "/api/mutation",
              mutation(f"t{i:04}", {"group_id": "g1"}), records)
    expected = {f"t{i:04}": dict(values(f"t{i:04}", "g1"), payload=PAYLOAD + "u") for i in range(256)}
    check_rows(node.call("/api/query", query(alternate, "by_group", {"HashKey": "g1"}))[0], expected)
    check_rows(node.call("/api/query", query(alternate, "by_group", {"HashKey": "g0"}))[0], {})
    for i in range(16):
        batch = [mutation(f"new{i:02}-{j:02}", values(f"new{i:02}-{j:02}", "new"), "create")
                 for j in range(32)]
        timed(node, label, pair, "batch_insert32", i, "/api/mutations/batch", batch, records)
    for i in range(128):
        timed(node, label, pair, "delete", i, "/api/mutation",
              mutation(f"t{i:04}", operation="delete"), records)
    check_rows(node.call("/api/query", query(alternate, "by_group", {"HashKey": "g1"}))[0],
               {key: fields for key, fields in expected.items() if int(key[1:]) >= 128})


def mixed(node, alternate, label, pair, records):
    def client(worker):
        samples = []
        key = f"mixed-{worker}"
        node.call("/api/mutation", mutation(key, values(key, "mixed"), "create"))
        for ordinal in range(32):
            timed(node, label, pair, "mixed_read", worker * 32 + ordinal, "/api/query",
                  query(alternate, "by_group", {"HashRangeKey": {"hash": "mixed", "range": key}}), samples)
            timed(node, label, pair, "mixed_write", worker * 32 + ordinal, "/api/mutation",
                  mutation(key, {"payload": PAYLOAD + str(ordinal)}), samples)
        return samples
    with ThreadPoolExecutor(max_workers=4) as pool:
        for samples in pool.map(client, range(4)):
            records.extend(samples)


def arm(args, label, pair, output):
    alternate = label == "candidate"
    binary = getattr(args, label)
    revision = getattr(args, label + "_revision")
    node = Node(binary, revision, output, environment=ENVIRONMENT,
                temporary_directory=Path(os.environ["RUNNER_TEMP"]).resolve())
    records = []
    try:
        node.start()
        declare(node, alternate)
        for _ in range(8):
            node.call("/api/query", query(alternate, "by_group", {"HashKey": "g0"}, limit=32))
        measure_reads(node, alternate, label, pair, records)
        measure_writes(node, alternate, label, pair, records)
        mixed(node, alternate, label, pair, records)
        node.stop()
        node.start()
        check_rows(node.call("/api/query", query(alternate, "by_group", {"HashKey": "new"}))[0],
                   {f"new{i:02}-{j:02}": values(f"new{i:02}-{j:02}", "new")
                    for i in range(16) for j in range(32)})
        save(output / "status.json", node.call("/api/status")[0])
    finally:
        node.close()
        save(output / "samples.json", records)
    require(len(records) == 1680, "comparison request coverage differs")
    return records


def quantile(values, fraction):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))]


def summary(args, records):
    lines = ["## Alternate-key spike latency", "",
             f"Baseline: `{args.baseline_revision}` (three schemas with proteins).",
             f"Candidate: `{args.candidate_revision}` (one schema with two alternate keys).", "",
             "Four fresh-home pairs use orders AB, BA, AB, BA. Fields, keys, and payloads match.",
             "The home formats differ, so this is a logical workload comparison, not an identical-file control.", "",
             "| Case | Base P50 ms | New P50 ms | Base P95 ms | New P95 ms | P95 change |",
             "|---|---:|---:|---:|---:|---:|"]
    results = {}
    for case in sorted({record["case"] for record in records}):
        samples = {label: [r["latency_ms"] for r in records if r["case"] == case and r["label"] == label]
                   for label in ("baseline", "candidate")}
        base, new = samples["baseline"], samples["candidate"]
        b95, n95 = quantile(base, .95), quantile(new, .95)
        results[case] = {label: {"count": len(items), "p50_ms": statistics.median(items),
                                "p95_ms": quantile(items, .95), "p99_ms": quantile(items, .99)}
                         for label, items in samples.items()}
        lines.append(f"| {case} | {statistics.median(base):.3f} | {statistics.median(new):.3f} | "
                     f"{b95:.3f} | {n95:.3f} | {(n95 / b95 - 1) * 100:+.1f}% |")
    lines += ["", f"Valid timed calls: {len(records)}. Request errors: 0.",
              "Mixed clients alternate a read and a write throughout each arm.",
              "All arms verify durable writes, logical records, reopen, and owned-home cleanup.", ""]
    text = "\n".join(lines)
    save(args.output / "summary.json", {"baseline": args.baseline_revision, "candidate": args.candidate_revision,
                                      "cases": results, "calls": len(records), "errors": 0,
                                      "machine": platform.uname()._asdict()})
    (args.output / "summary.md").write_text(text)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
        stream.write(text)
    print(text)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for label in ("baseline", "candidate"):
        parser.add_argument("--" + label, type=Path, required=True)
        parser.add_argument("--" + label + "-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    require(os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted", "CI only")
    runner = Path(os.environ["RUNNER_TEMP"]).resolve()
    require(not args.output.exists() and args.output.parent.resolve().is_relative_to(runner),
            "fresh output under RUNNER_TEMP required")
    args.output.mkdir(mode=0o700)
    records = []
    for pair in range(4):
        for label in (("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")):
            directory = args.output / f"pair-{pair}-{label}"
            directory.mkdir(mode=0o700)
            records.extend(arm(args, label, pair, directory))
    summary(args, records)


if __name__ == "__main__":
    main()
