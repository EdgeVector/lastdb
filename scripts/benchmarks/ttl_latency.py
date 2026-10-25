#!/usr/bin/env python3
"""Measure active TTL costs through real owner-socket writes on generated homes."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import gzip
import json
import os
from pathlib import Path
import threading
import time
import signal
import statistics

from node import Node, file_hash, save
from phases import capture_phases
from workload import describe, encoded, Workload
from ttl_maintenance import maintain

FIELDS = ["bucket", "id", "title", "body"]
STATES = ("no-policy", "tracking-only", "active-sweep", "active-sweep-compact")
CALLS, BATCH, EXPIRED = 512, 8, 8192
OLD_HASHES = [f"generated-{n:02d}" for n in range(16)]
LIVE, OLD = "TtlLatencyLive", "TtlLatencyExpired"


def mutations(schema, numbers, expired=False):
    records = []
    for n in numbers:
        key = f"{1:020d}-{n:09d}" if expired else f"live-{n:09d}"
        bucket = OLD_HASHES[n // 512] if expired else "generated"
        fields = {"bucket": bucket, "id": key, "title": f"value-{n}", "body": "x" * 1024}
        coordinate = {"hash": bucket, "range": key} if expired else {"hash": key}
        records.append({"type": "mutation", "schema": schema, "mutation_type": "create",
                        "durability": "durable", "key_value": coordinate, "fields_and_values": fields})
    return encoded({"mutations": records})


def seed(node, state):
    for schema, kind, keys in ((LIVE, "Hash", {"hash_field": "id"}),
                                (OLD, "HashRange", {"hash_field": "bucket", "range_field": "id"})):
        definition = {"source": "system_seed", "name": schema, "descriptive_name": schema,
                      "schema_type": kind, "key": keys, "fields": FIELDS,
                      "field_descriptions": {f: f"{schema} TTL latency {f}" for f in FIELDS}}
        node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": definition})
    definitions = {schema: node.call(f"/api/schemas/{schema}")[0]["schema"] for schema in (LIVE, OLD)}
    identities = {schema: set(definition["field_hashes"].values())
                  for schema, definition in definitions.items()}
    if (any(len(values) != len(FIELDS) for values in identities.values())
            or identities[LIVE].intersection(identities[OLD])):
        raise RuntimeError("TTL comparison schemas must have distinct canonical fields")
    save(node.output / "schema-identities.json", definitions)
    for start in range(0, EXPIRED, 128):
        result, _, _ = node.call("/api/mutations/batch", mutations(OLD, range(start, start + 128), True))
        Workload.validate_write(result, 128)
    if state != "no-policy":
        for schema, ttl, hashes in ((LIVE, 3600, []), (OLD, 30, OLD_HASHES)):
            node.call("/api/db/schema-retention", {"action": "set", "schema": schema,
                      "ttl_seconds": ttl, "hash_partitions": hashes, "confirm_expire_existing": True})
    node.stop()
    node.environment["LASTDB_TTL_SWEEP"] = "1" if state.startswith("active") else "0"
    node.start()


def writes(node, clients, output):
    # Build every payload before timing. Both client counts send 512 calls of eight records.
    jobs = [(n, mutations(LIVE, range(n * BATCH, (n + 1) * BATCH))) for n in range(CALLS)]
    barrier = threading.Barrier(clients + 1)

    def worker(client):
        barrier.wait(timeout=30)
        rows = []
        for n, body in jobs[client::clients]:
            request_id = f"ttl-{clients}-{n}"
            result, elapsed, _ = node.call("/api/mutations/batch", body, request_id=request_id)
            Workload.validate_write(result, BATCH)
            rows.append({"case": f"ttl-write/{clients}", "kind": "write", "client": client,
                         "request_id": request_id, "records": BATCH, "elapsed_ms": elapsed})
        return rows

    with ThreadPoolExecutor(max_workers=clients) as pool:
        futures = [pool.submit(worker, n) for n in range(clients)]
        first = time.time()
        started = time.perf_counter()
        barrier.wait(timeout=30)
        rows = [row for future in futures for row in future.result()]
        elapsed, last = time.perf_counter() - started, time.time()
    metric = describe(rows, elapsed)
    metric.update(clients=clients, batch_records=BATCH, first_unix=first, last_unix=last)
    metric["phase_coverage"] = capture_phases(node, output, f"ttl-write/{clients}", 0, rows)
    with gzip.open(output / "samples.jsonl.gz", "wt") as stream:
        for row in rows:
            stream.write(json.dumps(row) + "\n")
    return metric


def validate_live(node):
    # Exact reads verify every accepted generated value, after the timed window.
    for start in range(0, CALLS * BATCH, 32):
        queries = [{"schema_name": LIVE, "fields": FIELDS, "filter": {"HashKey": f"live-{n:09d}"}}
                   for n in range(start, min(start + 32, CALLS * BATCH))]
        result, _, _ = node.call("/api/queries/batch", {"queries": queries})
        replies = result["results"]
        if len(replies) != len(queries):
            raise RuntimeError("Incomplete TTL value verification")
        for n, reply in zip(range(start, start + len(queries)), replies):
            if reply["status"] != 200:
                raise RuntimeError("A batched TTL verification query failed")
            response = reply["response"]
            data = response.get("data", response)
            records = data.get("results", [])
            if (len(records) != 1 or records[0]["fields"]["title"] != f"value-{n}"
                    or records[0]["fields"]["body"] != "x" * 1024
                    or records[0]["fields"]["id"] != f"live-{n:09d}"
                    or records[0]["fields"]["bucket"] != "generated"):
                raise RuntimeError("An accepted live TTL value changed or expired")


def measure(binary, revision, output, state, clients, maintenance):
    output.mkdir()
    node = Node(binary, revision, output, environment={"LASTDB_TTL_SWEEP": "0",
                "LASTDB_SELF_METRICS_SAMPLE_SECS": "1"})
    result, compact = {"complete": False}, None
    try:
        node.start()
        seed(node, state)
        with ThreadPoolExecutor(max_workers=1) as pool:
            if state.endswith("compact"):
                compact = pool.submit(compact_during_writes, node)
            metric = writes(node, clients, output)
            if compact:
                evidence = compact.result(timeout=120)
                save(output / "compaction.json", evidence)
                if not (evidence["first_unix"] < metric["last_unix"]
                        and evidence["last_unix"] > metric["first_unix"]):
                    raise RuntimeError("Compaction did not overlap the write window")
        ledger = node.call("/api/db/delete-ledger")[0]["delete_ledger"]
        entries = [e for e in ledger["entries"] if e["caller"] == "ttl-sweep-purge" and e["committed"]]
        overlap = [e for e in entries if metric["first_unix"] <=
                   datetime.fromisoformat(e["at"].replace("Z", "+00:00")).timestamp() <= metric["last_unix"]]
        save(output / "ttl-ledger.json", entries)
        metric["ttl_batches_planned_during_writes"] = len(overlap)
        save(output / "status.json", node.call("/api/status")[0])
        save(output / "metrics.json", metric)
        if state.startswith("active") and not overlap:
            raise RuntimeError("No durable TTL purge overlaps the measured write window")
        if not state.startswith("active") and entries:
            raise RuntimeError("The control unexpectedly ran a TTL purge")
        if maintenance and state.startswith("active"):
            maintain(node, output, LIVE, OLD, CALLS * BATCH)
        validate_live(node)
        result = {"complete": True, "known_live_values": CALLS * BATCH, **metric}
    except BaseException as failure:
        result["error"] = type(failure).__name__ + ": " + str(failure)
        raise
    finally:
        node.close()
        save(output / "result.json", result)
    return result


def compact_during_writes(node):
    time.sleep(1)
    first = time.time()
    response = node.call("/api/db/compact", {"dry_run": False, "full_copy": True})[0]
    report = response["compact"]["native_disk"]
    if (report.get("dry_run") is not False or report.get("executed") is not True
            or report.get("records_copied", 0) <= 0):
        raise RuntimeError("The compaction response is only a preview")
    return {"first_unix": first, "last_unix": time.time(), "response": response}


def interrupted(_signum, _frame):
    raise KeyboardInterrupt("TTL benchmark interrupted")


def summary_lines(manifest):
    lines = ["## Active TTL write latency", "",
             "512 durable calls per case, eight records per call. Two alternating source pairs.",
             "Each aggregate is the median of two per-run p95 values. Shared-runner speed changes are advisory.",
             "The sampler uses a one-second CI override. TTL batch size and duty use product defaults.",
             "TTL overlap counts committed purge batches planned inside the write window; it does not cover every request.",
             "", "| State | Clients | Base p95 ms | Candidate p95 ms | Source change | Candidate TTL cost vs no policy |",
             "|---|---:|---:|---:|---:|---:|"]
    values = {}
    for label in ("baseline", "candidate"):
        for state in STATES:
            for clients in (1, 4):
                values[label, state, clients] = statistics.median(
                    r["p95_ms"] for r in manifest["runs"]
                    if (r["label"], r["state"], r["clients"]) == (label, state, clients))
    for state in STATES:
        for clients in (1, 4):
            base, candidate = (values[label, state, clients] for label in ("baseline", "candidate"))
            control = values["candidate", "no-policy", clients]
            lines.append(f"| {state} | {clients} | {base:.3f} | {candidate:.3f} | "
                         f"{100 * (candidate / base - 1):+.1f}% | {100 * (candidate / control - 1):+.1f}% |")
    lines.extend(["", "| Pair | Source | State | Clients | p50 / p95 / p99 ms | Calls/s | TTL overlap batches |",
                  "|---|---|---|---:|---:|---:|---:|"])
    for run in manifest["runs"]:
        lines.append(f"| {run['pair']} | {run['label']} | {run['state']} | {run['clients']} | "
                     f"{run['p50_ms']:.3f} / {run['p95_ms']:.3f} / {run['p99_ms']:.3f} | "
                     f"{run['requests_per_second']:.1f} | {run['ttl_batches_planned_during_writes']} |")
    return lines


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for label in ("baseline", "candidate"):
        parser.add_argument(f"--{label}", type=Path, required=True)
        parser.add_argument(f"--{label}-revision", required=True)
    parser.add_argument("--driver-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
        parser.error("TTL latency measurements require a GitHub-hosted Actions runner")
    signal.signal(signal.SIGTERM, interrupted)
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = {"driver_revision": args.driver_revision, "pairs": 2, "calls": CALLS,
                "batch_records": BATCH, "expired_seed_records": EXPIRED,
                "expired_hashes": len(OLD_HASHES), "expired_records_per_hash": 512,
                "sampler_seconds": 1, "ttl_pacing": "product defaults", "runs": [], "complete": False,
                "drivers": {name: file_hash(Path(__file__).parent / name) for name in
                            ("ttl_latency.py", "ttl_maintenance.py", "node.py", "phases.py", "workload.py")},
                "binaries": {}}
    for label in ("baseline", "candidate"):
        manifest["binaries"][label] = {"revision": getattr(args, f"{label}_revision"),
                                       "sha256": file_hash(getattr(args, label))}
    save(args.output / "benchmark.json", manifest)
    for pair in range(2):
        order = ("baseline", "candidate") if pair == 0 else ("candidate", "baseline")
        for label in order:
            for state in STATES if pair == 0 else reversed(STATES):
                for clients in (1, 4):
                    name = f"{pair}-{label}-{state}-{clients}"
                    print(json.dumps({"case": name}), flush=True)
                    result = measure(getattr(args, label), getattr(args, f"{label}_revision"),
                                     args.output / name, state, clients, label == "candidate")
                    manifest["runs"].append({"pair": pair, "label": label, "state": state,
                                              "clients": clients, "directory": name, **result})
                    save(args.output / "benchmark.json", manifest)
    manifest["complete"] = True
    save(args.output / "benchmark.json", manifest)
    summary = "\n".join(summary_lines(manifest)) + "\n"
    (args.output / "summary.md").write_text(summary)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
        stream.write(summary)
    print(summary)


if __name__ == "__main__":
    main()
