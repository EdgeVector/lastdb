#!/usr/bin/env python3
"""Compare 16 durable writers on paired CI programs and publish the writer report."""

from __future__ import annotations

import argparse
from bisect import bisect_left
from concurrent.futures import ThreadPoolExecutor
import hashlib, json, os, re, signal, sys, threading, time
from pathlib import Path
from statistics import mean, median

from disk_budget import require_search_off
from node import Node, file_hash, publish_summary, save
from phases import capture_phases
from process_counters import counter_delta, read_counters
from run import host_details
from workload import FIELDS, HASH, SCHEMA, Workload, describe, encoded, fields, key

RECORDS = 100000


class Writers:
    def __init__(self, node):
        self.node = node
        self.versions = {}
        self.sequence = 0

    def mutation(self, number, version, create=False):
        value = fields(number, version)
        return {"type": "mutation", "schema": SCHEMA, "mutation_type": "create" if create else "update",
                "durability": "durable", "key_value": {"hash": HASH, "range": key(number)},
                "fields_and_values": value if create else {n: value[n] for n in ("title", "body")}}

    def seed(self):
        definition = {"source": "system_seed", "name": SCHEMA, "descriptive_name": SCHEMA,
                      "schema_type": "HashRange", "key": {"hash_field": "bucket", "range_field": "id"},
                      "fields": FIELDS, "field_types": {f: "String" for f in FIELDS},
                      "field_descriptions": {f: f"generated benchmark {f}" for f in FIELDS}}
        self.node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": definition})
        actual = self.node.call(f"/api/schema/{SCHEMA}")[0]["schema"]
        if len(set(actual["field_hashes"].values())) != len(FIELDS):
            raise ValueError("Benchmark schema must have distinct canonical fields")
        save(self.node.output / "schema-identities.json", {SCHEMA: actual})
        for first in range(0, RECORDS, 128):
            numbers = range(first, min(first + 128, RECORDS))
            response, _, _ = self.node.call("/api/mutations/batch", {
                "mutations": [self.mutation(n, "seed", True) for n in numbers]})
            Workload.validate_write(response, len(numbers))
        if self.node.call("/api/db/flush", {})[0].get("durable") is not True:
            raise ValueError("Generated seed lacks its durable flush")
        self.node.stop()
        self.node.start()
        self.verify()

    def verify(self):
        numbers = [*range(32), *range(RECORDS - 256, RECORDS)]
        for first in range(0, len(numbers), 64):
            selected = numbers[first:first + 64]
            coordinates = [[HASH, key(n)] for n in selected]
            result = self.node.call("/api/query", {"schema_name": SCHEMA,
                "filter": {"HashRangeKeys": coordinates}, "limit": len(selected)})[0]
            expected = {(HASH, key(n)): fields(n, self.versions.get(n, "seed")) for n in selected}
            actual = {(r["key"]["hash"], r["key"]["range"]): r["fields"] for r in result["results"]}
            if result["returned_count"] != len(selected) or len(result["results"]) != len(selected) or actual != expected:
                raise ValueError("A known-key read differs from the last durable write")

    def jobs(self, name, clients, batch, requests):
        jobs = [[] for _ in range(clients)]
        owners = [{RECORDS - 256 + client + clients * n for n in range(256 // clients)} for client in range(clients)]
        if sum(map(len, owners)) != len(set.union(*owners)):
            raise ValueError("Writers unexpectedly share keys")
        for index in range(requests):
            client, turn = index % clients, index // clients
            owned = sorted(owners[client])
            numbers = [owned[(turn * batch + n) % len(owned)] for n in range(batch)]
            version = f"{name}-{index}"
            body = encoded({"mutations": [self.mutation(n, version) for n in numbers]})
            jobs[client].append((body, f"wc-{name}-{index:06d}", numbers))
            self.versions.update({n: version for n in numbers})
        return jobs

    def measure(self, name, clients, batch=1, requests=2048):
        jobs = self.jobs(name, clients, batch, requests)
        barrier = threading.Barrier(clients + 1)

        def worker(client):
            rows = []
            barrier.wait(timeout=30)
            for body, request_id, numbers in jobs[client]:
                result, elapsed, _ = self.node.call("/api/mutations/batch", body, request_id=request_id)
                Workload.validate_write(result, len(numbers))
                rows.append({"case": name, "kind": "write", "client": client, "records": len(numbers),
                             "request_id": request_id, "request_sha256": hashlib.sha256(body).hexdigest(),
                             "elapsed_ms": elapsed, "resident_stages_us": result.get("resident_stages_us")})
            return rows

        status = self.node.call("/api/status")[0]["status"]
        require_search_off(self.node, self.node.output, name)
        with ThreadPoolExecutor(max_workers=clients) as pool:
            futures = [pool.submit(worker, client) for client in range(clients)]
            before = {"daemon": read_counters(self.node.process.pid), "client": read_counters(os.getpid())}
            started_us, started = time.time_ns() // 1000, time.monotonic_ns()
            barrier.wait(timeout=30)
            rows = [row for future in futures for row in future.result()]
            ended, ended_us = time.monotonic_ns(), time.time_ns() // 1000
            after = {"daemon": read_counters(self.node.process.pid), "client": read_counters(os.getpid())}
        if len(rows) != requests:
            raise ValueError("The write case did not complete all requests")
        result = {"name": name, "clients": clients, "batch_records": batch, "requests": requests,
                  "maximum_inflight_records": clients * batch,
                  "workers": status["uds"]["workers"], "native_timing": self.node.native_timing,
                  "stdio_log_rotation": self.node.environment["LASTDBD_STDIO_LOG_ROTATION"],
                  "case_started_unix_us": started_us, "case_ended_unix_us": ended_us,
                  "wall_ns": ended - started, "metrics": describe(rows, (ended - started) / 1e9),
                  "process": {label: counter_delta(before[label], after[label]) for label in before}}
        for label, observation in result["process"].items():
            cpu = observation["derived"]["process_cpu_ns"]
            if cpu is None or not observation["process_identity_same"]:
                raise ValueError(f"Missing stable {label} CPU observations")
            observation["average_cpu_cores"] = cpu / observation["elapsed_ns"]
        result["phase_coverage"] = capture_phases(self.node, self.node.output, name, self.sequence, rows)
        self.sequence += 1
        save(self.node.output / f"{name}-samples.json", rows)
        self.verify()
        save(self.node.output / f"{name}.json", result)
        print(json.dumps({"case": name, "metrics": result["metrics"]}), flush=True)
        return result


def summarize(events):
    names = {name for event in events for name in event.get("phases_us", {})}
    return {name: {"reported": sum(name in e.get("phases_us", {}) for e in events),
                   "mean_us": mean(e["phases_us"][name] for e in events if name in e.get("phases_us", {})),
                   "median_us": median(e["phases_us"][name] for e in events if name in e.get("phases_us", {}))}
            for name in sorted(names)}


def totals(events):
    names = {name for event in events for name in event.get("counts", {})}
    return {name: {"reported": sum(name in e.get("counts", {}) for e in events),
                   "total": sum(e.get("counts", {}).get(name, 0) for e in events)}
            for name in sorted(names)}


def native_events(directory, cases):
    path = directory / "node.log"
    if path.stat().st_size > 256 * 2**20:
        raise ValueError("The investigation log exceeds its 256 MiB bound")
    events = []
    with path.open() as source:
        for line in source:
            for marker in ("LASTDB_SNAPSHOT_TIMING ", "LASTDB_WRITE_REQUEST_TIMING "):
                if marker in line:
                    events.append(json.loads(line.split(marker, 1)[1]))
    for case in cases:
        if not case["native_timing"]:
            continue
        first, last = case["case_started_unix_us"], case["case_ended_unix_us"]
        selected = [e for e in events if first <= e["started_us"] <= e["ended_us"] <= last]
        requests = [e for e in selected if e["kind"] == "request"]
        if len(requests) != case["requests"] or any(e["status"] != 200 for e in requests):
            raise ValueError("Native HTTP request clocks do not cover all successful measured calls")
        scopes = [e for e in selected if e["kind"] == "scope" and e["background"] is False]
        sequences = sorted(e["sequence"] for e in scopes)
        if len(scopes) < case["requests"] or len(set(sequences)) != len(sequences):
            raise ValueError("Observed foreground storage scopes are incomplete or duplicated")
        commits = [e for e in selected if e["kind"] == "commit" and e["ok"]
                   and (pos := bisect_left(sequences, e["first_sequence"])) < len(sequences)
                   and sequences[pos] <= e["sequence"]]
        if any(sum(c["first_sequence"] <= seq <= c["sequence"] for c in commits) != 1 for seq in sequences):
            raise ValueError("A write scope lacks one distinct durable publication")
        case["native"] = {"http_requests": len(requests), "request_phases": summarize(requests),
                          "socket_queue_mean_us": mean(e["queue_wait_us"] for e in requests),
                          "scopes": len(scopes), "extra_foreground_scopes": len(scopes) - len(requests),
                          "scope_population": "All observed foreground storage work, including author-clock metadata",
                          "commits": len(commits), "requests_per_commit": mean(c["requests"] for c in commits),
                          "scope_phases": summarize(scopes), "commit_phases": summarize(commits),
                          "commit_mean_us": mean(c["total_us"] for c in commits),
                          "commit_slots_include_background": True, "scope_phase_totals_overlap": False}
        save(directory / f"{case['name']}.json", case)


def diagnostics(directory, cases):
    native_events(directory, cases)
    events = [json.loads(line.split("LASTDB_SNAPSHOT_TIMING ", 1)[1])
              for line in (directory / "node.log").open() if "LASTDB_SNAPSHOT_TIMING " in line]
    for case in cases:
        if not case["native_timing"]:
            continue
        first, last = case["case_started_unix_us"], case["case_ended_unix_us"]
        selected = [e for e in events if first <= e["started_us"] <= e["ended_us"] <= last]
        prepare = [e for e in selected if e["kind"] == "prepare"]
        if len(prepare) < case["requests"]:
            raise ValueError("Logical preparation records do not cover all measured calls")
        case["preparation"] = {"records": len(prepare), "phases": summarize(prepare), "counts": totals(prepare)}
        reuse = [e for e in selected if e["kind"] == "write_reads"]
        if reuse and len(reuse) < case["requests"]:
            raise ValueError("Private write reuse records are incomplete")
        case["write_reads"] = {"records": len(reuse), "counts": totals(reuse),
                               "scope": "Absent instrumentation is unreported, not zero reuse"}
        save(directory / f"{case['name']}.json", case)


def measure_home(args, label, pair):
    revision, binary = getattr(args, f"{label}_revision"), getattr(args, label)
    directory = args.output / f"pair-{pair}-{label}"
    directory.mkdir()
    node = Node(binary, revision, directory,
                environment={"LASTDB_SEARCH_ENABLED": "0", "LASTDBD_STDIO_LOG_ROTATION": "1"}, record_protocol=True)
    home = {"label": label, "pair": pair, "revision": revision,
            "directory": directory.name, "complete": False, "cases": []}
    try:
        node.start()
        require_search_off(node, directory, "before")
        writes = Writers(node)
        writes.seed()
        home["cases"].append(writes.measure("ordinary-16", 16))
        if pair == 2:
            node.stop()
            node.native_timing = True
            node.environment["LASTDBD_STDIO_LOG_ROTATION"] = "0"
            node.start()
            writes.verify()
            home["cases"].append(writes.measure("native-16", 16))
        require_search_off(node, directory, "after")
        if node.call("/api/db/native-status")[0]["native_disk"].get("writer_failed") is not False:
            raise ValueError("The native writer is unhealthy")
    finally:
        node.close()
    diagnostics(directory, home["cases"])
    home["complete"] = True
    return home


def render_report(output):
    data = json.loads((output / "write-gate.json").read_text())
    if not data["complete"] or len(data["homes"]) != 6 or not all(h["complete"] for h in data["homes"]):
        raise ValueError(f"Incomplete writer comparison: {data.get('error', 'missing homes')}")
    lines = ["# Paired 16-writer comparison", "",
             f"Driver: `{data['driver_revision']}`. Each fresh home has {data['records']} generated records.",
             "Six fresh homes form three alternating base/candidate pairs on this runner.",
             "Every client owns different keys in one schema and one hash. Search is off.",
             "Each case completes 2,048 single-record durable RPCs and verifies the final values.",
             "The final pair also records native phases after a restart. Native latency is separate from the ordinary comparison.",
             "All phase averages cover complete foreground storage work; nested preparation stages overlap.",
             "Extra metadata scopes remain explicit. Missing instrumentation is unreported, not zero.", "",
             "| Home / case | P95 ms | Records/s | Daemon CPU cores | Logical prepare mean ms | Reused disk records |",
             "|---|---:|---:|---:|---:|---:|"]
    ordinary, detail = {label: [] for label in ("baseline", "candidate")}, []
    for home in data["homes"]:
        cleanup = json.loads((output / home["directory"] / "cleanup.json").read_text())
        if not cleanup["daemon_stopped"] or not cleanup["removed"]:
            raise ValueError("A generated writer home lacks complete cleanup")
        for case in home["cases"]:
            metrics = case["metrics"]
            if case["name"] == "ordinary-16":
                ordinary[home["label"]].append(metrics)
            prepare = case.get("native", {}).get("scope_phases", {}).get("logical_prepare_us", {}).get("mean_us")
            reuse = case.get("write_reads", {}).get("counts", {}).get("reused_records", {}).get("total")
            prep_text = f"{prepare / 1000:.3f}" if prepare is not None else "unreported"
            lines.append(f"| {home['directory']} / {case['name']} | {metrics['p95_ms']:.3f} | "
                         f"{metrics['records_per_second']:.2f} | {case['process']['daemon']['average_cpu_cores']:.3f} | "
                         f"{prep_text} | {reuse if reuse is not None else 'unreported'} |")
            if "preparation" in case:
                detail.extend(["", f"## {home['directory']} preparation", "", "```json",
                               json.dumps({"native": case["native"], "preparation": case["preparation"],
                                           "write_reads": case["write_reads"]}, indent=2, sort_keys=True), "```"])
    if any(len(values) != 3 for values in ordinary.values()):
        raise ValueError("Ordinary measurements lack three complete pairs")
    lines.extend(["", "| Ordinary median of three cases | Base | Candidate | Change |", "|---|---:|---:|---:|"])
    for field in ("p95_ms", "records_per_second"):
        base, cand = (median(row[field] for row in ordinary[label]) for label in ("baseline", "candidate"))
        lines.append(f"| {field} | {base:.3f} | {cand:.3f} | {(cand / base - 1) * 100:+.1f}% |")
    lines.extend(["", "Speed changes remain advisory. Completion and verified uploads remain required.",
                  "Fresh homes and the focused history do not replay the full nightly workload."])
    return "\n".join(lines + detail) + "\n"


def _interrupt(_signum, _frame):
    raise KeyboardInterrupt("The paired writer comparison received an interruption")


def main():
    if sys.argv[1:2] == ["report"]:
        parser = argparse.ArgumentParser(prog="write_gate.py report", description=__doc__)
        parser.add_argument("--output", type=Path, required=True)
        args = parser.parse_args(sys.argv[2:])
        try:
            report, code = render_report(args.output), 0
        except (OSError, ValueError, KeyError, TypeError) as error:
            report, code = f"# Paired writer comparison incomplete\n\n{type(error).__name__}: {error}\n", 1
        publish_summary(args.output, report)
        return code
    parser = argparse.ArgumentParser(description=__doc__)
    for label in ("baseline", "candidate"):
        parser.add_argument(f"--{label}", type=Path, required=True)
        parser.add_argument(f"--{label}-revision", required=True)
    parser.add_argument("--driver-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
        parser.error("Writer comparisons require a GitHub-hosted Actions runner")
    if any(not re.fullmatch(r"[0-9a-f]{40}", r) for r in (args.baseline_revision, args.candidate_revision, args.driver_revision)):
        parser.error("Every source revision must be a complete SHA")
    parent = Path(os.environ["BENCH_OUTPUT"]).resolve(strict=True)
    if args.output.exists() or args.output.is_symlink() or args.output.parent.resolve() != parent:
        parser.error("The comparison requires a fresh child of the external CI result directory")
    if parent.parent != Path(os.environ["RUNNER_TEMP"]).resolve(strict=True):
        parser.error("The result directory must be outside Git in RUNNER_TEMP")
    args.output.mkdir()
    programs = {label: {"revision": getattr(args, f"{label}_revision"), "sha256": file_hash(getattr(args, label))}
                for label in ("baseline", "candidate")}
    manifest = {"complete": False, "driver_revision": args.driver_revision, "host": host_details(), "records": RECORDS,
                "pairs": 3, "requests_per_case": 2048, "clients": 16, "batch_records": 1, "schema": "GitHubBenchmark",
                "hash": "generated", "search_enabled": False, "programs": programs, "homes": [],
                "scope": "Three alternating pairs; separate final-pair native capture"}
    signal.signal(signal.SIGTERM, _interrupt)
    try:
        for pair in range(3):
            for label in (("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")):
                print(json.dumps({"pair": pair, "version": label}), flush=True)
                manifest["homes"].append(measure_home(args, label, pair))
                save(args.output / "write-gate.json", manifest)
        manifest["complete"] = True
    except BaseException as error:
        manifest["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        save(args.output / "write-gate.json", manifest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
