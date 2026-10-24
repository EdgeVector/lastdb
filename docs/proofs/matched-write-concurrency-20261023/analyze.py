#!/usr/bin/env python3
"""Summarize real client latency and shared physical commits without double counts."""

import argparse
from collections import Counter
import gzip
import json
import math
from pathlib import Path
import statistics

HERE = Path(__file__).resolve().parent
OUTER_PHASES = ("payload_us", "prior_generation_us", "lookup_files_us", "new_generation_us",
                "root_publication_us", "final_drain_us", "memory_publication_us")


def load(path):
    if path.exists():
        return json.loads(path.read_text())
    return json.loads(gzip.decompress(path.with_suffix(path.suffix + ".gz").read_bytes()))


def stats(values):
    values = sorted(values)
    return {"count": len(values), "average_ms": statistics.mean(values),
            "median_ms": statistics.median(values), "p95_ms": values[math.ceil(.95 * len(values)) - 1],
            "p99_ms": values[math.ceil(.99 * len(values)) - 1], "maximum_ms": values[-1]}


def phases(path, samples, system):
    marker = "LASTDB_SNAPSHOT_TIMING "
    events = [json.loads(line.split(marker, 1)[1]) for line in gzip.decompress(
        (path / "daemon.txt.gz").read_bytes()).decode().splitlines() if marker in line]
    first = min(row["start_unix_us"] for row in samples)
    last = max(row["end_unix_us"] for row in samples)
    scopes = [e for e in events if e["kind"] == "scope" and not e["background"]
              and first <= e["started_us"] <= e["ended_us"] <= last]
    commits = [e for e in events if e["kind"] == "commit" and e["ok"]]
    assert len(scopes) == len(samples) == len({s["sequence"] for s in scopes})
    used = {}
    for scope in scopes:
        matching = [c for c in commits if c.get("first_sequence", c["sequence"]) <= scope["sequence"] <= c["sequence"]
                    and scope["started_us"] <= c["started_us"] <= c["ended_us"] <= scope["ended_us"]]
        assert len(matching) == 1
        used[matching[0]["sequence"]] = matching[0]
    assert sum(c["requests"] for c in used.values()) == len(samples)
    if system == "Darwin":
        assert all(c["counts"]["device_flushes"] == 1 for c in used.values())
    disk_keys = sorted({key for c in used.values() for key in c["phases_us"]})
    count_keys = sorted({key for c in used.values() for key in c["counts"]})
    return {"native_scopes": len(scopes), "physical_commits": len(used),
        "commit_request_counts": dict(Counter(c["requests"] for c in used.values())),
        "request_phase_averages_ms": {key: statistics.mean(s["phases_us"].get(key, 0) / 1000 for s in scopes)
                                      for key in sorted(scopes[0]["phases_us"])},
        "physical_commit_average_ms": statistics.mean(c["total_us"] / 1000 for c in used.values()),
        "physical_phase_averages_ms": {key: statistics.mean(c["phases_us"].get(key, 0) / 1000 for c in used.values())
                                       for key in disk_keys},
        "physical_count_averages": {key: statistics.mean(c["counts"].get(key, 0) for c in used.values())
                                     for key in count_keys},
        "exclusive_disk_phase_names": OUTER_PHASES,
        "other_phases_are_nested": True, "shared_commit_cost_is_not_per_request_exclusive": True}


def analyze(path):
    inputs, writes = load(path / "inputs.json"), load(path / "writes.json")
    samples = writes["samples"]
    assert inputs["records_per_request"] == 8 and len(samples) == inputs["requests"]
    assert all(s["status"] == 200 and s["ok"] and s["receipt"]["durability"] == "durable" for s in samples)
    assert load(path / "summary.json")["complete"] and load(path / "cleanup.json")["removed"]
    assert load(path / "readback.json")["all_values_match"]
    assert load(path / "recovery.json")["all_values_match"] and load(path / "notice-count.json")["all_once"]
    assert load(path / "native-shutdown.json")["flush_ok"] and load(path / "shutdown.json")["exit_code"] == 0
    result = {"inputs": inputs, "client": stats([s["latency_ms"] for s in samples]),
              "records_per_second": writes["summary"]["records_per_second"],
              "expected_digest": writes["expected_sha256"], "recovery_receipts_pass": True}
    if inputs["profile"]:
        result.update(phases(path, samples, inputs.get("platform", "Darwin")))
        if inputs.get("github_actions"):
            counters = load(path / "atom-counters.json")
            assert counters["mismatches_total"] == 0 and counters["tally_complete"] is True
    (path / "analysis.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({k: v for k, v in result.items() if k not in ("expected_digest", "inputs")}, indent=2))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", type=Path)
    args = parser.parse_args()
    for path in args.paths:
        analyze(path.resolve(strict=True))


if __name__ == "__main__":
    main()
