#!/usr/bin/env python3
"""Join numeric native scope and commit times to real client request receipts."""

import gzip
from datetime import datetime
import json
import math
from pathlib import Path
import statistics
from collections import Counter

ROOT = Path(__file__).resolve().parent
DISK_PHASES = ("payload", "prior_generation", "lookup_files", "new_generation",
               "root_publication", "final_drain", "memory_publication")
REQUEST_PHASES = ("part_room", "admission", "logical_prepare", "capacity_prepare", "accept")
CHILD_PHASES = ("device_flush", "file_set_flush", "ordered_barrier")


def load(path):
    return json.loads(path.read_text())


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def stats(values):
    values = sorted(values)
    return {"average": statistics.mean(values), "median": statistics.median(values),
            "p95": values[math.ceil(.95 * len(values)) - 1],
            "p99": values[math.ceil(.99 * len(values)) - 1], "maximum": values[-1]}


def analyze(name):
    output = ROOT / name
    writes = load(output / "writes.json")
    samples = writes["samples"]
    raw = gzip.decompress((output / "daemon.txt.gz").read_bytes()).decode()
    marker = "LASTDB_SNAPSHOT_TIMING "
    events = [json.loads(line.split(marker, 1)[1]) for line in raw.splitlines() if marker in line]
    scopes = [event for event in events if event["kind"] == "scope" and not event["background"]]
    commits = [event for event in events if event["kind"] == "commit"]
    background = [event for event in events if event["kind"] == "scope" and event["background"]]
    background_commits = [c for c in commits if any(
        c["sequence"] == b["sequence"] and b["started_us"] <= c["started_us"]
        and c["ended_us"] <= b["ended_us"] for b in background)]
    ordered = sorted(background_commits, key=lambda c: c["started_us"])
    if any(a["ended_us"] > b["started_us"] for a, b in zip(ordered, ordered[1:])):
        raise RuntimeError("background commit intervals overlap; use interval union before attribution")
    rows, used = [], set()
    for sample in samples:
        selected = [s for s in scopes if sample["start_unix_us"] <= s["started_us"]
                    and s["ended_us"] <= sample["end_unix_us"]]
        if len(selected) != 1:
            raise RuntimeError(f"{name} request {sample['number']} has {len(selected)} foreground scopes")
        scope = selected[0]
        matching = [c for c in commits if c["sequence"] == scope["sequence"]
                    and scope["started_us"] <= c["started_us"] and c["ended_us"] <= scope["ended_us"]]
        if len(matching) != 1 or matching[0]["requests"] != 1 or not matching[0]["ok"]:
            raise RuntimeError("each measured scope needs one complete, exclusive, successful native commit")
        commit = matching[0]
        admission_start = scope["started_us"] + scope["phases_us"]["part_room_us"]
        admission_end = admission_start + scope["phases_us"]["admission_us"]
        background_overlap = sum(max(0, min(admission_end, c["ended_us"])
                                     - max(admission_start, c["started_us"]))
                                 for c in background_commits) / 1000
        if background_overlap > scope["phases_us"]["admission_us"] / 1000 + .02:
            raise RuntimeError("background commit overlap exceeds the measured admission interval")
        identity = (commit["sequence"], commit["started_us"])
        if identity in used:
            raise RuntimeError("a commit entered two client requests")
        used.add(identity)
        phases = {p + "_ms": scope["phases_us"].get(p + "_us", 0) / 1000 for p in REQUEST_PHASES}
        phases.update({p + "_ms": commit["phases_us"].get(p + "_us", 0) / 1000 for p in DISK_PHASES})
        phases["queue_other_ms"] = (scope["phases_us"]["durable_wait_us"] - commit["total_us"]) / 1000
        phases["commit_other_ms"] = commit["total_us"] / 1000 - sum(phases[p + "_ms"] for p in DISK_PHASES)
        phases["scope_other_ms"] = (scope["total_us"] - sum(scope["phases_us"].values())) / 1000
        phases["client_other_ms"] = sample["latency_ms"] - scope["total_us"] / 1000
        if min(phases.values()) < -.02 or abs(sum(phases.values()) - sample["latency_ms"]) > .001:
            raise RuntimeError("exclusive phase sum differs from the client duration")
        rows.append({"number": sample["number"], "mutation_type": sample["mutation_type"],
                     "latency_ms": sample["latency_ms"], "sequence": scope["sequence"],
                     "background_commit_overlap_with_admission_ms": background_overlap,
                     "start_unix_us": sample["start_unix_us"], "end_unix_us": sample["end_unix_us"],
                     "phases_ms": phases, "commit_us": commit["total_us"],
                     "nested_sync_ms": {p: commit["phases_us"].get(p + "_us", 0) / 1000
                                        for p in CHILD_PHASES}, "counts": commit["counts"]})
    save(output / "request-breakdown.json", rows)
    phase_stats = {key: stats([row["phases_ms"][key] for row in rows]) for key in rows[0]["phases_ms"]}
    p99 = sorted(rows, key=lambda row: row["latency_ms"])[math.ceil(.99 * len(rows)) - 1]
    result = {"whole_request": writes["summary"], "matched_requests": len(rows),
              "exclusive_phase_ms": phase_stats, "mean_phase_sum_ms": sum(s["average"] for s in phase_stats.values()),
              "nested_sync_ms": {key: stats([r["nested_sync_ms"][key] for r in rows]) for key in CHILD_PHASES},
              "mean_counts": {key: statistics.mean(r["counts"].get(key, 0) for r in rows)
                              for key in rows[0]["counts"]},
              "actual_p99_request": p99, "maximum_request": max(rows, key=lambda row: row["latency_ms"]),
              "mean_commit_ms": statistics.mean(r["commit_us"] / 1000 for r in rows),
              "background_commit_overlap_with_admission_ms": stats([
                  r["background_commit_overlap_with_admission_ms"] for r in rows]),
              "background_storage_operation_counts": dict(Counter(
                  c["counts"].get("storage_mutations", 0) for c in background_commits
                  if rows[0]["start_unix_us"] <= c["started_us"] <= rows[-1]["end_unix_us"])),
              "parts_before": writes["before"]["native_disk"]["parts"],
              "parts_after": writes["after"]["native_disk"]["parts"],
              "first_100_update_mean_ms": statistics.mean(r["latency_ms"] for r in rows[64:164]),
              "last_100_update_mean_ms": statistics.mean(r["latency_ms"] for r in rows[-100:]),
              "maintenance_refusals_in_measurement": sum(
                  "eight catch-ups" in line for line in raw.splitlines()
                  if line.startswith("20") and rows[0]["start_unix_us"] <=
                  datetime.fromisoformat(line.split()[0].replace("Z", "+00:00")).timestamp() * 1e6
                  <= rows[-1]["end_unix_us"])}
    save(output / "phase-summary.json", result)
    return result


def main():
    results = {}
    for name in ("control", "profile", "no-maintenance"):
        output = ROOT / name
        writes = load(output / "writes.json")
        recovery = load(output / "recovery.json")
        boundary = load(output / "stopped-boundaries.json")
        if (writes["summary"]["errors"] or not recovery["all_values_match"] or not recovery["clock_advanced"]
                or len(writes["samples"]) != 1000
                or any(s["receipt"].get("durability") != "durable" for s in writes["samples"])
                or not load(output / "native-shutdown.json")["flush_ok"]
                or load(output / "shutdown.json")["exit_code"] != 0
                or boundary["journal_directory_exists"] or boundary["journal_tail_exists"]):
            raise RuntimeError("a durable value, recovery, or shutdown receipt failed")
        if writes["before"]["native_disk"]["parts"]["auto_maintenance"] != (name != "no-maintenance"):
            raise RuntimeError("the maintenance selection differs from the requested measurement")
        if name == "control":
            results[name] = {"whole_request": writes["summary"]}
        else:
            results[name] = analyze(name)
    save(ROOT / "summary.json", results)
    print(json.dumps({name: {"whole_request": result["whole_request"],
                            "mean_phases": {key: value["average"] for key, value in
                                            result.get("exclusive_phase_ms", {}).items()}}
                      for name, result in results.items()}, indent=2))


if __name__ == "__main__":
    main()
