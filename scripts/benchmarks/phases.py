"""Capture request phases, batch-update tail diagnostics, and native commit correlations."""

from __future__ import annotations

from collections import Counter
import gzip
import json
from statistics import mean

from node import save


def tail_calls(samples):
    rows = []
    for sample in sorted(samples, key=lambda row: row["elapsed_ms"], reverse=True)[:5]:
        operation = sample.get("server_request_ops", {})
        before, after = sample.get("process_before", {}), sample.get("process_after", {})
        rows.append({
            "request_id": sample["request_id"], "rpc_ms": sample["elapsed_ms"],
            "server_ms": operation.get("duration_ms"), "server_completed_unix_ms": operation.get("ts_ms"),
            "request_phases_us": operation.get("phases"), "resident_stages_us": sample.get("resident_stages_us"),
            "observer_before_unix_us": before.get("unix_us"), "observer_after_unix_us": after.get("unix_us"),
            "process_delta": sample.get("process_delta"),
        })
    return rows


def update_tail_lines(output, manifest):
    runs = []
    for run in manifest["runs"]:
        directory = output / run["directory"]
        metric_path, sample_path = directory / "metrics.json", directory / "samples.jsonl.gz"
        if not metric_path.exists() or not sample_path.exists():
            continue
        metric = json.loads(metric_path.read_text()).get("update/128")
        if metric is None:
            continue
        with gzip.open(sample_path, "rt") as source:
            samples = [row for line in source if (row := json.loads(line))["case"] == "update/128"
                       and "error" not in row]
        runs.append((run, metric, samples))
    if not runs:
        return []
    lines = ["", "## Batch-update tail diagnostics", "",
             "These are per-run percentiles and five exact slow requests from each run.",
             "No percentile from one measure is subtracted from another measure.",
             "Every successful write retains its reported resident stages; null means unreported.",
             "The apply request phase is residual time, not a direct memory or disk measurement.",
             "Process observations bracket the RPC outside its response-time timer.",
             "The observer interval also includes counter reads and response decode; it differs from RPC time.",
             "Process CPU and I/O counters include every daemon thread and concurrent maintenance.",
             "Host CPU and pressure counters cover the whole CI machine, including other processes.",
             "Thread runqueue totals can overlap across CPUs. Do not derive wait as wall time minus CPU time.",
             "Missing counters have explicit reason codes. Their absence is not a measured zero.", "",
             "| Run | Calls | p50 ms | p95 ms | p99 ms | Exact phase matches |", "|---|---:|---:|---:|---:|---:|"]
    for run, metric, samples in runs:
        coverage = metric.get("phase_coverage", {})
        lines.append(f"| {run['directory']} | {len(samples)} | {metric['p50_ms']:.3f} | "
                     f"{metric['p95_ms']:.3f} | {metric['p99_ms']:.3f} | "
                     f"{coverage.get('matched', 'unreported')}/{len(samples)} |")
    for run, _, samples in runs:
        lines.extend(["", f"### {run['directory']}", "",
                      "Five slowest calls, with their own phases, stages, and counter deltas:", "", "```jsonl"])
        lines.extend(json.dumps(row, separators=(",", ":"), sort_keys=True) for row in tail_calls(samples))
        lines.append("```")
    return lines


def capture_phases(node, output, case, sequence, samples):
    operations = node.call("/api/status?recent=1")[0].get("status", {}).get("request_ops", {})
    requested = {item["request_id"] for sample in samples
                 for item in sample.get("physical_requests", [{"request_id": sample["request_id"]}])}
    matched = {}
    for operation in operations.get("recent", []):
        request_id = operation.get("request_id")
        if request_id not in requested or operation.get("client") != "github-benchmark":
            continue
        if request_id in matched:
            raise RuntimeError("The request ring repeats a measured physical request ID")
        matched[request_id] = operation
    complete = 0
    for sample in samples:
        physical = sample.get("physical_requests", [{"request_id": sample["request_id"]}])
        ids = [item["request_id"] for item in physical]
        sample["server_physical_ops"] = [matched[key] for key in ids if key in matched]
        sample["response_request_id"] = ids[-1]
        if ids[-1] in matched:
            sample["server_request_ops"] = matched[ids[-1]]
        complete += all(key in matched for key in ids)
    coverage = {"requests": len(samples), "matched": complete,
                "physical_requests": len(requested), "physical_matched": len(matched),
                "with_phases": sum(bool(row.get("phases")) for row in matched.values()),
                "ring_capacity": operations.get("ring_capacity"), "complete": complete == len(samples),
                "scope": "Every physical RPC is retained; server_request_ops is the final response RPC. Missing phases are unreported."}
    destination = output / "request-phases"
    destination.mkdir(exist_ok=True)
    save(destination / f"{sequence:02d}.json", {"case": case, "coverage": coverage, "request_ops": operations})
    return coverage


def phase_lines(output, manifest):
    lines = ["", "## Slow four-client writes and request phases", "",
             "These are actual measured calls joined to server records by request ID.",
             "Phase capture runs after each timed case. It is excluded from latency and throughput.",
             "The request ring is finite. Missing coverage and omitted phases mean unreported, not zero.",
             "Socket queue wait precedes server duration; it is not part of that duration.",
             "Phase totals can overlap; do not add a parent phase to its children.", "",
             "| Run | Matching requests | Slow logical write ms | Server ms | Largest reported phases ms |",
             "|---|---:|---:|---:|---|"]
    for run in manifest["runs"]:
        path = output / run["directory"] / "samples.jsonl.gz"
        if not path.exists():
            continue
        with gzip.open(path, "rt") as source:
            writes = [s for line in source if (s := json.loads(line))["case"] == "concurrent-write/4" and "error" not in s]
        if not writes:
            continue
        matched = sum("server_request_ops" in sample for sample in writes)
        for sample in sorted(writes, key=lambda row: row["elapsed_ms"], reverse=True)[:4]:
            operation = sample.get("server_request_ops", {})
            phases = sorted(operation.get("phases", {}).items(), key=lambda item: item[1], reverse=True)[:5]
            detail = ", ".join(f"{name}={value / 1000:.3f}" for name, value in phases) or "unreported"
            lines.append(f"| {run['directory']} | {matched}/{len(writes)} | "
                         f"{sample['elapsed_ms']:.3f} | {operation.get('duration_ms', 'unreported')} | {detail} |")
    return lines + update_tail_lines(output, manifest)


def _averages(events, field):
    names = sorted({name for event in events for name in event.get(field, {})})
    return {name: {"reported": sum(name in event.get(field, {}) for event in events),
                   "total": sum(event[field][name] for event in events if name in event.get(field, {})),
                   "mean": mean(event[field][name] for event in events if name in event.get(field, {}))}
            for name in names}


def _slow_requests(rows, scopes, commits):
    selected = []
    for row in sorted(rows, key=lambda item: item["elapsed_ms"], reverse=True)[:5]:
        before, after = row["process_before"]["unix_us"], row["process_after"]["unix_us"]
        matching = [s for s in scopes if before <= s["started_us"] <= s["ended_us"] <= after]
        if len(matching) != 1:
            raise ValueError("An observed update lacks one enclosed native request scope")
        scope = matching[0]
        physical = [c for c in commits if c["first_sequence"] <= scope["sequence"] <= c["sequence"]
                    and scope["started_us"] <= c["started_us"] <= c["ended_us"] <= scope["ended_us"]]
        if len(physical) != 1:
            raise ValueError("An observed update lacks one enclosed physical commit")
        selected.append({"request_id": row["request_id"], "elapsed_ms": row["elapsed_ms"],
                         "resident_stages_us": row.get("resident_stages_us"),
                         "server_request_ops": row.get("server_request_ops"),
                         "process_delta": row["process_delta"], "scope": scope, "physical_commit": physical[0]})
    return selected


def _correlate(case, rows, metric, events):
    first, last = metric["case_started_unix_us"], metric["case_ended_unix_us"]
    scopes = [e for e in events if e["kind"] == "scope" and not e["background"]
              and first <= e["started_us"] <= e["ended_us"] <= last]
    expected = sum(row["kind"] == "write" for row in rows)
    if len(scopes) != expected or len({e["sequence"] for e in scopes}) != expected:
        raise ValueError(f"{case}: native scope count differs from actual successful write calls")
    commits = [e for e in events if e["kind"] == "commit" and e["ok"]
               and first <= e["started_us"] <= e["ended_us"] <= last]
    used = {}
    for scope in scopes:
        matches = [e for e in commits if e["first_sequence"] <= scope["sequence"] <= e["sequence"]
                   and scope["started_us"] <= e["started_us"] <= e["ended_us"] <= scope["ended_us"]]
        if len(matches) != 1:
            raise ValueError(f"{case}: a native scope lacks one unambiguous physical commit")
        used[(matches[0]["started_us"], matches[0]["sequence"])] = matches[0]
    physical = list(used.values())
    if not physical or sum(e["requests"] for e in physical) < expected:
        raise ValueError(f"{case}: physical commit coverage is incomplete")
    return {"write_calls": expected, "scopes": len(scopes), "distinct_physical_commits": len(physical),
            "physical_request_slots": sum(e["requests"] for e in physical),
            "physical_request_counts": dict(Counter(e["requests"] for e in physical)),
            "scope_mean_us": mean(e["total_us"] for e in scopes), "scope_phases_us": _averages(scopes, "phases_us"),
            "physical_mean_us": mean(e["total_us"] for e in physical),
            "physical_phases_us": _averages(physical, "phases_us"), "physical_counts": _averages(physical, "counts"),
            "slowest_scopes": sorted(scopes, key=lambda e: e["total_us"], reverse=True)[:4],
            "slowest_physical_commits": sorted(physical, key=lambda e: e["total_us"], reverse=True)[:4],
            "slowest_requests": _slow_requests(rows, scopes, physical) if case in ("update/1", "update/128") else []}


def collect_native_phases(output):
    marker = "LASTDB_SNAPSHOT_TIMING "
    events = [json.loads(line.split(marker, 1)[1])
              for line in (output / "node.log").read_text().splitlines() if marker in line]
    if not events:
        raise ValueError("The diagnostic daemon emitted no native timing records")
    metrics = json.loads((output / "metrics.json").read_text())
    with gzip.open(output / "samples.jsonl.gz", "rt") as source:
        samples = [json.loads(line) for line in source]
    if any("error" in row for row in samples):
        raise ValueError("Native diagnostics contain rejected calls")
    cases = {}
    for case, metric in metrics.items():
        rows = [r for r in samples if r["case"] == case]
        if any(r["kind"] == "write" for r in rows):
            cases[case] = _correlate(case, rows, metric, events)
    if not cases:
        raise ValueError("The diagnostic capture contains no measured writes")
    return {"complete": True, "cases": cases,
            "scope": "Separate diagnostic run; phase totals can overlap; physical commits count once",
            "association": "Sequence interval plus enclosed time interval; no per-call physical cost claim",
            "physical_request_slots_include_background": True}
