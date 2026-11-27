"""Capture /api/ops write_phases deltas and batch-update tail diagnostics."""

from __future__ import annotations

import gzip
import json

from node import save

REPORTED_WRITE_CASES = (
    "insert/1", "insert/128", "update/1", "update/128",
    "concurrent-write/1", "concurrent-write/4", "concurrent-write/16",
    "concurrent-write/32", "concurrent-mixed/32",
)


def read_ops(node):
    try:
        return node.call("/api/ops")[0]
    except RuntimeError as error:
        if getattr(error, "status", None) == 404:
            return None
        raise


def phase_delta(before, after):
    if not isinstance(after, dict):
        return None
    after_wp = after.get("write_phases")
    if not isinstance(after_wp, dict):
        return None
    before_wp = before.get("write_phases") if isinstance(before, dict) else {}
    if not isinstance(before_wp, dict):
        before_wp = {}
    before_totals = {
        row["phase"]: row.get("total_ns", 0)
        for row in (before_wp.get("phases") or [])
        if isinstance(row, dict) and "phase" in row
    }
    writes = max(0, int(after_wp.get("writes", 0)) - int(before_wp.get("writes", 0)))
    phases = []
    for row in after_wp.get("phases") or []:
        name = row["phase"]
        total_ns = max(0, int(row.get("total_ns", 0)) - int(before_totals.get(name, 0)))
        phases.append({
            "phase": name,
            "total_ns": total_ns,
            "mean_ns": total_ns / writes if writes else 0.0,
            "daemon_slowest_ns": int(row.get("slowest_ns", 0)),
        })
    return {"writes": writes, "phases": phases}


def capture_phases(node, output, case, sequence, samples, before=None):
    after = read_ops(node)
    delta = phase_delta(before, after)
    expected = sum(s.get("kind") == "write" and "error" not in s for s in samples)
    observed = delta["writes"] if delta is not None else None
    complete = observed == expected if observed is not None else False
    coverage = {
        "requests": len(samples),
        "write_requests": expected,
        "matched": observed if observed is not None else 0,
        "observed_writes": observed,
        "with_phases": observed if observed else 0,
        "complete": complete,
        "write_phases": delta,
        "scope": "Per-case /api/ops write_phases delta captured outside timed windows; daemon_slowest_ns is cumulative since daemon start.",
    }
    destination = output / "request-phases"
    destination.mkdir(exist_ok=True)
    save(destination / f"{sequence:02d}.json", {"case": case, "coverage": coverage, "ops": after})
    return coverage


def tail_calls(samples):
    rows = []
    for sample in sorted(samples, key=lambda row: row["elapsed_ms"], reverse=True)[:5]:
        before, after = sample.get("process_before", {}), sample.get("process_after", {})
        rows.append({
            "request_id": sample["request_id"],
            "rpc_ms": sample["elapsed_ms"],
            "resident_stages_us": sample.get("resident_stages_us"),
            "observer_before_unix_us": before.get("unix_us"),
            "observer_after_unix_us": after.get("unix_us"),
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
             "These are per-run percentiles and five slow requests from each run.",
             "No percentile from one measure is subtracted from another measure.",
             "Process observations bracket the RPC outside its response-time timer.",
             "Process CPU and I/O counters include every daemon thread and concurrent maintenance.",
             "Missing counters have explicit reason codes. Their absence is not a measured zero.", "",
             "| Run | Calls | p50 ms | p95 ms | p99 ms | Committed /api/ops writes |", "|---|---:|---:|---:|---:|---:|"]
    for run, metric, samples in runs:
        coverage = metric.get("phase_coverage", {})
        lines.append(f"| {run['directory']} | {len(samples)} | {metric['p50_ms']:.3f} | "
                     f"{metric['p95_ms']:.3f} | {metric['p99_ms']:.3f} | "
                     f"{coverage.get('matched', 'unreported')}/{len(samples)} |")
    for run, _, samples in runs:
        lines.extend(["", f"### {run['directory']}", "",
                      "Five slowest calls, with their own stages and counter deltas:", "", "```jsonl"])
        lines.extend(json.dumps(row, separators=(",", ":"), sort_keys=True) for row in tail_calls(samples))
        lines.append("```")
    return lines


def phase_lines(output, manifest):
    lines = ["", "## Committed write phases (`/api/ops`)", "",
             "Phase capture snapshots `GET /api/ops` before and after each timed write case.",
             "It runs outside the latency, throughput, memory, and disk-size windows.",
             "Dotted sub-phases belong to the phase before the dot; do not add a parent phase to its children.", "",
             "| Run / case | Committed writes | Handler mean ms | Commit mean ms | Largest phase means ms |",
             "|---|---:|---:|---:|---|"]
    for run in manifest["runs"]:
        path = output / run["directory"] / "metrics.json"
        if not path.exists():
            continue
        metrics = json.loads(path.read_text())
        for case in REPORTED_WRITE_CASES:
            wp = ((metrics.get(case) or {}).get("phase_coverage") or {}).get("write_phases")
            if not wp or not wp.get("writes"):
                continue
            by_name = {item["phase"]: item["mean_ns"] / 1e6 for item in wp["phases"]}
            top = sorted(
                ((k, v) for k, v in by_name.items() if k != "handler" and "." not in k),
                key=lambda item: item[1],
                reverse=True,
            )[:4]
            detail = ", ".join(f"{name}={val:.3f}" for name, val in top) or "unreported"
            lines.append(
                f"| {run['directory']} / {case} | {wp['writes']} | "
                f"{by_name.get('handler', 0.0):.3f} | {by_name.get('commit', 0.0):.3f} | {detail} |"
            )
    return lines + update_tail_lines(output, manifest)


def collect_native_phases(output):
    metrics = json.loads((output / "metrics.json").read_text())
    with gzip.open(output / "samples.jsonl.gz", "rt") as source:
        samples = [json.loads(line) for line in source]
    if any("error" in row for row in samples):
        raise ValueError("Native diagnostics contain rejected calls")
    cases = {}
    for case, metric in metrics.items():
        rows = [r for r in samples if r["case"] == case]
        expected = sum(r["kind"] == "write" for r in rows)
        if not expected:
            continue
        wp = (metric.get("phase_coverage") or {}).get("write_phases")
        if not wp or wp["writes"] != expected:
            raise ValueError(f"{case}: /api/ops write_phases count differs from successful write calls")
        writes = wp["writes"]
        by_name = {item["phase"]: item for item in wp["phases"]}
        scope_phases = {
            name: {"reported": writes, "total": item["total_ns"] / 1000.0, "mean": item["mean_ns"] / 1000.0}
            for name, item in by_name.items()
            if name != "handler" and not name.startswith("commit.")
        }
        physical_phases = {
            name: {"reported": writes, "total": item["total_ns"] / 1000.0, "mean": item["mean_ns"] / 1000.0}
            for name, item in by_name.items()
            if name == "commit" or name.startswith("commit.")
        }
        slowest = [
            {
                "request_id": row["request_id"],
                "elapsed_ms": row["elapsed_ms"],
                "resident_stages_us": row.get("resident_stages_us"),
                "process_delta": row.get("process_delta"),
            }
            for row in sorted(rows, key=lambda item: item["elapsed_ms"], reverse=True)[:5]
        ] if case in ("update/1", "update/128") else []
        cases[case] = {
            "write_calls": expected,
            "scopes": writes,
            "distinct_physical_commits": writes,
            "scope_mean_us": by_name.get("handler", {}).get("mean_ns", 0.0) / 1000.0,
            "scope_phases_us": scope_phases,
            "physical_mean_us": by_name.get("commit", {}).get("mean_ns", 0.0) / 1000.0,
            "physical_phases_us": physical_phases,
            "slowest_requests": slowest,
        }
    if not cases:
        raise ValueError("The diagnostic capture contains no measured writes")
    return {
        "complete": True,
        "cases": cases,
        "scope": "Separate diagnostic run; /api/ops write_phases delta per case; dotted sub-phases overlap their parent phase",
    }
