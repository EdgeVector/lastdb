"""Show exact slow batch-update calls, their stages, and bounded OS observations."""

import gzip
import json


def tail_calls(samples):
    rows = []
    for sample in sorted(samples, key=lambda row: row["elapsed_ms"], reverse=True)[:5]:
        operation = sample.get("server_request_ops", {})
        before, after = sample.get("process_before", {}), sample.get("process_after", {})
        rows.append({
            "request_id": sample["request_id"], "rpc_ms": sample["elapsed_ms"],
            "server_ms": operation.get("duration_ms"),
            "server_completed_unix_ms": operation.get("ts_ms"),
            "request_phases_us": operation.get("phases"),
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
             "These are per-run percentiles and five exact slow requests from each run.",
             "No percentile from one measure is subtracted from another measure.",
             "Every successful write retains its reported resident stages; null means unreported.",
             "The apply request phase is residual time, not a direct memory or disk measurement.",
             "Process observations bracket the RPC outside its response-time timer.",
             "The observer interval also includes counter reads and response decode; it differs from RPC time.",
             "Process CPU and I/O counters include every daemon thread and concurrent maintenance.",
             "Host CPU and pressure counters cover the whole CI machine, including other processes.",
             "Thread runqueue totals can overlap across CPUs. Do not derive wait as wall time minus CPU time.",
             "Missing counters have explicit reason codes. Their absence is not a measured zero.",
             "Native status observations occur outside the case and cannot locate maintenance within one call.", "",
             "| Run | Calls | p50 ms | p95 ms | p99 ms | Exact phase matches |",
             "|---|---:|---:|---:|---:|---:|"]
    for run, metric, samples in runs:
        coverage = metric.get("phase_coverage", {})
        lines.append(f"| {run['directory']} | {len(samples)} | {metric['p50_ms']:.3f} | "
                     f"{metric['p95_ms']:.3f} | {metric['p99_ms']:.3f} | "
                     f"{coverage.get('matched', 'unreported')}/{len(samples)} |")
    for run, metric, samples in runs:
        lines.extend(["", f"### {run['directory']}", "",
                      "Cumulative native status before and after the case:", "", "```json",
                      json.dumps({"before": metric.get("native_before"),
                                  "after": metric.get("native_after")}, separators=(",", ":"), sort_keys=True),
                      "```", "", "Five slowest calls, with their own phases, stages, and counter deltas:",
                      "", "```jsonl"])
        lines.extend(json.dumps(row, separators=(",", ":"), sort_keys=True) for row in tail_calls(samples))
        lines.append("```")
    return lines
