"""Separate TTL diagnostics with complete, exact request-phase coverage."""

import gzip
import json
import os
from pathlib import Path
import statistics

from native_phases import correlate
from node import save
from workload import describe


def phase_summary(directory, require_native):
    with gzip.open(directory / "samples.jsonl.gz", "rt") as stream:
        rows = [json.loads(line) for line in stream]
    names = sorted({name for row in rows for name in row["server_request_ops"]["phases"]})
    stages = sorted({name for row in rows for name in row["resident_stages_us"]})
    summary = {}
    for section, fields in (("phases", names), ("resident_stages_us", stages)):
        summary[section] = {}
        for name in fields:
            values = [row["server_request_ops"]["phases"].get(name) if section == "phases"
                      else row[section].get(name) for row in rows]
            reported = [value for value in values if value is not None]
            summary[section][name] = {"reported": len(reported),
                "p50_ms": statistics.median(reported) / 1000 if reported else None,
                "p95_ms": describe([{"elapsed_ms": value / 1000, "records": 0} for value in reported], 1)["p95_ms"]
                if reported else None}
    summary["slowest_calls"] = sorted(rows, key=lambda row: row["elapsed_ms"], reverse=True)[:8]
    marker = "LASTDB_SNAPSHOT_TIMING "
    events = [json.loads(line.split(marker, 1)[1])
              for line in (directory / "node.log").read_text().splitlines() if marker in line]
    metric = json.loads((directory / "metrics.json").read_text())
    trace_ids = {row["server_request_ops"].get("native_trace_id") for row in rows}
    if trace_ids == {None} and not require_native:
        summary["native"] = {"available": False,
            "reason": "Selected baseline predates opaque native trace identifiers; "
                      "native events include maintenance and cannot be assigned to foreground calls."}
    else:
        if None in trace_ids or len(trace_ids) != 512:
            raise RuntimeError("TTL diagnostic lacks unique native traces for all successful calls")
        summary["native"] = {"available": True,
            **correlate("ttl-profile", rows, metric, events, trace_ids=trace_ids)}
        if summary["native"]["scopes"] != 512:
            raise RuntimeError("TTL diagnostic lacks one native scope per successful call")
    save(directory / "phase-summary.json", summary)
    return summary


def profile(args, measure):
    root = args.output / "diagnostics"
    root.mkdir()
    manifest = {"complete": False, "runs": [], "calls": 512, "wave_calls": 64,
        "limit": "Separate diagnostic windows; status capture gaps affect maintenance exposure. "
                 "Use the uninterrupted cases for latency/throughput comparisons. "
                 "Phase totals can overlap. Missing fields are unreported, never zero. "
                 "Native instrumentation is enabled only in diagnostics. Native request scopes join "
                 "distinct physical commits by sequence and enclosed time, with complete case coverage. "
                 "A shared physical commit is counted once; its cost is not assigned to every call. "
                 "Candidate native coverage is mandatory. A baseline that predates trace identifiers "
                 "retains complete RPC phases and raw native events, with native association explicitly unreported."}
    lines = ["", "## Active TTL request phases", "", manifest["limit"], "",
             "| Source | State | Clients | Exact phase matches | Largest reported stage p95 ms |",
             "|---|---|---:|---:|---|"]
    save(root / "profile.json", manifest)
    for label in ("baseline", "candidate"):
        for state in ("active-sweep", "active-sweep-compact"):
            for clients in (1, 4):
                name = f"{label}-{state}-{clients}"
                print(json.dumps({"diagnostic": name}), flush=True)
                result = measure(getattr(args, label), getattr(args, f"{label}_revision"),
                                 root / name, state, clients, False, diagnostic=True)
                summary = phase_summary(root / name, require_native=label == "candidate")
                stages = sorted(summary["resident_stages_us"].items(),
                                key=lambda item: item[1]["p95_ms"] or 0, reverse=True)[:5]
                detail = ", ".join(f"{name}={value['p95_ms']:.3f}" for name, value in stages)
                lines.append(f"| {label} | {state} | {clients} | "
                             f"{result['phase_coverage']['matched']}/512 | {detail} |")
                native = summary["native"]
                if native["available"]:
                    scope = sorted(native["scope_phases_us"].items(),
                                   key=lambda item: item[1]["mean"], reverse=True)[:5]
                    physical = sorted(native["physical_phases_us"].items(),
                                      key=lambda item: item[1]["mean"], reverse=True)[:5]
                    lines.extend(["", f"{label}/{state}/{clients}: {native['scopes']} native scopes, "
                        f"{native['distinct_physical_commits']} distinct physical commits. "
                        "Native scope means (ms): " + ", ".join(f"{name}={value['mean']/1000:.3f}" for name, value in scope)
                        + ". Physical commit means (ms): "
                        + ", ".join(f"{name}={value['mean']/1000:.3f}" for name, value in physical), ""])
                else:
                    lines.extend(["", f"{label}/{state}/{clients}: native association unreported. "
                                  + native["reason"], ""])
                manifest["runs"].append({"source": label, "state": state, "clients": clients,
                                          "directory": name, "result": result, "phases": summary})
                save(root / "profile.json", manifest)
    manifest["complete"] = True
    save(root / "profile.json", manifest)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
        stream.write("\n".join(lines) + "\n")
    return manifest
