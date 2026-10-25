"""Separate TTL diagnostics with complete, exact request-phase coverage."""

import gzip
import json
import os
from pathlib import Path
import statistics

from node import save
from workload import describe


def phase_summary(directory):
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
    save(directory / "phase-summary.json", summary)
    return summary


def profile(args, measure):
    root = args.output / "diagnostics"
    root.mkdir()
    manifest = {"complete": False, "runs": [], "calls": 512, "wave_calls": 64,
        "limit": "Separate diagnostic windows; status capture gaps affect maintenance exposure. "
                 "Use the uninterrupted cases for latency/throughput comparisons. "
                 "Phase totals can overlap. Missing fields are unreported, never zero."}
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
                summary = phase_summary(root / name)
                stages = sorted(summary["resident_stages_us"].items(),
                                key=lambda item: item[1]["p95_ms"] or 0, reverse=True)[:5]
                detail = ", ".join(f"{name}={value['p95_ms']:.3f}" for name, value in stages)
                lines.append(f"| {label} | {state} | {clients} | "
                             f"{result['phase_coverage']['matched']}/512 | {detail} |")
                manifest["runs"].append({"source": label, "state": state, "clients": clients,
                                          "directory": name, "result": result, "phases": summary})
                save(root / "profile.json", manifest)
    manifest["complete"] = True
    save(root / "profile.json", manifest)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
        stream.write("\n".join(lines) + "\n")
    return manifest
