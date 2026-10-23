#!/usr/bin/env python3
"""Attribute complete real client requests to exclusive native write phases."""

import importlib.util
import gzip
import json
from pathlib import Path
import statistics
import sys

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location(
    "snapshot_timing", ROOT.parent / "snapshot-write-timing-20261022/analyze.py")
timing = importlib.util.module_from_spec(spec)
spec.loader.exec_module(timing)
timing.ROOT = ROOT


def concurrent(name):
    output = ROOT / name
    writes = timing.load(output / "writes.json")
    samples = writes["samples"]
    first = min(s["start_unix_us"] for s in samples)
    last = max(s["end_unix_us"] for s in samples)
    marker = "LASTDB_SNAPSHOT_TIMING "
    events = [json.loads(line.split(marker, 1)[1]) for line in
              gzip.decompress((output / "daemon.txt.gz").read_bytes()).decode().splitlines() if marker in line]
    scopes = [e for e in events if e["kind"] == "scope"
              and first <= e["started_us"] <= e["ended_us"] <= last]
    commits = [e for e in events if e["kind"] == "commit"]
    background = [c for c in commits if any(s["background"] and s["sequence"] == c["sequence"]
                  and s["started_us"] <= c["started_us"] <= c["ended_us"] <= s["ended_us"] for s in scopes)]
    foreground = [s for s in scopes if not s["background"]]
    rows = []
    for scope in foreground:
        matched = [c for c in commits if c["sequence"] == scope["sequence"]
                   and scope["started_us"] <= c["started_us"] <= c["ended_us"] <= scope["ended_us"]]
        if len(matched) != 1 or not matched[0]["ok"] or matched[0]["requests"] != 1:
            raise RuntimeError("each complete native client scope needs one successful commit")
        commit = matched[0]
        admission = scope["started_us"] + scope["phases_us"]["part_room_us"]
        end = admission + scope["phases_us"]["admission_us"]
        rows.append({"sequence": scope["sequence"], "scope_ms": scope["total_us"] / 1000,
            "admission_ms": scope["phases_us"]["admission_us"] / 1000,
            "commit_ms": commit["total_us"] / 1000,
            "background_overlap_ms": sum(max(0, min(end, c["ended_us"]) - max(admission, c["started_us"]))
                                         for c in background) / 1000})
    if len(rows) != len(samples) or len({r["sequence"] for r in rows}) != len(rows):
        raise RuntimeError("complete native scope count differs from the client request count")
    result = {"whole_request": writes["summary"], "matched_native_scopes": len(rows),
        "background_commits": len(background),
        "native_scopes": {key: timing.stats([row[key] for row in rows]) for key in
                          ("scope_ms", "admission_ms", "commit_ms", "background_overlap_ms")},
        "client_other_average_ms": writes["summary"]["average_ms"] - statistics.mean(r["scope_ms"] for r in rows),
        "rows": rows}
    timing.save(output / "concurrent-summary.json", result)
    print(json.dumps({key: value for key, value in result.items() if key != "rows"}, indent=2))


def main():
    for name in sys.argv[1:]:
        if "concurrent" in name:
            concurrent(name)
            continue
        result = timing.analyze(name)
        print(json.dumps({"name": name, "whole_request": result["whole_request"],
            "phases_ms": {key: value["average"] for key, value in result["exclusive_phase_ms"].items()},
            "mean_counts": result["mean_counts"],
            "background_overlap": result["background_commit_overlap_with_admission_ms"],
            "actual_p99_request": result["actual_p99_request"],
            "parts_before": result["parts_before"], "parts_after": result["parts_after"]}, indent=2))


if __name__ == "__main__":
    main()
