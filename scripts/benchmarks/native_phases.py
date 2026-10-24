"""Correlate actual native request scopes and distinct physical commits."""

from collections import Counter
import gzip
import json
from statistics import mean


def averages(events, field):
    names = sorted({name for event in events for name in event.get(field, {})})
    # A missing field stays explicit: zero must not hide incomplete diagnostics.
    return {name: {"reported": sum(name in event.get(field, {}) for event in events),
                   "mean": mean(event[field][name] for event in events if name in event.get(field, {}))}
            for name in names}


def correlate(case, rows, metric, events):
    first, last = metric["case_started_unix_us"], metric["case_ended_unix_us"]
    scopes = [event for event in events if event["kind"] == "scope" and not event["background"]
              and first <= event["started_us"] <= event["ended_us"] <= last]
    expected = sum(row["kind"] == "write" for row in rows)
    if len(scopes) != expected or len({event["sequence"] for event in scopes}) != expected:
        raise ValueError(f"{case}: native scope count differs from actual successful write calls")
    commits = [event for event in events if event["kind"] == "commit" and event["ok"]
               and first <= event["started_us"] <= event["ended_us"] <= last]
    used = {}
    for scope in scopes:
        matches = [event for event in commits
                   if event["first_sequence"] <= scope["sequence"] <= event["sequence"]
                   and scope["started_us"] <= event["started_us"] <= event["ended_us"] <= scope["ended_us"]]
        if len(matches) != 1:
            raise ValueError(f"{case}: a native scope lacks one unambiguous physical commit")
        event = matches[0]
        used[(event["started_us"], event["sequence"])] = event
    physical = list(used.values())
    if not physical or sum(event["requests"] for event in physical) < expected:
        raise ValueError(f"{case}: physical commit coverage is incomplete")
    return {"write_calls": expected, "scopes": len(scopes), "distinct_physical_commits": len(physical),
            "physical_request_slots": sum(event["requests"] for event in physical),
            "physical_request_counts": dict(Counter(event["requests"] for event in physical)),
            "scope_mean_us": mean(event["total_us"] for event in scopes),
            "scope_phases_us": averages(scopes, "phases_us"),
            "physical_mean_us": mean(event["total_us"] for event in physical),
            "physical_phases_us": averages(physical, "phases_us"),
            "physical_counts": averages(physical, "counts"),
            "slowest_scopes": sorted(scopes, key=lambda event: event["total_us"], reverse=True)[:4],
            "slowest_physical_commits": sorted(physical, key=lambda event: event["total_us"], reverse=True)[:4]}


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
        rows = [row for row in samples if row["case"] == case]
        if any(row["kind"] == "write" for row in rows):
            cases[case] = correlate(case, rows, metric, events)
    if not cases:
        raise ValueError("The diagnostic capture contains no measured writes")
    return {"complete": True, "cases": cases,
            "scope": "Separate diagnostic run; phase totals can overlap; physical commits count once",
            "association": "Sequence interval plus enclosed time interval; no per-call physical cost claim",
            "physical_request_slots_include_background": True}
