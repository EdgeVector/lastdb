"""Correlate actual native request scopes and distinct physical commits."""

from collections import Counter
import gzip
import json
from statistics import mean


def averages(events, field):
    names = sorted({name for event in events for name in event.get(field, {})})
    # A missing field stays explicit: zero must not hide incomplete diagnostics.
    return {name: {"reported": sum(name in event.get(field, {}) for event in events),
                   "total": sum(event[field][name] for event in events if name in event.get(field, {})),
                   "mean": mean(event[field][name] for event in events if name in event.get(field, {}))}
            for name in names}


def slow_requests(rows, scopes, commits):
    selected = []
    for row in sorted(rows, key=lambda item: item["elapsed_ms"], reverse=True)[:5]:
        before, after = row["process_before"]["unix_us"], row["process_after"]["unix_us"]
        matching = [scope for scope in scopes
                    if before <= scope["started_us"] <= scope["ended_us"] <= after]
        if len(matching) != 1:
            raise ValueError("An observed update lacks one enclosed native request scope")
        scope = matching[0]
        physical = [commit for commit in commits
                    if commit["first_sequence"] <= scope["sequence"] <= commit["sequence"]
                    and scope["started_us"] <= commit["started_us"] <= commit["ended_us"] <= scope["ended_us"]]
        if len(physical) != 1:
            raise ValueError("An observed update lacks one enclosed physical commit")
        selected.append({"request_id": row["request_id"], "elapsed_ms": row["elapsed_ms"],
            "resident_stages_us": row.get("resident_stages_us"),
            "server_request_ops": row.get("server_request_ops"),
            "process_delta": row["process_delta"], "scope": scope, "physical_commit": physical[0]})
    return selected


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
            "slowest_physical_commits": sorted(physical, key=lambda event: event["total_us"], reverse=True)[:4],
            "slowest_requests": slow_requests(rows, scopes, physical) if case in ("update/1", "update/128") else []}


def native_events(output):
    # The daemon stops before capture. Read its retained rotations as well as
    # the current file; exact scope coverage still rejects lost or duplicate data.
    rotations = [path for path in output.glob("node.log.*") if path.suffix[1:].isdigit()]
    paths = sorted(rotations, key=lambda path: int(path.suffix[1:]), reverse=True)
    paths.append(output / "node.log")
    marker = "LASTDB_SNAPSHOT_TIMING "
    return [json.loads(line.split(marker, 1)[1])
            for path in paths for line in path.read_text().splitlines() if marker in line]


def collect_native_phases(output):
    events = native_events(output)
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
