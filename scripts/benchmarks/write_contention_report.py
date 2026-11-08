#!/usr/bin/env python3
"""Retain native wait and commit clocks for the write gate."""

from bisect import bisect_left
import json
from statistics import mean, median

from node import save


def summarize(events):
    names = {name for event in events for name in event.get("phases_us", {})}
    return {name: {"reported": sum(name in e.get("phases_us", {}) for e in events),
                   "mean_us": mean(e["phases_us"][name] for e in events if name in e.get("phases_us", {})),
                   "median_us": median(e["phases_us"][name] for e in events if name in e.get("phases_us", {}))}
            for name in sorted(names)}


def native_events(directory, cases):
    path = directory / "node.log"
    if path.stat().st_size > 256 * 2**20:
        raise ValueError("The investigation log exceeds its 256 MiB bound")
    events = []
    with path.open() as source:
        for line in source:
            for marker in ("LASTDB_SNAPSHOT_TIMING ", "LASTDB_WRITE_REQUEST_TIMING "):
                if marker in line:
                    events.append(json.loads(line.split(marker, 1)[1]))
    for case in cases:
        if not case["native_timing"]:
            continue
        first, last = case["case_started_unix_us"], case["case_ended_unix_us"]
        selected = [e for e in events if first <= e["started_us"] <= e["ended_us"] <= last]
        requests = [e for e in selected if e["kind"] == "request"]
        if len(requests) != case["requests"] or any(e["status"] != 200 for e in requests):
            raise ValueError("Native HTTP request clocks do not cover all successful measured calls")
        scopes = [e for e in selected if e["kind"] == "scope" and e["background"] is False]
        sequences = sorted(e["sequence"] for e in scopes)
        # Author-clock persistence can open additional foreground scopes. Do not
        # identify those metadata scopes as HTTP mutations or discard their cost.
        if len(scopes) < case["requests"] or len(set(sequences)) != len(sequences):
            raise ValueError("Observed foreground storage scopes are incomplete or duplicated")
        commits = []
        for event in selected:
            if event["kind"] != "commit" or not event["ok"]:
                continue
            position = bisect_left(sequences, event["first_sequence"])
            if position < len(sequences) and sequences[position] <= event["sequence"]:
                commits.append(event)
        if any(sum(c["first_sequence"] <= seq <= c["sequence"] for c in commits) != 1 for seq in sequences):
            raise ValueError("A write scope lacks one distinct durable publication")
        case["native"] = {"http_requests": len(requests), "request_phases": summarize(requests),
                          "socket_queue_mean_us": mean(e["queue_wait_us"] for e in requests),
                          "scopes": len(scopes), "extra_foreground_scopes": len(scopes)-len(requests),
                          "scope_population": "All observed foreground storage work, including author-clock metadata",
                          "commits": len(commits),
                          "requests_per_commit": mean(c["requests"] for c in commits),
                          "scope_phases": summarize(scopes), "commit_phases": summarize(commits),
                          "commit_mean_us": mean(c["total_us"] for c in commits),
                          "commit_slots_include_background": True,
                          "scope_phase_totals_overlap": False}
        save(directory / f"{case['name']}.json", case)
