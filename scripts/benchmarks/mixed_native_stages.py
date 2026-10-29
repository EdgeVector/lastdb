"""Bounded native-stage attribution for the separate mixed-write clock arms."""

from collections import Counter
import gzip
import json
import math
from statistics import mean

from mixed_layout_support import CASE
from native_phases import correlate
from node import save

MARKERS = ("LASTDB_SNAPSHOT_TIMING ", "LASTDB_WRITE_REQUEST_TIMING ")
LEAF_PHASES = ("v2_commit_lock_wait_us", "v2_publish_lock_wait_us",
               "v2_run_file_flush_us", "v2_run_directory_flush_us",
               "v2_current_file_flush_us", "v2_current_directory_flush_us",
               "v2_current_encoding_us", "v2_record_encoding_us")


def distribution(values):
    ordered = sorted(values)
    if not ordered:
        return {"reported": 0, "mean_us": None, "p95_us": None, "max_us": None}
    return {"reported": len(ordered), "mean_us": mean(ordered),
            "p95_us": ordered[math.ceil(.95 * len(ordered)) - 1], "max_us": ordered[-1]}


def phases(events):
    names = sorted({name for event in events for name in event.get("phases_us", {})})
    return {name: distribution([e["phases_us"][name] for e in events if name in e["phases_us"]])
            for name in names}


def read_events(output):
    path = output / "node.log"
    if path.stat().st_size > 256 * 2**20:
        raise ValueError("Native history log exceeds its 256 MiB bound")
    events = []
    with path.open("rb") as source:
        while line := source.readline(256 * 1024 + 1):
            if len(line) > 256 * 1024:
                raise ValueError("A native history log line exceeds its bound")
            for marker in MARKERS:
                if marker.encode() not in line:
                    continue
                event = json.loads(line.split(marker.encode(), 1)[1])
                # Read counters have an interval but deliberately no duration.
                fields = ("started_us", "ended_us")
                if isinstance(event, dict) and event.get("kind") != "write_reads":
                    fields += ("total_us",)
                if (not isinstance(event, dict) or len(events) >= 100_000
                        or any(type(event.get(k)) is not int or event[k] < 0
                               for k in fields)
                        or event["started_us"] > event["ended_us"]):
                    raise ValueError("Invalid or excessive native timing events")
                events.append(event)
    if not events:
        raise ValueError("The native arm emitted no timing events")
    return events


def overlaps(left, right):
    return left["started_us"] <= right["ended_us"] and right["started_us"] <= left["ended_us"]


def associate(scopes, commits, completions, merges):
    """Commit cost is counted once, regardless of its number of waiting scopes."""
    result = []
    for commit in commits:
        members = [s for s in scopes if commit["first_sequence"] <= s["sequence"] <= commit["sequence"]]
        if not members:
            continue  # Preserve unrelated/background commits in the raw event file.
        finished = [e for e in completions if e["sequence"] == commit["sequence"]]
        if len(finished) != 1 or not finished[0]["ok"] or finished[0]["requests"] != commit["requests"]:
            raise ValueError("A physical commit lacks one successful completion event")
        result.append({"commit": commit, "foreground_sequences": [s["sequence"] for s in members],
                       "completion": finished[0],
                       "overlapping_merges": [i for i, m in enumerate(merges) if overlaps(commit, m)]})
    return result


def collect(output):
    metric = json.loads((output / "metrics.json").read_text())[CASE]
    with gzip.open(output / "samples.jsonl.gz", "rt") as source:
        rows = [json.loads(line) for line in source]
    all_events = read_events(output)
    window = {"started_us": metric["case_started_unix_us"], "ended_us": metric["case_ended_unix_us"]}
    events = [e for e in all_events if overlaps(e, window)]
    enclosed = [e for e in events if window["started_us"] <= e["started_us"]
                and e["ended_us"] <= window["ended_us"]]
    requests = [e for e in enclosed if e["kind"] == "request"]
    expected = sum(r["kind"] == "write" for r in rows)
    if expected != 128 or len(requests) != expected or any(e["status"] != 200 for e in requests):
        raise ValueError("Native HTTP clocks do not cover all 128 successful target writes")
    # Reuse the existing sequence + enclosed interval proof for foreground scopes.
    coverage = correlate(CASE, rows, metric, all_events)
    scopes = [e for e in enclosed if e["kind"] == "scope" and e["background"] is False]
    commits = [e for e in enclosed if e["kind"] == "commit" and e["ok"]]
    completions = [e for e in events if e["kind"] == "completion"]
    merges = [e for e in events if e["kind"] == "merge" and e["work"]]
    if any(not e["ok"] for e in events if e["kind"] in ("commit", "completion", "merge")):
        raise ValueError("Native diagnostics include failed storage work")
    groups = associate(scopes, commits, completions, merges)
    physical = [g["commit"] for g in groups]
    required = ("v2_run_build_us", "v2_publish_lock_wait_us", "v2_current_file_flush_us")
    if (len(groups) != coverage["distinct_physical_commits"]
            or any(any(name not in e["phases_us"] for name in required) for e in physical)):
        raise ValueError("Physical stage attribution is incomplete")
    report = {"complete": True, "case": CASE, "window": window, "coverage": coverage,
              "history_events": len(all_events), "intersecting_events": len(events),
              "crossing_events": len(events) - len(enclosed),
              "request_total": distribution([e["total_us"] for e in requests]),
              "scope_phases": phases(scopes), "commit_phases": phases(physical),
              "commit_total": distribution([e["total_us"] for e in physical]),
              "completion_phases": phases([g["completion"] for g in groups]),
              "merge_kinds": dict(Counter(str(e["merge_kind"]) for e in merges)),
              "merge_phases": phases(merges), "merges": merges, "physical_groups": groups,
              "scope": "Native arms only; instrumentation affects timing and preceding history. "
                       "Foreground scopes match commits by sequence and enclosed time, not HTTP IDs. "
                       "HTTP IDs have separate server-ring coverage. Nested phases overlap; never add "
                       "parents and children, scope waits and commits, or concurrent merge costs. "
                       "Merge overlap is temporal correlation, not proof of blocking or causation."}
    save(output / "native-events.json", {"window": window, "events": events})
    save(output / "native-diagnostics.json", report)


def summary_lines(output, manifest):
    lines = ["", "## Separate native-stage arms", "",
             "Physical commits count once; clocks/logging alter pacing. Phase totals can overlap.",
             "Native sequences do not identify HTTP request IDs. Merge overlap is not causation.", "",
             "| Run | HTTP clocks / scopes / commits | Commit P95 ms | Durable wait P95 ms | "
             "Merge steps (tier/base) | Largest leaf-phase means ms |",
             "|---|---:|---:|---:|---:|---|"]
    for run in manifest["runs"]:
        if not run["mode"].endswith("-native"):
            continue
        report = json.loads((output / run["directory"] / "native-diagnostics.json").read_text())
        coverage = report["coverage"]
        leaf = [(k, v["mean_us"]) for k, v in report["commit_phases"].items() if k in LEAF_PHASES]
        detail = ", ".join(f"{k}={v / 1000:.3f}" for k, v in sorted(leaf, key=lambda p: p[1], reverse=True)[:4])
        wait = report["scope_phases"].get("durable_wait_us", {}).get("p95_us")
        if wait is None:
            raise ValueError("Native scope durable-wait timing is missing")
        lines.append(f"| {run['directory']} | {report['request_total']['reported']} / "
                     f"{coverage['scopes']} / {coverage['distinct_physical_commits']} | "
                     f"{report['commit_total']['p95_us'] / 1000:.3f} | {wait / 1000:.3f} | "
                     f"{report['merge_kinds'].get('1', 0)} / {report['merge_kinds'].get('2', 0)} | {detail} |")
    return lines
