"""Audit complete native populations and locate physical disk-flush tails."""

from bisect import bisect_left
import json
import math
from statistics import mean, median

FLUSHES = ("v2_current_directory_flush_us", "v2_current_file_flush_us",
           "v2_current_rename_us", "v2_run_directory_flush_us", "v2_run_file_flush_us")


def stats(values):
    ordered = sorted(values)
    if not ordered:
        return None
    return {"count": len(ordered), "mean_us": mean(ordered), "p50_us": median(ordered),
            "p95_us": ordered[math.ceil(len(ordered)*.95)-1],
            "p99_us": ordered[math.ceil(len(ordered)*.99)-1], "max_us": ordered[-1]}


def summarize(directory, case):
    path = directory / "node.log"
    if path.stat().st_size > 256 * 2**20:
        raise ValueError("Native log exceeds its 256 MiB bound")
    first, last = case["case_started_unix_us"], case["case_ended_unix_us"]
    events = []
    with path.open() as stream:
        for line in stream:
            for marker in ("LASTDB_SNAPSHOT_TIMING ", "LASTDB_WRITE_REQUEST_TIMING "):
                if marker in line:
                    event = json.loads(line.split(marker, 1)[1])
                    if first <= event["started_us"] <= event["ended_us"] <= last:
                        events.append(event)
    requests = [e for e in events if e["kind"] == "request"]
    scopes = [e for e in events if e["kind"] == "scope" and not e["background"]]
    sequences = sorted(e["sequence"] for e in scopes)
    commits = []
    for event in events:
        if event["kind"] != "commit" or not event["ok"]:
            continue
        at = bisect_left(sequences, event["first_sequence"])
        if at < len(sequences) and sequences[at] <= event["sequence"]:
            commits.append(event)
    if (len(requests) != 2048 or any(e["status"] != 200 for e in requests)
            or len(sequences) < 2048 or len(set(sequences)) != len(sequences)):
        raise ValueError("Native HTTP or foreground scope coverage is incomplete")
    if any(sum(c["first_sequence"] <= seq <= c["sequence"] for c in commits) != 1 for seq in sequences):
        raise ValueError("A foreground scope lacks one physical commit")
    if any(not e["ok"] for e in events if e["kind"] in ("commit", "completion", "merge")):
        raise ValueError("Native storage reports a failed physical operation")
    names = {name for e in commits for name in e["phases_us"]}
    phases = {name: stats(e["phases_us"][name] for e in commits if name in e["phases_us"])
              for name in sorted(names)}
    tails = []
    for commit in sorted(commits, key=lambda c: c["total_us"], reverse=True)[:12]:
        members = [s for s in scopes if commit["first_sequence"] <= s["sequence"] <= commit["sequence"]]
        tails.append({"sequence": commit["sequence"], "requests": commit["requests"],
                      "started_us": commit["started_us"], "ended_us": commit["ended_us"],
                      "total_us": commit["total_us"],
                      "flush_us": {name: commit["phases_us"].get(name) for name in FLUSHES},
                      "member_durable_wait_us": stats(s["phases_us"]["durable_wait_us"] for s in members)})
    return {"http_requests": len(requests), "scopes": len(scopes), "physical_commits": len(commits),
            "commit_us": stats(c["total_us"] for c in commits), "commit_phases": phases,
            "slow_commit_counts": {str(limit): sum(c["total_us"] > limit for c in commits)
                                   for limit in (10_000, 50_000, 100_000)},
            "server_service_plus_queue_us": stats(e["total_us"]+e["queue_wait_us"] for e in requests),
            "largest_commits": tails,
            "scope": "Each commit is counted once. Scope sequences identify commit membership, not HTTP IDs. "
                     "Nested phases and concurrent requests overlap; do not add their percentiles. "
                     "Host pressure and flush timing are observations, not proof of an external cause."}
