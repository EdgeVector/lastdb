#!/usr/bin/env python3
"""Attribute real client delays to numeric native-daemon timing events."""

import argparse
from collections import Counter
import json
import math
from pathlib import Path
import re
import statistics

STAGES = {"part_room", "gate_wait", "write_mutex_wait", "sealed_prefix_wait", "journal_accept", "directory_publish",
          "queue_commit", "queue_delivery", "scope_complete"}
NUMBERS = ("sequence", "records", "start_unix_us", "end_unix_us", "elapsed_us",
           "full_sync_us", "ordered_sync_us", "file_set_sync_us")


def read_events(path):
    if path.stat().st_size > 32 * 1024 * 1024:
        raise RuntimeError("the explicit numeric diagnostic log exceeds 32 MiB")
    events = []
    for line in path.read_text().splitlines():
        line = re.sub(r"\x1b\[[0-9;]*m", "", line)
        if "fold_db::native_latency" not in line or "native write latency" not in line:
            continue
        fields = {name: quoted if quoted else raw for name, quoted, raw in
                  re.findall(r'(\w+)=(?:"([^"]*)"|(\S+))', line)}
        if fields.get("stage") not in STAGES or fields.get("background") not in ("true", "false"):
            raise RuntimeError("a numeric diagnostic event has an invalid stage or flag")
        event = {name: int(fields[name]) for name in NUMBERS}
        event.update(stage=fields["stage"], background=fields["background"] == "true")
        events.append(event)
    if not events:
        raise RuntimeError("the explicit log contains no numeric diagnostic events")
    return events


def contained(events, stage, first, last, background=False):
    return [event for event in events if event["stage"] == stage
            and event["background"] == background
            and first <= event["start_unix_us"] <= event["end_unix_us"] <= last]


def one(events, description):
    if len(events) != 1:
        raise RuntimeError(f"{description}: expected one event, found {len(events)}")
    return events[0]


def overlap_us(first, second):
    return max(0, min(first["end_unix_us"], second["end_unix_us"])
               - max(first["start_unix_us"], second["start_unix_us"]))


def attribute(samples, events):
    rows = []
    publishes = [event for event in events if event["stage"] == "directory_publish"]
    deliveries = [event for event in events if event["stage"] == "queue_delivery"]
    accepts = {event["sequence"]: event for event in events
               if event["stage"] == "journal_accept"}
    for sample in samples:
        first, last = sample["start_unix_us"], sample["end_unix_us"]
        scope = one(contained(events, "scope_complete", first, last),
                    f"request {sample['number']} scope")
        gate = one(contained(events, "gate_wait", scope["start_unix_us"], scope["end_unix_us"]),
                   f"request {sample['number']} gate")
        room = one(contained(events, "part_room", scope["start_unix_us"], scope["end_unix_us"]),
                   f"request {sample['number']} room")
        accept = accepts[scope["sequence"]]
        if accept["background"] or not first <= accept["start_unix_us"] <= accept["end_unix_us"] <= last:
            raise RuntimeError("the accepted sequence differs from its foreground request")
        row = {"number": sample["number"], "sequence": scope["sequence"],
               "start_unix_us": first, "end_unix_us": last,
               "client_ms": sample["latency_ms"], "scope_ms": scope["elapsed_us"] / 1000,
               "part_room_ms": room["elapsed_us"] / 1000,
               "gate_wait_ms": gate["elapsed_us"] / 1000,
               "journal_accept_ms": accept["elapsed_us"] / 1000,
               "journal_full_sync_ms": accept["full_sync_us"] / 1000,
               "journal_ordered_sync_ms": accept["ordered_sync_us"] / 1000,
               "journal_file_set_sync_ms": accept["file_set_sync_us"] / 1000,
               "directory_overlap_ms": sum(overlap_us(gate, event) for event in publishes) / 1000,
               "delivery_overlap_ms": sum(overlap_us(gate, event) for event in deliveries) / 1000}
        row["other_scope_ms"] = (scope["elapsed_us"] - gate["elapsed_us"]
                                  - room["elapsed_us"] - accept["elapsed_us"]) / 1000
        row["post_journal_ms"] = (scope["end_unix_us"] - accept["end_unix_us"]) / 1000
        row["pre_journal_ms"] = row["other_scope_ms"] - row["post_journal_ms"]
        row["directory_after_journal_ms"] = sum(
            max(0, min(scope["end_unix_us"], event["end_unix_us"])
                - max(accept["end_unix_us"], event["start_unix_us"]))
            for event in publishes) / 1000
        row["client_outside_scope_ms"] = row["client_ms"] - row["scope_ms"]
        row["largest_stage"] = max(("part_room_ms", "gate_wait_ms", "journal_accept_ms", "pre_journal_ms", "post_journal_ms", "client_outside_scope_ms"),
                                   key=row.get)
        rows.append(row)
    return rows


def distribution(values):
    values = sorted(values)
    if not values:
        return None
    return {"count": len(values), "median": statistics.median(values),
            "mean": statistics.mean(values), "p99": values[math.ceil(len(values) * .99) - 1],
            "maximum": values[-1]}


def save_rows(path, values):
    body = ",\n".join("  " + json.dumps(value, separators=(",", ":")) for value in values)
    path.write_text("[\n" + body + "\n]\n")


def summarize(rows, events):
    slow = [row for row in rows if row["client_ms"] > 80]
    fields = [name for name in rows[0] if name.endswith("_ms")]
    measured = [event for event in events if event["end_unix_us"] >= rows[0]["start_unix_us"]
                and event["start_unix_us"] <= rows[-1]["end_unix_us"]]
    publish = [event for event in measured if event["stage"] == "directory_publish"]
    delivery = [event for event in measured if event["stage"] == "queue_delivery"]
    return {"requests": len(rows), "events": len(events), "over_80_ms": len(slow),
            "slow_largest_stage": dict(Counter(row["largest_stage"] for row in slow)),
            "slow_with_directory_overlap_over_50_ms": sum(row["directory_overlap_ms"] > 50 for row in slow),
            "all": {field: distribution(row[field] for row in rows) for field in fields},
            "slow": {field: distribution(row[field] for row in slow) for field in fields},
            "directory_publication_ms": distribution(event["elapsed_us"] / 1000 for event in publish),
            "directory_full_sync_ms": distribution(event["full_sync_us"] / 1000 for event in publish),
            "directory_ordered_sync_ms": distribution(event["ordered_sync_us"] / 1000 for event in publish),
            "queue_delivery_ms": distribution(event["elapsed_us"] / 1000 for event in delivery),
            "worst_12": sorted(rows, key=lambda row: row["client_ms"], reverse=True)[:12]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--writes", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    events = read_events(args.log)
    samples = json.loads(args.writes.read_text())["samples"]
    rows = attribute(samples, events)
    summary = summarize(rows, events)
    args.output.mkdir(exist_ok=False)
    save_rows(args.output / "events.json", events)
    save_rows(args.output / "requests.json", rows)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({name: summary[name] for name in ("requests", "over_80_ms", "slow_largest_stage",
                                                      "slow_with_directory_overlap_over_50_ms")}))


if __name__ == "__main__":
    main()
