#!/usr/bin/env python3
"""Separate global-gate delay from preparation and durable batch completion."""

import argparse
from bisect import bisect_right
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "native-log-improved-20261021"))
import analyze as previous

previous.STAGES.update({"directory_prepare", "directory_join_wait", "directory_install",
    "maintenance_serial", "compaction_prepare", "compaction_catch_up", "compaction_join",
    "compaction_publish", "compaction_install", "compaction_reclaim", "orphan_cleanup"})


def client_overlap(samples):
    boundaries = []
    for row in samples:
        boundaries += [(row["start_unix_us"], 1, row["schema"]),
                       (row["end_unix_us"], -1, row["schema"])]
    active, maximum, collisions = {}, 0, 0
    for _, change, schema in sorted(boundaries):
        if change == 1 and active.get(schema, 0):
            collisions += 1
        active[schema] = active.get(schema, 0)+change
        maximum = max(maximum, sum(active.values()))
    return {"maximum_simultaneous_client_calls": maximum,
            "same_schema_client_overlap": collisions}


def native_rows(events, first, last):
    measured = [e for e in events if first <= e["start_unix_us"] <= e["end_unix_us"] <= last]
    gates = sorted([e for e in measured if e["stage"] == "gate_wait"], key=lambda e: e["end_unix_us"])
    gate_ends = [e["end_unix_us"] for e in gates]
    scopes = {e["sequence"]: e for e in measured if e["stage"] == "scope_complete"}
    rows, used = [], set()
    components = {}
    for event in measured:
        if event["stage"] in ("write_mutex_wait", "sealed_prefix_wait"):
            components.setdefault(event["sequence"], []).append(event)
    for event in measured:
        if event["stage"] != "journal_accept":
            continue
        position = bisect_right(gate_ends, event["start_unix_us"])-1
        if position < 0:
            continue  # A background call may start before the measured interval.
        gate = gates[position]
        scope = scopes.get(event["sequence"])
        if scope is None:
            continue  # A background call may end after the measured interval.
        if (position in used or gate["background"] != event["background"]
                or not scope["start_unix_us"] <= gate["start_unix_us"] <= gate["end_unix_us"]):
            raise RuntimeError("exclusive gate/append correlation is ambiguous")
        used.add(position)
        detail = components.get(gate["sequence"], []) if gate["sequence"] else []
        row = {"sequence": event["sequence"], "admission_trace": gate["sequence"],
            "background": event["background"],
            "records": event["records"], "start_unix_us": scope["start_unix_us"],
            "end_unix_us": scope["end_unix_us"], "scope_ms": scope["elapsed_us"]/1000,
            "write_admission_wait_ms": gate["elapsed_us"]/1000,
            "preparation_under_gate_ms": (event["start_unix_us"]-gate["end_unix_us"])/1000,
            "journal_accept_ms": event["elapsed_us"]/1000,
            "global_gate_through_accept_ms": (event["end_unix_us"]-gate["end_unix_us"])/1000,
            "after_journal_ms": (scope["end_unix_us"]-event["end_unix_us"])/1000,
            "disk_sync_counter_deltas_us": {k:event[k] for k in ("full_sync_us", "ordered_sync_us", "file_set_sync_us")}}
        if detail:
            for component in ("write_mutex_wait", "sealed_prefix_wait"):
                row[component+"_ms"] = sum(e["elapsed_us"] for e in detail if e["stage"]==component)/1000
            if row["write_mutex_wait_ms"]+row["sealed_prefix_wait_ms"] > row["write_admission_wait_ms"]+1:
                raise RuntimeError("admission component intervals exceed their correlated parent")
        rows.append(row)
    return rows, measured


def summarize(rows):
    fields = ("scope_ms", "write_admission_wait_ms", "preparation_under_gate_ms", "journal_accept_ms",
              "global_gate_through_accept_ms", "after_journal_ms", "write_mutex_wait_ms", "sealed_prefix_wait_ms")
    return {field: previous.distribution(row[field] for row in rows if field in row) for field in fields}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    writes = json.loads((args.run/"writes.json").read_text())
    samples = writes["samples"]
    first = min(row["start_unix_us"] for row in samples)
    last = max(row["end_unix_us"] for row in samples)
    events = previous.read_events(args.log)
    rows, measured = native_rows(events, first, last)
    foreground = [row for row in rows if not row["background"]]
    background = [row for row in rows if row["background"]]
    if len(foreground) != len(samples):
        raise RuntimeError(f"foreground native calls {len(foreground)} differ from client calls {len(samples)}")
    overlap = client_overlap(samples)
    if overlap["same_schema_client_overlap"]:
        raise RuntimeError("the workload overlapped calls on the same schema")
    schemas = {row["schema"] for row in samples}
    for names in writes["assignments"].values():
        if not set(names).issubset(schemas):
            raise RuntimeError("an assigned schema received no measured batch")
    summary = {"client": writes["summary"], "schemas_used": len(schemas), **overlap,
        "foreground_native_calls": len(foreground), "background_native_calls": len(background),
        "foreground": summarize(foreground), "background": summarize(background),
        "worst_native_scopes": sorted(foreground, key=lambda row:row["scope_ms"], reverse=True)[:12],
        "client_by_operation": {name: previous.distribution(row["latency_ms"] for row in samples if row["mutation_type"]==name)
                                for name in ("create", "update")},
        "phases": {stage: previous.distribution(e["elapsed_us"]/1000 for e in measured if e["stage"]==stage)
                   for stage in ("directory_prepare", "directory_publish", "directory_install", "queue_delivery",
                                 "compaction_prepare", "compaction_join", "compaction_install", "compaction_reclaim")},
        "receipt_gate_wait_limit": "resident_stages_us.gate_wait is explicitly zeroed by the product. Use numeric native gate events.",
        "admission_limit": "write_admission_wait_ms covers mutex acquisition plus waits for sealed prefix completion. Separate components require the new numeric admission correlation id.",
        "correlation": "The exclusive global gate pairs each append with the last preceding request gate acquisition. Sequence links that append to its native scope. Client P99 uses all raw client calls; concurrent HTTP calls are not individually paired with native scopes.",
        "counter_limit": "Disk sync counters are process-wide, so overlapping phase deltas are not exclusive costs."}
    args.output.mkdir(exist_ok=False)
    previous.save_rows(args.output/"events.json", events)
    previous.save_rows(args.output/"native-scopes.json", rows)
    (args.output/"summary.json").write_text(json.dumps(summary,indent=2)+"\n")
    print(json.dumps({k:summary[k] for k in ("client", "schemas_used", "maximum_simultaneous_client_calls",
                                         "same_schema_client_overlap", "foreground_native_calls", "foreground")}))


if __name__ == "__main__":
    main()
