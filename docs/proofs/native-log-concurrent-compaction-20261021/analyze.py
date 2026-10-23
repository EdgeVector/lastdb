#!/usr/bin/env python3
"""Measure foreground progress during real compaction, publication, and deletion."""

import argparse
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "native-log-improved-20261021"))
import analyze as previous

STAGES = {"directory_prepare", "directory_join_wait", "directory_install", "maintenance_serial",
          "compaction_prepare", "compaction_catch_up", "compaction_join", "compaction_publish", "compaction_install",
          "compaction_reclaim", "orphan_cleanup"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--writes", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    previous.STAGES.update(STAGES)
    events = previous.read_events(args.log)
    samples = json.loads(args.writes.read_text())["samples"]
    rows = previous.attribute(samples, events)
    measured = [event for event in events if event["end_unix_us"] >= rows[0]["start_unix_us"]
                and event["start_unix_us"] <= rows[-1]["end_unix_us"]]
    summary = previous.summarize(rows, events)
    for stage in STAGES:
        phase = [event for event in measured if event["stage"] == stage]
        summary[stage + "_ms"] = previous.distribution(event["elapsed_us"] / 1000 for event in phase)
        summary[stage + "_client_acks_inside"] = sum(
            any(event["start_unix_us"] <= row["start_unix_us"] <= row["end_unix_us"] <= event["end_unix_us"]
                for event in phase) for row in rows)
        summary[stage + "_foreground_admissions_inside"] = sum(
            any(event["start_unix_us"] <= gate["end_unix_us"] <= event["end_unix_us"] for event in phase)
            for gate in measured if gate["stage"] == "gate_wait" and not gate["background"])
    copies = [event for event in measured if event["stage"] == "compaction_prepare"]
    normal = [event for event in measured if event["stage"] == "directory_publish"]
    summary["normal_publications_inside_compaction_copy"] = sum(
        any(copy["start_unix_us"] <= event["start_unix_us"] <= event["end_unix_us"] <= copy["end_unix_us"]
            for copy in copies) for event in normal)
    cleanup = [event for event in events if event["stage"] in ("compaction_reclaim", "orphan_cleanup")]
    summary["files_removed_by_concurrent_cleanup"] = sum(event["records"] for event in cleanup)
    summary["barrier_counter_limit"] = "Process-wide disk-sync deltas overlap concurrent phases; phase elapsed times and complete client intervals establish progress."
    args.output.mkdir(exist_ok=False)
    previous.save_rows(args.output / "events.json", events)
    previous.save_rows(args.output / "requests.json", rows)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({key:summary[key] for key in (
        "requests", "compaction_prepare_ms", "compaction_join_ms", "compaction_install_ms",
        "compaction_prepare_client_acks_inside", "normal_publications_inside_compaction_copy",
        "compaction_reclaim_client_acks_inside", "files_removed_by_concurrent_cleanup")}))


if __name__ == "__main__":
    main()
