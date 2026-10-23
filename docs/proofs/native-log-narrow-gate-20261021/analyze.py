#!/usr/bin/env python3
"""Attribute real write delay and gate admissions during durable CURRENT I/O."""

import argparse
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "native-log-improved-20261021"))
import analyze as previous


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--writes", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    previous.STAGES.update({"directory_prepare", "directory_join_wait", "directory_install"})
    events = previous.read_events(args.log)
    samples = json.loads(args.writes.read_text())["samples"]
    rows = previous.attribute(samples, events)
    measured = [event for event in events if event["end_unix_us"] >= rows[0]["start_unix_us"]
                and event["start_unix_us"] <= rows[-1]["end_unix_us"]]
    publishes = [event for event in measured if event["stage"] == "directory_publish"]
    gates = [event for event in measured if event["stage"] == "gate_wait" and not event["background"]]
    accepts = [event for event in measured if event["stage"] == "journal_accept" and not event["background"]]
    summary = previous.summarize(rows, events)
    joins = {event["sequence"]: event for event in events
             if event["stage"] == "directory_join_wait"}
    installs = [event for event in measured if event["stage"] == "directory_install"]
    summary["directory_gate_hold_ms"] = previous.distribution(
        (event["end_unix_us"] - joins[event["sequence"]]["end_unix_us"]) / 1000
        for event in installs)
    for stage in ("directory_prepare", "directory_join_wait", "directory_install"):
        summary[stage + "_ms"] = previous.distribution(
            event["elapsed_us"] / 1000 for event in measured if event["stage"] == stage)
    summary["foreground_gate_admissions_during_current_io"] = sum(
        any(pub["start_unix_us"] < gate["end_unix_us"] < pub["end_unix_us"]
            for pub in publishes) for gate in gates)
    summary["foreground_appends_overlap_current_io"] = sum(
        any(previous.overlap_us(accept, pub) > 0 for pub in publishes) for accept in accepts)
    summary["memory_install_full_sync_us"] = [event["full_sync_us"] for event in measured
                                             if event["stage"] == "directory_install"]
    summary["barrier_counter_limit"] = "Process-wide sync deltas overlap concurrent I/O; elapsed intervals and gate admission times identify progress."
    args.output.mkdir(exist_ok=False)
    previous.save_rows(args.output / "events.json", events)
    previous.save_rows(args.output / "requests.json", rows)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({key: summary[key] for key in (
        "requests", "over_80_ms", "slow_largest_stage", "directory_install_ms",
        "directory_gate_hold_ms", "directory_publication_ms", "foreground_gate_admissions_during_current_io",
        "foreground_appends_overlap_current_io")}))


if __name__ == "__main__":
    main()
