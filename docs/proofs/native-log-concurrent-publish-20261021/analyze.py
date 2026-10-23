#!/usr/bin/env python3
"""Attribute real native writes and confirm progress during lookup preparation."""

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
    previous.STAGES.add("directory_prepare")
    events = previous.read_events(args.log)
    samples = json.loads(args.writes.read_text())["samples"]
    rows = previous.attribute(samples, events)
    prepare = [event for event in events if event["stage"] == "directory_prepare"]
    accepts = {event["sequence"]: event for event in events if event["stage"] == "journal_accept"}
    for row in rows:
        accept = accepts[row["sequence"]]
        row["accepted_during_lookup_prepare"] = any(
            row["sequence"] > event["sequence"]
            and event["start_unix_us"] <= accept["start_unix_us"]
            and accept["end_unix_us"] <= event["end_unix_us"]
            for event in prepare)
        row["ack_during_lookup_prepare"] = any(
            row["sequence"] > event["sequence"]
            and event["start_unix_us"] <= row["start_unix_us"]
            and row["end_unix_us"] <= event["end_unix_us"]
            for event in prepare)
    summary = previous.summarize(rows, events)
    summary["lookup_preparation_ms"] = previous.distribution(
        event["elapsed_us"] / 1000 for event in prepare
        if rows[0]["start_unix_us"] <= event["start_unix_us"] <= rows[-1]["end_unix_us"])
    summary["accepted_during_lookup_prepare"] = sum(row["accepted_during_lookup_prepare"] for row in rows)
    summary["ack_during_lookup_prepare"] = sum(row["ack_during_lookup_prepare"] for row in rows)
    summary["barrier_counter_limit"] = "Process-wide sync counter deltas overlap concurrent appends and preparation; they are not exclusive phase costs."
    args.output.mkdir(exist_ok=False)
    previous.save_rows(args.output / "events.json", events)
    previous.save_rows(args.output / "requests.json", rows)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({name: summary[name] for name in (
        "requests", "over_80_ms", "slow_largest_stage", "accepted_during_lookup_prepare",
        "ack_during_lookup_prepare", "lookup_preparation_ms", "directory_publication_ms")}))


if __name__ == "__main__":
    main()
