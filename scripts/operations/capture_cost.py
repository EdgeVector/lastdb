#!/usr/bin/env python3
"""What capturing a write costs the caller, on the baseline and the candidate of a benchmark run.

A node that syncs captures every write: the commit puts an entry in the spool before the caller is
answered. The other benchmark lanes run a store that captures nothing, so they do not see that work.
This lane does.

Real processes only: each build's `lastdbd` and `lastdb`, and the `reference_relay` built from the
driver source. For each build and each round a fresh home runs the same writes twice: once with no
relay (nothing is captured) and once with the relay set (every write is captured, and the daemon
sends in the background). The order of the builds alternates between rounds.

The time is the caller's wall time of one `/v1/write` request, answered after the commit:
  single   one record with a 1,024-byte value
  batch    128 such records in one request
  file 1   one record with a 1 MiB file named by path
  file 16  one record with a 16 MiB file named by path

The captured run also checks that every write reached the relay. The times are advisory. Speed on a
hosted runner varies between runs: read the difference between the two builds in the same run, not
the absolute times.
"""
import argparse
import json
import os
from pathlib import Path
import platform
import statistics
import sys
import time
from types import SimpleNamespace

_OPS = Path(__file__).resolve().parent
sys.path[:0] = [str(_OPS), str(_OPS.parent)]
from benchmarks.node import Node  # noqa: E402
from harness import append_step_summary  # noqa: E402
from personal_proof_nodes import ALPHA, FIELDS, MEBIBYTE, Device, Relay, wait  # noqa: E402
from record_runs import definition, require, system_seed  # noqa: E402
import v1_records as v1  # noqa: E402

WARM, SINGLES, BATCHES, BATCH = 100, 1000, 30, 128
FILES = ((1, 20), (16, 6))
CLASSES = ("single", "batch", "file 1", "file 16")
VALUE = "v" * 1024
SYNC_SECONDS = 900
LABELS = ("baseline", "candidate")


def record(key):
    return v1.create_item(ALPHA, {"hash": key}, {"id": key, "note": VALUE})


def timed(node, mutations):
    started = time.perf_counter()
    v1.write_items(node, mutations)
    return (time.perf_counter() - started) * 1000


def make_files(folder):
    """One file of random bytes for each timed file write: size in MiB -> paths."""
    folder.mkdir(mode=0o700, parents=True)
    found = {}
    for size, count in FILES:
        found[size] = []
        for index in range(count):
            path = folder / f"file-{size}-{index}.bin"
            path.write_bytes(os.urandom(size * MEBIBYTE))
            found[size].append(path)
    return found


def workload(node, files, settle):
    """Run every class of write on one fresh home. `settle` runs between classes. Class -> times in ms."""
    system_seed(node, definition(ALPHA, "Hash", FIELDS, description_prefix="Capture cost "))
    for index in range(WARM):
        v1.write_items(node, [record(f"warm-{index}")])
    settle()
    times = {"single": [timed(node, [record(f"single-{index}")]) for index in range(SINGLES)]}
    settle()
    times["batch"] = [timed(node, [record(f"batch-{batch}-{index}") for index in range(BATCH)])
                      for batch in range(BATCHES)]
    settle()
    for size, paths in files.items():
        times[f"file {size}"] = []
        for index, path in enumerate(paths):
            item = record(f"file-{size}-{index}")
            item["file"] = {"path": str(path), "name": path.name, "media_type": "application/octet-stream"}
            times[f"file {size}"].append(timed(node, [item]))
        settle()
    return times


def run_plain(build, out, files):
    """A home with no relay: the store captures nothing."""
    out.mkdir(parents=True)
    node = Node(Path(build["daemon"]), build["revision"], out)
    try:
        node.start()
        return workload(node, files, lambda: None)
    finally:
        node.close()


def run_captured(build, name, out, relay, files):
    """A home that syncs with the relay: every write is captured, and all of them reach the relay."""
    args = SimpleNamespace(daemon=build["daemon"], cli=build["cli"], revision=build["revision"])
    device = Device(name, args, out, relay)
    try:
        device.init()
        device.start()
        times = workload(device.node, files,
                         lambda: wait(f"{name} to upload every entry", device.synced_up, SYNC_SECONDS))
        state = device.spool_report()
        writes = WARM + SINGLES + BATCHES * BATCH + sum(count for _, count in FILES)
        require(state is not None and state["head"] >= writes and state["head"] == state["acked"],
                f"{name}: the spool reports {state!r} after {writes} records")
        return times
    finally:
        device.node.close()


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * fraction))]


def summarize(times):
    return {name: {"p50": statistics.median(values), "p95": percentile(values, 0.95),
                   "mean": statistics.fmean(values)} for name, values in times.items()}


def middle(rounds, mode, label, name, measure):
    """The median over the rounds of one measure of one class."""
    return statistics.median(entry[mode][label][name][measure] for entry in rounds)


def table(rounds, builds):
    lines = [f"## What capturing a write costs the caller ({platform.system()} {platform.machine()})",
             f"- baseline: `{builds['baseline']['revision'][:9]}`",
             f"- candidate: `{builds['candidate']['revision'][:9]}`",
             f"- {len(rounds)} rounds, a fresh home for each build and mode; the median over the rounds "
             "is shown, in ms. Advisory.",
             "",
             "| Write | Measure | Baseline, no capture | Candidate, no capture | Baseline, captured | "
             "Candidate, captured | Candidate minus baseline, captured |",
             "|---|---|---:|---:|---:|---:|---:|"]
    for name in CLASSES:
        for measure in ("p50", "p95", "mean"):
            cells = [middle(rounds, mode, label, name, measure)
                     for mode in ("plain", "captured") for label in LABELS]
            gap = cells[3] - cells[2]
            share = f"{gap / cells[2] * 100:+.0f}%" if cells[2] else "n/a"
            lines.append(f"| {name} | {measure} | " + " | ".join(f"{cell:.2f}" for cell in cells)
                         + f" | {gap:+.2f} ({share}) |")
    lines += ["", "Each round, p50 in ms (baseline plain, candidate plain, baseline captured, candidate captured):"]
    for index, entry in enumerate(rounds, 1):
        cells = [f"{name}: " + ", ".join(f"{entry[mode][label][name]['p50']:.2f}"
                                         for mode in ("plain", "captured") for label in LABELS)
                 for name in CLASSES]
        lines.append(f"- round {index}: " + "; ".join(cells))
    return "\n".join(lines) + "\n"


def measure(args, builds, relay, files):
    rounds = []
    for index in range(args.rounds):
        order = LABELS if index % 2 == 0 else tuple(reversed(LABELS))
        entry = {"plain": {}, "captured": {}}
        for label in order:
            name = f"r{index + 1}-{label}"
            entry["plain"][label] = summarize(run_plain(builds[label], args.output / f"{name}-plain", files))
            entry["captured"][label] = summarize(
                run_captured(builds[label], f"{name}-captured", args.output, relay, files))
            print(f"round {index + 1} {label}: plain single p50 {entry['plain'][label]['single']['p50']:.2f} ms, "
                  f"captured single p50 {entry['captured'][label]['single']['p50']:.2f} ms", flush=True)
        rounds.append(entry)
    return rounds


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--relay-bin", required=True, type=Path)
    for label in LABELS:
        for part in ("daemon", "cli", "revision"):
            parser.add_argument(f"--{label}-{part}", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--rounds", type=int, default=3)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    builds = {label: {part: getattr(args, f"{label}_{part}") for part in ("daemon", "cli", "revision")}
              for label in LABELS}
    files = make_files(args.output / "files")
    relay = Relay(args.relay_bin, args.output)
    relay.start()
    try:
        rounds = measure(args, builds, relay, files)
    finally:
        relay.stop()
        for paths in files.values():
            for path in paths:
                path.unlink(missing_ok=True)
    (args.output / "capture-cost.json").write_text(json.dumps(
        {"platform": f"{platform.system()} {platform.machine()}", "builds": builds, "rounds": rounds}, indent=1))
    report = table(rounds, builds)
    print(report, flush=True)
    append_step_summary(report)


if __name__ == "__main__":
    main()
