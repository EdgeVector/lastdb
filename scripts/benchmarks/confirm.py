#!/usr/bin/env python3
"""Repeat a suspect suite once on the same GitHub runner, without a build."""

import argparse
import os
from pathlib import Path
import subprocess
import sys

from advisories import load, suspects
from growth_report import compare as growth_compare
from disk_budget_report import compare as disk_compare
from node import save
from report import compare as speed_compare


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--kind", choices=("speed", "growth", "disk"), required=True)
    args = parser.parse_args()
    if (os.environ.get("GITHUB_ACTIONS") != "true"
            or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"):
        parser.error("Automatic benchmark confirmation requires a GitHub-hosted Actions runner")
    compare = {"growth": growth_compare, "speed": speed_compare, "disk": disk_compare}[args.kind]
    manifest, comparison = load(args.output, compare)
    initial = sorted(suspects(comparison, args.kind))
    save(args.output / "repeat-plan.json", {"kind": args.kind, "suspects": initial, "max_repeats": 1})
    if not initial:
        print("No suspect measures; skip the confirmation run.", flush=True)
        return 0
    print(f"Repeat {args.kind} once for {len(initial)} suspect case/measure combinations.", flush=True)
    command = [sys.executable, str(Path(__file__).with_name("run.py")),
               "--profile", manifest["profile"], "--pairs", str(manifest["recipe"]["pairs"]),
               "--driver-revision", manifest["driver_revision"], "--output", str(args.output / "confirmation")]
    for label, binary in manifest["binaries"].items():
        command.extend([f"--{label}", binary["path"], f"--{label}-revision", binary["revision"],
                        f"--{label}-workers", str(manifest["recipe"]["uds_workers"][label])])
    if args.kind == "growth":
        command.append("--growth-only")
    if args.kind == "disk":
        command.append("--disk-budget-only")
    if manifest["recipe"].get("seed_with_baseline"):
        command.extend(["--seed-with-baseline", "--minimum-retired", str(manifest["recipe"]["minimum_retired"])])
    if manifest["recipe"].get("typed_range_comparison"):
        command.append("--candidate-typed-ranges")
    if manifest["recipe"]["native_timing"]:
        command.append("--native-timing")
    subprocess.run(command, check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
