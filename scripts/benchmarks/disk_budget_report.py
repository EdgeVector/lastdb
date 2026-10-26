#!/usr/bin/env python3
"""Publish a confirmed, advisory v2 disk-size budget in the CI summary."""

import argparse
import json
import os
from pathlib import Path
import statistics

from advisories import confirm, lines, notices
import disk_budget
from growth import CASES
from node import save


def compare(output, manifest):
    recipe = manifest["recipe"]
    if recipe.get("disk_budget") is not True or recipe.get("disk_format") != 2:
        raise ValueError("The disk budget recipe requires v2 generated homes")
    values = {label: {} for label in ("baseline", "candidate")}
    for run in manifest["runs"]:
        if not run["complete"]:
            raise ValueError("A disk budget measurement did not complete")
        directory = output / run["directory"]
        marker = json.loads((directory / "disk-format.json").read_text())
        if marker != {"marker": "format=3\nhome=disk-run\ncas=disk-cas\n"}:
            raise ValueError("The disk budget did not measure a v2 run home")
        cases = json.loads((directory / "growth.json").read_text())
        if set(cases) != set(CASES):
            raise ValueError("The disk budget cases differ from the recipe")
        for case in CASES:
            disk_budget.validate(case, cases[case], recipe["growth"])
        if run["pair"] in values[run["label"]]:
            raise ValueError("The disk budget contains a duplicate pair")
        values[run["label"]][run["pair"]] = cases
    pairs = set(range(recipe["pairs"]))
    if any(set(runs) != pairs for runs in values.values()):
        raise ValueError("The disk budget lacks complete pairs")
    comparison = {}
    for case in CASES:
        item = {label: {name: statistics.median(runs[p][case]["metrics"][name] for p in sorted(pairs))
                        for name in disk_budget.MEASURES} for label, runs in values.items()}
        item["paired_changes_percent"] = {
            name: [100 * (values["candidate"][p][case]["metrics"][name]
                          / values["baseline"][p][case]["metrics"][name] - 1) for p in sorted(pairs)]
            for name in disk_budget.MEASURES}
        item["advisories"] = disk_budget.advisories(item["baseline"], item["candidate"])
        comparison[case] = item
    return comparison


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--job-status", required=True)
    parser.add_argument("--confirmation-status", required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    try:
        manifest = json.loads((args.output / "benchmark.json").read_text())
        if not manifest["complete"] or args.job_status != "success":
            raise ValueError("The disk budget measurement did not complete")
        comparison = compare(args.output, manifest)
        save(args.output / "comparison.json", comparison)
        decision = confirm(args.output, manifest, comparison, compare, "disk", args.confirmation_status)
        report = ("# LastDB v2 disk size budget — advisory\n\n"
                  f"Profile: **{manifest['profile']}**. Pairs: **{manifest['recipe']['pairs']}**.  \n"
                  f"Base: `{manifest['binaries']['baseline']['revision']}`  \n"
                  f"Candidate: `{manifest['binaries']['candidate']['revision']}`  \n"
                  f"Driver: `{manifest['driver_revision']}`\n"
                  + disk_budget.render(comparison) + lines(decision))
        complete = True
    except (OSError, ValueError, KeyError, TypeError, ZeroDivisionError) as failure:
        report = f"# LastDB v2 disk size budget — incomplete\n\n{type(failure).__name__}: {failure}\n"
        complete = False
    (args.output / "summary.md").write_text(report)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
            stream.write(report)
    print(report)
    if complete:
        notices(decision)
    return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())
