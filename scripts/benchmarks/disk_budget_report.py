#!/usr/bin/env python3
"""Require the confirmed v2 disk-size limit and report relative-growth advisories."""

import argparse
import json
import os
from pathlib import Path
import statistics

from advisories import confirm, lines, notices
import disk_budget
from growth import CASES
from node import save
from typed_ranges import conversion_lines

REQUIRED_METRICS = {"max_disk_to_logical_ratio"}


def compare(output, manifest):
    recipe = manifest["recipe"]
    if (recipe.get("disk_budget") is not True or recipe.get("disk_format") != 2
            or recipe.get("search_enabled") is not False):
        raise ValueError("The disk budget recipe requires v2 generated homes with Search off")
    values = {label: {} for label in ("baseline", "candidate")}
    for run in manifest["runs"]:
        if not run["complete"]:
            raise ValueError("A disk budget measurement did not complete")
        directory = output / run["directory"]
        for phase in ("before", "after"):
            search = json.loads((directory / f"disk-search-{phase}.json").read_text())
            if not isinstance(search, dict) or search.get("enabled") is not False or search.get("status_error"):
                raise ValueError("The disk budget does not establish Search off")
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
                          / values["baseline"][p][case]["metrics"][name] - 1)
                   if values["baseline"][p][case]["metrics"][name] else None for p in sorted(pairs)]
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
    exceeded = False
    try:
        manifest = json.loads((args.output / "benchmark.json").read_text())
        if not manifest["complete"] or args.job_status != "success":
            raise ValueError("The disk budget measurement did not complete")
        comparison = compare(args.output, manifest)
        save(args.output / "comparison.json", comparison)
        decision = confirm(args.output, manifest, comparison, compare, "disk", args.confirmation_status)
        exceeded = any(row["confirmed"] and row["metric"] in REQUIRED_METRICS for row in decision["results"])
        report = ("# LastDB v2 disk size budget — required 1.5× limit\n\n"
                  f"Required limit: **{'FAIL' if exceeded else 'PASS'}**. Relative growth stays advisory.\n\n"
                  f"Profile: **{manifest['profile']}**. Pairs: **{manifest['recipe']['pairs']}**.  \n"
                  f"Base: `{manifest['binaries']['baseline']['revision']}`  \n"
                  f"Candidate: `{manifest['binaries']['candidate']['revision']}`  \n"
                  f"Driver: `{manifest['driver_revision']}`\n"
                  + disk_budget.render(comparison) + lines(decision, REQUIRED_METRICS)
                  + "\n".join(conversion_lines(args.output, manifest)) + "\n")
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
        notices(decision, REQUIRED_METRICS)
    return 0 if complete and not exceeded else 1


if __name__ == "__main__":
    raise SystemExit(main())
