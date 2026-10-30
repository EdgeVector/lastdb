#!/usr/bin/env python3
"""Require the total v2 disk-size ceiling after one pass, without a repeat."""

import argparse
import json
import os
from pathlib import Path

import disk_budget
from node import BASELINE_NODE_MARKER, RUN_NODE_MARKER, save


def compare(output, manifest):
    recipe = manifest["recipe"]
    if (recipe.get("disk_budget") is not True or recipe.get("disk_format") != 2
            or recipe.get("search_enabled") is not False or recipe.get("pairs") != 1):
        raise ValueError("The disk budget requires one v2 pair with Search off")
    values = {label: {} for label in ("baseline", "candidate")}
    for run in manifest["runs"]:
        if not run["complete"]:
            raise ValueError("A disk budget measurement did not complete")
        directory = output / run["directory"]
        for corpus in (directory, directory / "delete"):
            for phase in ("before", "after"):
                search = json.loads((corpus / f"disk-search-{phase}.json").read_text())
                if not isinstance(search, dict) or search.get("enabled") is not False or search.get("status_error"):
                    raise ValueError("The disk budget does not establish Search off in both homes")
            marker = json.loads((corpus / "disk-format.json").read_text())
            if set(marker) != {"marker"} or marker["marker"] not in (BASELINE_NODE_MARKER, RUN_NODE_MARKER):
                raise ValueError("The disk budget did not measure two v2 run homes")
        cases = json.loads((directory / "growth.json").read_text())
        if set(cases) != set(disk_budget.CASES):
            raise ValueError("The disk budget cases differ from the recipe")
        deleted = json.loads((directory / "delete/growth.json").read_text())
        cleanup = json.loads((directory / "delete/cleanup.json").read_text())
        if (deleted != {"delete": cases["delete"]}
                or cleanup.get("daemon_stopped") is not True or not cleanup.get("removed")):
            raise ValueError("The separate delete case lacks matching results and complete cleanup")
        for case in disk_budget.CASES:
            disk_budget.validate(case, cases[case], recipe["growth"])
        if run["pair"] in values[run["label"]]:
            raise ValueError("The disk budget contains a duplicate pair")
        values[run["label"]][run["pair"]] = cases
    if any(set(runs) != {0} for runs in values.values()):
        raise ValueError("The disk budget lacks its single complete pair")
    comparison = {}
    for case in disk_budget.CASES:
        item = {label: {name: runs[0][case]["metrics"][name] for name in disk_budget.MEASURES}
                for label, runs in values.items()}
        item["paired_changes_percent"] = {
            name: [100 * (item["candidate"][name] / item["baseline"][name] - 1)
                   if item["baseline"][name] else None] for name in disk_budget.MEASURES}
        item["advisories"] = disk_budget.advisories(item["baseline"], item["candidate"])
        comparison[case] = item
    return comparison


def headroom_lines(comparison):
    rows = ["", "## Room below the total 1.5× limit", "",
            "At each case's highest settled ratio. Negative room means excess.", "",
            "| Case | Maximum ratio | Room MiB | Room % of limit |", "|---|---:|---:|---:|"]
    for case, item in comparison.items():
        value = item["candidate"]
        rows.append(f"| {case} | {value['max_disk_to_logical_ratio']:.6f}× | "
                    f"{value['limit_headroom_bytes'] / 2**20:.3f} | {value['limit_headroom_percent']:.2f}% |")
    deleted = comparison["delete"]["candidate"]
    rows.extend(["", f"Delete corpus: {deleted['starting_logical_atom_bytes'] / 2**20:.3f} MiB logical; "
                 f"deleted {deleted['deleted_records']} of {deleted['starting_live_records']} records; "
                 f"{deleted['logical_atom_bytes'] / 2**20:.3f} MiB logical remains."])
    return "\n".join(rows) + "\n"


def notices(comparison):
    for case, item in comparison.items():
        ratio = item["candidate"]["max_disk_to_logical_ratio"]
        if ratio > disk_budget.LIMIT:
            print(f"::error title=Required disk size limit exceeded::{case}: {ratio:.6f}x exceeds "
                  f"{disk_budget.LIMIT}x total. This single-pass result blocks merge.")
        for name in item["advisories"]:
            print(f"::warning title=Disk growth advisory::{case}: {name}; "
                  f"{item['baseline'][name]} -> {item['candidate'][name]} bytes. This does not block merge.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--job-status", required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    exceeded = False
    try:
        manifest = json.loads((args.output / "benchmark.json").read_text())
        if not manifest["complete"] or args.job_status != "success":
            raise ValueError("The disk budget measurement did not complete")
        comparison = compare(args.output, manifest)
        save(args.output / "comparison.json", comparison)
        exceeded = any(item["candidate"]["max_disk_to_logical_ratio"] > disk_budget.LIMIT
                       for item in comparison.values())
        report = ("# LastDB v2 disk size budget — required 1.5× total limit\n\n"
                  f"Required limit: **{'FAIL' if exceeded else 'PASS'}**. One pair; no repeat.\n\n"
                  f"Profile: **{manifest['profile']}**.  \n"
                  f"Base: `{manifest['binaries']['baseline']['revision']}`  \n"
                  f"Candidate: `{manifest['binaries']['candidate']['revision']}`  \n"
                  f"Driver: `{manifest['driver_revision']}`\n"
                  + disk_budget.render(comparison) + headroom_lines(comparison))
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
        notices(comparison)
    return 0 if complete and not exceeded else 1


if __name__ == "__main__":
    raise SystemExit(main())
