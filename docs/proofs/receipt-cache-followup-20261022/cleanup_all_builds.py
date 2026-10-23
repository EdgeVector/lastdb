#!/usr/bin/env python3
"""Remove exact historical copies and isolated targets after all final proof."""

import json
import os
from pathlib import Path
import time

from cleanup_build import cleaned_runs
from owned_build_cleanup import ROOT, digest, load, remove


def capability_cleanups():
    result = []
    for variant in ("before", "after"):
        folder = ROOT / (variant + "-writer-capability")
        summary, cleanup = load(folder / "summary.json"), load(folder / "cleanup.json")
        if (summary.get("complete") is not True or summary.get("all_owned_processes_stopped") is not True
                or cleanup.get("removed") is not True or Path(summary["scratch"]).exists()):
            raise ValueError("A real writer capability home remains or its proof is incomplete")
        result.append({"run": folder.name, "summary_sha256": digest(folder / "summary.json"),
                       "cleanup_sha256": digest(folder / "cleanup.json")})
    failed = ROOT / "rejected-build-1/retention-operational"
    if (load(failed / "summary.json").get("complete") is not False
            or load(failed / "failed-scratch-cleanup.json").get("removed") is not True):
        raise ValueError("Preserve the original false failure and complete its exact owned cleanup")
    return result


def accepted_final():
    if (load(ROOT / "isolated-followup-window.json").get("complete") is not True
            or load(ROOT / "matched-numeric-audit.json").get("all_audits_passed") is not True):
        raise ValueError("All final stages and the complete matched numeric audit must pass")
    for variant in ("before", "draft", "after"):
        if load(ROOT / ("auto-" + variant + "-numeric-audit.json")).get("all_audits_passed") is not True:
            raise ValueError("A descriptive AUTO numeric audit is incomplete")
    return {(row["binary"], row["sha256"]) for row in load(ROOT / "final-build-checks.json")["accepted_binaries"]}


def entries(variant):
    rows = [load(ROOT / (variant + "-" + suffix)) for suffix in ("build.json", "cli-build.json")]
    rows += list(load(ROOT / (variant + "-disk-build.json"))["binaries"].values())
    return [{"path": row["binary"], "sha256": row["binary_sha256"]} for row in rows]


def main():
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise ValueError("Use the standard exclusive lock for final build-artifact cleanup")
    target = ROOT / "build-scratch-cleanup.json"
    if target.exists():
        raise ValueError("Do not replace a final build cleanup receipt")
    products = cleaned_runs() + capability_cleanups()
    accepted = accepted_final()
    historical = load(ROOT / "rejected-build-1/final-build-checks.json")["accepted_binaries"]
    old = [{"path": row["binary"], "sha256": row["sha256"]} for row in historical]
    groups = [("historical-and-rejected", load(ROOT / "build-scratch-owner.json"), old, False)]
    groups += [(variant, load(ROOT / (variant + "-isolated-owner.json")), entries(variant), True)
               for variant in ("before", "after")]
    all_entries = [row for _, _, rows, _ in groups for row in rows]
    if len(all_entries) != 25 or not accepted.issubset({(row["path"], row["sha256"]) for row in all_entries}):
        raise ValueError("The exact accepted and historical binary sets differ")
    value = {"removed": False, "generated_data_cleanup": products, "roots": [],
             "copied_binary_count": len(all_entries), "at_unix_ns": time.time_ns(),
             "driver_sha256": digest(Path(__file__))}
    target.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    for label, owner, binaries, has_target in groups:
        value["roots"].append(remove(owner, binaries, label, has_target))
        target.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    value["removed"] = all(row["removed"] for row in value["roots"])
    target.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"all_owned_build_roots_removed": value["removed"], "copied_binaries": 25}))


if __name__ == "__main__":
    main()
