#!/usr/bin/env python3
"""Preserve the failed shared-target provenance without changing its receipts."""

import hashlib
import json
from pathlib import Path
import time

ROOT = Path(__file__).resolve().parent


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def load(name):
    return json.loads((ROOT / name).read_text())


def main():
    target = ROOT / "rejected-cache-build-1.json"
    if target.exists():
        raise ValueError("Do not replace a rejected-build distinction")
    failed = load("retention-operational/summary.json")
    verified = load("retention-operational/retained-root-verified.json")
    after, before = load("after-disk-build.json"), load("before-disk-build.json")
    if (failed.get("complete") is not False or failed.get("all_owned_processes_stopped") is not True
            or verified["features"] & 512):
        raise ValueError("The exact original accounting guard failure differs")
    physical = {}
    for name, item in after["binaries"].items():
        if item["binary_sha256"] != before["binaries"][name]["binary_sha256"]:
            raise ValueError("The observed duplicate physical-tool identity differs")
        if digest(Path(item["binary"])) != item["binary_sha256"]:
            raise ValueError("A rejected copied physical tool changed")
        physical[name] = {"binary": item["binary"], "same_sha256_as_08d_baseline": item["binary_sha256"]}
    names = ["after-cache-preparation.json", "after-build.json", "after-cli-build.json", "after-disk-build.json",
             "final-build-checks.json", "followup-window.json", "review-runs.json", "operational-runs.json",
             "retention-operational/summary.json", "retention-operational/ownership.json",
             "retention-operational/retained-root-verified.json", "retention-operational/seed-response.json",
             "after-profile-run.json", "after-profile-1/summary.json", "after-profile-1/diagnostic-inputs.json",
             "after-profile-1/profile-provenance.json"]
    result = {"recorded_unix_ns": time.time_ns(), "rejected_for_measurement": True,
              "product_source_change_required_established": False, "product_failure_established": False,
              "cause": "Shared-target node-only cache preparation left baseline engine/tool artifacts; the node version stamp did not prove linked library identity",
              "receipt_feature_observed": verified["features"], "required_feature_bit_missing": 512,
              "original_false_summary_preserved": True, "all_owned_processes_stopped": True,
              "failed_owned_home_retained": failed["scratch"], "no_matched_or_auto_run_started": True,
              "prior_acceptance_preserved_but_insufficient_for_engine_provenance": True,
              "physical_tools": physical,
              "invalid_for_cache_performance_claim": "after-profile-1",
              "rejected_evidence_sha256": {name: digest(ROOT / name) for name in names},
              "driver_sha256": digest(Path(__file__))}
    target.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"rejected_build_recorded": True, "feature": verified["features"], "preserved_false_summary": True}))


if __name__ == "__main__":
    main()
