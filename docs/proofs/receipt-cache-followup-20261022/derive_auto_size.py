#!/usr/bin/env python3
"""Derive observed size peaks and finite-window rates from completed AUTO data."""

import hashlib
import json
from pathlib import Path

from analyze_auto import equal, load, read_bytes

ROOT = Path(__file__).resolve().parent
METRICS = ("bytes", "allocated_bytes", "files")


def digest(path):
    return hashlib.sha256(read_bytes(path)).hexdigest()


def derive(variant):
    folder = ROOT / ("auto-" + variant + "-1")
    summary_path, samples_path = folder / "summary.json", folder / "size-samples.json"
    audit_path = ROOT / ("auto-" + variant + "-numeric-audit.json")
    summary, samples, audit = load(summary_path), load(samples_path), load(audit_path)
    if summary.get("complete") is not True or audit.get("all_audits_passed") is not True:
        raise ValueError("Require a complete capture and its unchanged numeric audit")
    equal(digest(summary_path), audit["runs"][0]["input_sha256"]["summary.json"])
    equal(digest(samples_path), summary["size_samples_sha256"])
    equal(digest(samples_path), audit["runs"][0]["input_sha256"]["size-samples.json"])
    if not 4 <= len(samples) <= 64:
        raise ValueError("The saved sample count leaves its finite bound")
    selected = {name: summary[name]["inventory"] for name in
                ("seeded", "after_active", "after_quiet_tail", "stable_final")}
    for inventory in (row["inventory"] for row in samples):
        equal(inventory["atomic_inventory"], False)
        for metric in METRICS:
            equal(inventory[metric], sum(row[metric] for row in inventory["families"].values()))
    peaks = {}
    for metric in METRICS:
        row = max(samples, key=lambda item: item["inventory"][metric])
        inventory = row["inventory"]
        peaks[metric] = {"value": inventory[metric], "sample_label": row["label"],
                         "sample_started_unix_ns": inventory["started_unix_ns"],
                         "sample_ended_unix_ns": inventory["ended_unix_ns"],
                         "sample_totals": {name: inventory[name] for name in METRICS}}
    growth = {name: selected["after_active"][name] - selected["seeded"][name] for name in METRICS}
    tail = {name: selected["after_quiet_tail"][name] - selected["after_active"][name] for name in METRICS}
    return {"variant": variant, "n": 1, "observed_sample_peaks": peaks,
            "actual_active_seconds": summary["actual_active_seconds"],
            "active_net_growth": growth,
            "active_net_growth_per_second": {name: value / summary["actual_active_seconds"]
                                             for name, value in growth.items()},
            "observed_quiet_tail_seconds": summary["observed_quiet_tail_seconds"],
            "quiet_tail_delta": tail,
            "selected_totals": {label: {name: row[name] for name in METRICS}
                                for label, row in selected.items()},
            "input_sha256": {"summary.json": digest(summary_path),
                             "size-samples.json": digest(samples_path),
                             "numeric-audit.json": digest(audit_path)},
            "scope": "Observed interval samples can miss a larger peak. Net active growth divided by actual active seconds describes this finite window; it is not a steady-state slope. Inventories occur around the checkpoints, while the denominator ends at the final ack, so the rate is not an exact write-byte production rate. Tail deltas are signed."}


def main():
    target = ROOT / "auto-retained-size-derived.json"
    if target.exists():
        raise ValueError("Do not replace a prior size derivation")
    value = {"complete": True, "driver_sha256": digest(Path(__file__)),
             "runs": [derive(variant) for variant in ("before", "draft", "after")]}
    target.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"complete": True, "variants": 3}))


if __name__ == "__main__":
    main()
