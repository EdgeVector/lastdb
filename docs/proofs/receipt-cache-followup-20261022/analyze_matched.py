#!/usr/bin/env python3
"""Independently audit six final matched runs against bounded raw evidence."""

import argparse
import gzip
import hashlib
import json
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
from matched_audit_metrics import (ERASE_SPECS, IDENTITY, MEMORY, WRITE_SPECS, compact_metrics,
                                   equal, expected_request, inventory, paired, require, subset,
                                   unwrap, write_metrics)
from matched_audit_extra import erased, focused, state_at, state_digest

REVISIONS = {"before": "08d8436ccce9e2493e187a6b0133dc93f47c410f",
             "after": "e79b711517519c9ce27d0f6f1b405a364be3ba91"}
LABELS = tuple(f"{variant}-{pair}" for pair in (1, 2, 3) for variant in REVISIONS)
PHASES = {spec[0] for spec in WRITE_SPECS} | {"compaction"}


class Evidence:
    def __init__(self, root):
        self.root, self.hashes = root, {}

    def read(self, path, limit=64 * 1024 * 1024):
        if not path.exists() and not path.is_symlink():
            path = path.with_name(path.name + ".gz")
        require(not path.is_symlink() and path.is_file() and path.stat().st_size <= limit,
                "Missing, linked, or oversized evidence: " + str(path))
        require(path.resolve(strict=True).is_relative_to(self.root), "Evidence leaves the exact proof root")
        opener = gzip.open if path.suffix == ".gz" else open
        deadline, chunks, size, digest = time.monotonic() + 30, [], 0, hashlib.sha256()
        with opener(path, "rb") as stream:
            while True:
                block = stream.read(min(1024 * 1024, limit + 1 - size))
                if not block:
                    break
                size += len(block)
                require(size <= limit and time.monotonic() <= deadline, "Evidence exceeds its byte or time bound")
                digest.update(block)
                chunks.append(block)
        self.hashes[str(path.relative_to(self.root))] = {"decompressed_bytes": size, "sha256": digest.hexdigest()}
        return b"".join(chunks)

    def load(self, path):
        return json.loads(self.read(path))

    def receipts(self, folder):
        candidates = (folder / "receipts.jsonl", folder / "receipts.jsonl.gz", folder / "receipts.full.local.gz")
        source = next((path for path in candidates if path.exists() or path.is_symlink()), candidates[0])
        lines = self.read(source, 128 * 1024 * 1024).splitlines()
        require(0 < len(lines) <= 10000 and all(len(line) <= 8 * 1024 * 1024 for line in lines),
                "Full receipts exceed their finite line or count bound")
        return [json.loads(line) for line in lines]


def requests_from(rows):
    requests = [row for row in rows if row.get("operation") == "request"]
    require(requests and len({row["request_id"] for row in requests}) == len(requests), "Raw request identities repeat")
    for row in requests:
        require("request" in row and "response" in row, "Full request and response evidence is required")
        equal(row["http_status"], 200)
        require(row["started_monotonic_ns"] <= row["ended_monotonic_ns"], "A request ends before its start")
        equal(row["elapsed_seconds"], (row["ended_monotonic_ns"] - row["started_monotonic_ns"]) / 1e9)
        encoded = b"" if row["request"] is None else json.dumps(row["request"], separators=(",", ":")).encode()
        equal(row["request_bytes"], len(encoded))
        if row["path"] == "/api/mutations/batch":
            changes = row["request"]["mutations"]
            equal(unwrap(row)["count"], len(changes))
            require(unwrap(row)["durability"] in ("queued", "durable"), "A mutation lacks an ack contract")
            if any(change["durability"] == "durable" for change in changes):
                equal(unwrap(row)["durability"], "durable")
    return sorted(requests, key=lambda row: row["started_monotonic_ns"])


def checkpoints(evidence, folder, name, raw):
    rows = evidence.load(folder / name)
    require(0 < len(rows) <= 64 and len({row["label"] for row in rows}) == len(rows), "Invalid checkpoint list")
    captured = {row["label"]: row for row in raw if row.get("operation") == "checkpoint"}
    for row in rows:
        inventory(row["inventory"])
        receipt = captured[row["label"]]
        equal(row["native"], receipt["native"], "checkpoint native receipt")
        equal(row["process"], receipt["process"], "checkpoint process receipt")
        equal({key: value for key, value in row["inventory"].items() if key != "_files"}, receipt["inventory"],
              "checkpoint public inventory receipt")
    return rows, {row["label"]: row for row in rows}


def memory_scope(rows):
    identities = {}
    for row in rows:
        process = row["process"]
        identity = ":".join(str(process[key]) for key in IDENTITY)
        target = identities.setdefault(identity, {key: 0 for key in MEMORY})
        for key in MEMORY:
            target[key] = max(target[key], process[key])
    return {"checkpoint_count": len(rows), "process_count": len(identities), "by_identity": identities,
            "observed_peaks_bytes": {key: max(point[key] for point in identities.values()) for key in MEMORY}}


def lifecycle(raw):
    active, starts, stopped = None, [], []
    for row in raw:
        if row.get("operation") == "owned-node-start":
            require(active is None, "An owned node starts before its predecessor stops")
            active = row["pid"]
            starts.append(active)
        elif row.get("operation") == "owned-node-stop":
            equal(row["pid"], active, "owned stop identity")
            subset(row, {"exit_code": 0, "forced": False, "shutdown_receipt_error": None})
            equal(row["shutdown_receipt"]["flush_ok"], True)
            stopped.append(active)
            active = None
    require(active is None and len(starts) == len(stopped) == 4, "The run lacks four complete graceful node lifetimes")
    return starts


def audit_run(evidence, label, final):
    folder, variant = evidence.root / label, label.rsplit("-", 1)[0]
    require(not folder.is_symlink() and folder.resolve(strict=True).parent == evidence.root, "Run leaves proof root")
    summary, inputs = evidence.load(folder / "summary.json"), evidence.load(folder / "inputs.json")
    subset(summary, {"complete": True, "all_owned_processes_stopped": True,
                     "all_phase_values_verified": True, "graceful_reopen_verified": True})
    subset(inputs, {"source_revision": REVISIONS[variant], "automatic_maintenance": False})
    equal(inputs["settings"]["LASTDB_RESIDENT_PERSIST_MS"], "500")
    equal(inputs["binary_sha256"], final["builds"][variant]["binary_sha256"])
    equal(evidence.load(folder / "cleanup.json")["removed"], True)
    raw = evidence.receipts(folder)
    requests, pids = requests_from(raw), lifecycle(raw)
    equal(sum(row["path"] == "/api/mutations/batch" for row in requests), 514, "full mutation request count")
    sets = [checkpoints(evidence, folder, name, raw) for name in
            ("checkpoints.json", "focused-checkpoints.json", "erase-checkpoints.json")]
    require(all(row["process"]["pid"] in pids for rows, _ in sets for row in rows), "Checkpoint PID lacks an owned lifetime")
    matched_rows, matched = sets[0]
    focus_value = evidence.load(folder / "focused-compaction.json")
    erase_value = evidence.load(folder / "erase-results.json")
    require(len(summary["phases"]) == 6 and {row["label"] for row in summary["phases"]} == PHASES, "Phase set differs")
    phase_values = {row["label"]: row for row in summary["phases"]}
    phases = {}
    for name, value in phase_values.items():
        stem = "compact" if name == "compaction" else name
        first, last = matched[stem + "-before"], matched[stem + "-after"]
        phases[name] = (compact_metrics if name == "compaction" else write_metrics)(value, first, last, requests)
        reported = final["runs"][label]["phases"][name]
        equal(reported["metrics"], phases[name], label + "." + name + " metrics")
        if name != "compaction":
            subset(reported, {"inputs": value["inputs"], "new_or_grown_by_family": value["growth"]["new_or_grown"],
                              "inventory_families_before": first["inventory"]["families"],
                              "inventory_families_after": last["inventory"]["families"]})
        else:
            subset(reported, {"probes": value["probes"], "report": value["report"]["compact"]["native_disk"]})
    seed = [row for row in requests if row["phase"] == "seed" and row["path"] == "/api/mutations/batch"]
    equal(len(seed), 16)
    for ordinal, row in enumerate(seed):
        equal(row["request"], expected_request(range(ordinal * 32, (ordinal + 1) * 32), 0, operation="create"))
    equal(summary["expected_values_sha256"], state_digest(state_at(requests, focus_value["dispatched_monotonic_ns"])))
    subset(summary["extra_focus"], {"matched_measurements_unchanged": True})
    subset(summary["extra_erase"], {"matched_measurements_unchanged": True})
    focus = focused(focus_value, sets[1][1], requests)
    erase = erased(erase_value, sets[2][1], requests)
    equal(final["focused_compaction"]["runs"][label], focus, label + " focused root summary")
    equal(final["erase_benchmark"]["runs"][label], erase, label + " erase root summary")
    peaks = {}
    for row in matched_rows:
        process = row["process"]
        identity = f'{process["pid"]}:{process["ri_proc_start_abstime"]}'
        peaks[identity] = max(peaks.get(identity, 0), process["ri_lifetime_max_phys_footprint"])
    subset(final["runs"][label], {"source_revision": REVISIONS[variant], "binary_sha256": inputs["binary_sha256"],
                                 "settings": inputs["settings"], "helpers_sha256": inputs["helpers_sha256"],
                                 "expected_values_sha256": summary["expected_values_sha256"],
                                 "process_lifetime_peaks_by_identity": peaks, "peak_process_memory_bytes": max(peaks.values())})
    return {"phases": phases, "phase_values": phase_values, "focus": focus, "erase": erase, "inputs": inputs,
            "matched_peak": max(peaks.values()), "expected_values_sha256": summary["expected_values_sha256"],
            "request_count": len(requests), "memory": {
                "matched_plus_focused": memory_scope(sets[0][0] + sets[1][0]),
                "all_captured_including_erase": memory_scope([row for rows, _ in sets for row in rows])}}


def compare(runs, final):
    count = 0
    for name in PHASES:
        pair_metrics = [(runs[f"before-{n}"]["phases"][name], runs[f"after-{n}"]["phases"][name]) for n in (1, 2, 3)]
        derived = {key: paired([(first[key], last[key]) for first, last in pair_metrics]) for key in pair_metrics[0][0]}
        equal(final["comparisons"][name]["metrics"], derived, name + " paired distributions")
        count += len(derived)
        equal(final["comparisons"][name]["independent_pairs"], 3)
        if name == "compaction":
            overlaps = {variant: {role: sum(runs[f"{variant}-{n}"]["phase_values"][name]["probes"][role]
                                          ["request_intervals_overlap"] for n in (1, 2, 3))
                                  for role in ("warm_read", "durable_write")} for variant in REVISIONS}
            equal(final["comparisons"][name]["probe_request_overlap_counts"], overlaps)
        else:
            equal(final["comparisons"][name]["frontier_deltas_match_in_all_pairs"],
                  all(first["frontier_delta"] == last["frontier_delta"] for first, last in pair_metrics))
    focuses = [(runs[f"before-{n}"]["focus"], runs[f"after-{n}"]["focus"]) for n in (1, 2, 3)]
    focus_expected = {"compact_wall_seconds": paired([(first["compact_wall_seconds"], last["compact_wall_seconds"])
                                                       for first, last in focuses]), "roles": {}}
    count += 1
    for role in ("warm_read", "durable_write"):
        common = set.intersection(*(set(arm["roles"][role]) for pair in focuses for arm in pair))
        focus_expected["roles"][role] = {key: paired([(first["roles"][role][key], last["roles"][role][key])
                                                    for first, last in focuses]) for key in common}
        count += len(common)
    equal(final["focused_compaction"]["comparisons"], focus_expected, "focused paired distributions")
    for name, *_ in ERASE_SPECS:
        pairs = [(runs[f"before-{n}"]["erase"]["phases"][name], runs[f"after-{n}"]["erase"]["phases"][name])
                 for n in (1, 2, 3)]
        for first, last in pairs:
            equal(first["inputs"], last["inputs"])
            equal(first["expected_values_sha256"], last["expected_values_sha256"])
        derived = {key: paired([(first["metrics"][key], last["metrics"][key]) for first, last in pairs])
                   for key in pairs[0][0]["metrics"]}
        equal(final["erase_benchmark"]["comparisons"][name], derived, name + " paired distributions")
        count += len(derived)
    equal(final["peak_process_memory_bytes"], paired([(runs[f"before-{n}"]["matched_peak"],
                                                     runs[f"after-{n}"]["matched_peak"]) for n in (1, 2, 3)]))
    return count + 1


def memory_distributions(runs):
    result = {}
    for scope in ("matched_plus_focused", "all_captured_including_erase"):
        result[scope] = {"runs": {label: run["memory"][scope] for label, run in runs.items()},
                         "paired_peaks_bytes": {key: paired([(runs[f"before-{n}"]["memory"][scope]["observed_peaks_bytes"][key],
                                                              runs[f"after-{n}"]["memory"][scope]["observed_peaks_bytes"][key])
                                                             for n in (1, 2, 3)]) for key in MEMORY}}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--output", type=Path, required=True, help="New audit artifact; existing files are refused")
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    require(not {".lastdb", ".folddb", ".primary-lastdb-archive-20261014"}.intersection(root.parts), "Invalid proof root")
    require(not args.output.exists() and not args.output.is_symlink(), "Audit output must be a new file")
    evidence = Evidence(root)
    final, manifest = evidence.load(root / "summary.json"), evidence.load(root / "pair-runs.json")
    subset(final, {"complete": True, "benchmark_round": 4, "independent_pairs": 3, "runs_per_variant": 3,
                   "request_samples_pooled": False, "automatic_maintenance": False, "flush_ms": 500})
    equal(set(final["runs"]), set(LABELS))
    require(manifest["complete"] is True and len(manifest["runs"]) == 6, "Final six-run manifest is incomplete")
    equal({row["run"] for row in manifest["runs"]}, set(LABELS))
    for row in manifest["runs"]:
        subset(row, {"exit_code": 0, "scratch_removed": True})
    for variant, revision in REVISIONS.items():
        build = evidence.load(root / (variant + "-build.json"))
        subset(build, {"revision": revision, "product_source_clean": True})
        equal(final["builds"][variant], manifest["builds"][variant])
        for key in ("revision", "binary_sha256", "product_source_clean"):
            equal(final["builds"][variant][key], build[key])
    runs = {label: audit_run(evidence, label, final) for label in LABELS}
    equal(set(final["comparisons"]), PHASES)
    equal(set(final["focused_compaction"]["runs"]), set(LABELS))
    equal(set(final["erase_benchmark"]["runs"]), set(LABELS))
    subset(final["erase_benchmark"], {"independent_pairs": 3, "requests_pooled": False})
    for variant in REVISIONS:
        for key in ("cargo", "rustc", "build_command"):
            equal(final["builds"][variant][key], final["builds"]["before"][key])
    for run in runs.values():
        equal(run["inputs"]["helpers_sha256"], runs["before-1"]["inputs"]["helpers_sha256"])
    for number in (1, 2, 3):
        first, last = runs[f"before-{number}"], runs[f"after-{number}"]
        for key in ("settings", "helpers_sha256"):
            equal(first["inputs"][key], last["inputs"][key])
        equal(first["expected_values_sha256"], last["expected_values_sha256"])
        equal(first["focus"]["write_inputs"], last["focus"]["write_inputs"])
        equal(first["focus"]["expected_values_sha256"], last["focus"]["expected_values_sha256"])
    count = compare(runs, final)
    equal(count, 351, "required paired distribution count")
    result = {"all_audits_passed": True, "source_revisions": REVISIONS, "runs": list(runs),
              "paired_distributions_verified": count, "raw_request_counts": {label: run["request_count"] for label, run in runs.items()},
              "process_memory": memory_distributions(runs), "input_sha256": evidence.hashes,
              "limitations": ["Ack sequence times are checked against the last raw ack and timed flush start; the driver omits an exact final-ack marker.",
                              "Process counters describe the captured node, not physical SSD writes.",
                              "Checkpoint peaks omit unsampled processes and activity after the last checkpoint.",
                              "Compaction overlap identifies request intervals, not exact internal gate ownership."],
              "audit_source_sha256": {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in
                                       (Path(__file__), Path(__file__).with_name("matched_audit_metrics.py"),
                                        Path(__file__).with_name("matched_audit_extra.py"))}}
    with args.output.open("x") as stream:
        stream.write(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"all_audits_passed": True, "runs": 6, "paired_distributions_verified": count}))


if __name__ == "__main__":
    main()
