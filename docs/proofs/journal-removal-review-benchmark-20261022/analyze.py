#!/usr/bin/env python3
"""Analyze the frozen captures with an explicit check of the two known build commands."""

import argparse
import importlib.util
import json
from pathlib import Path
import shlex
import sys

sys.dont_write_bytecode = True
from reuse import ROOT, helper_files, original, validate_revisions
import erase_benchmark


def review_analyzer():
    path = ROOT / "summarize.py"
    spec = importlib.util.spec_from_file_location("journal_review_frozen_analysis", path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def command_equivalence(builds):
    base = ["cargo", "build", "--locked", "--release", "-p", "lastdb_node", "--bin", "lastdbd"]
    wrapper = ["scripts/ci/with-fold-host-cargo-lock.sh", "--"]
    result = {}
    for label in ("before", "after"):
        build = builds[label]
        raw = shlex.split(build["build_command"])
        wrapped = raw[:len(wrapper)] == wrapper
        command = raw[len(wrapper):] if wrapped else raw
        expected = base if label == "before" else [*base, "--bin", "lastdb"]
        if command != expected or (label == "before" and not wrapped) or (label == "after" and wrapped):
            raise ValueError("A build command differs from the two exact recorded Cargo invocations")
        environment = build["build_environment"]
        if environment.get("profile", "release") != "release":
            raise ValueError("A recorded build profile differs from the explicit release command")
        if environment.get("package_features", "lastdb_node defaults: sentry-telemetry,purging-allocator") != (
                "lastdb_node defaults: sentry-telemetry,purging-allocator"):
            raise ValueError("A recorded package feature selection differs")
        result[label] = {"actual_command": build["build_command"], "actual_argv": raw,
                         "known_host_wrapper_removed": wrapped, "cargo_argv": command,
                         "measured_binary": "lastdbd", "additional_binary": "lastdb" if label == "after" else None}
    for key in ("cargo", "rustc"):
        if builds["before"][key] != builds["after"][key]:
            raise ValueError("The two recorded compiler toolchains differ")
    if builds["before"]["build_environment"]["CARGO_TARGET_DIR"] != builds["after"]["build_environment"]["CARGO_TARGET_DIR"]:
        raise ValueError("The two Cargo target directories differ")
    return {"equivalent_for_measured_lastdbd": True, "commands": result,
            "effective_profile": "release", "feature_selection": "Package defaults; neither command overrides features",
            "dependency_selection": "Both Cargo commands require --locked",
            "reason": "The baseline records the known host-lock wrapper. The final build also emits the lastdb CLI.",
            "limit": "Command equivalence preserves the recorded commands; it does not assert identical binaries or source."}


def validate_manifest(root, builds, analyzer):
    manifest = analyzer.load(root / "pair-runs.json")
    order = [["before", 1], ["after", 1], ["after", 2], ["before", 2], ["before", 3], ["after", 3]]
    runs = manifest.get("runs", [])
    if (manifest.get("complete") is not True or manifest.get("order") != order
            or [[row.get("label"), row.get("pair")] for row in runs] != order
            or any(row.get("exit_code") != 0 or row.get("scratch_removed") is not True for row in runs)):
        raise ValueError("The manifest lacks the six exact successful runs and their cleanup")
    if manifest["builds"] != builds:
        raise ValueError("The manifest build receipts differ from the required source receipts")
    for row in runs:
        inputs = analyzer.load(root / row["run"] / "inputs.json")
        if inputs["binary"] != builds[row["label"]]["binary"]:
            raise ValueError("A measured executable path differs from its build receipt")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--before-revision", required=True)
    parser.add_argument("--after-revision", required=True)
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    if {".lastdb", ".folddb", ".primary-lastdb-archive-20261014"}.intersection(root.parts):
        raise ValueError("Analyze proof receipts outside primary and archived data")
    analyzer, review = original("summarize"), review_analyzer()
    builds = validate_revisions(root, args.before_revision, args.after_revision)
    manifest = validate_manifest(root, builds, analyzer)
    equivalence = command_equivalence(builds)
    runs = {f"{label}-{number}": analyzer.read_run(root, label, number, builds[label])
            for number in analyzer.PAIRS for label in analyzer.LABELS}
    helpers = {str(path): analyzer.digest(path) for path in helper_files()}
    if len(helpers) != 14 or any(value["helpers_sha256"] != helpers for value in runs.values()):
        raise ValueError("All six runs must share the fourteen exact frozen helpers")
    focused = {f"{label}-{number}": review.read_focus(root, label, number)
               for number in analyzer.PAIRS for label in analyzer.LABELS}
    comparison = review.compare_focus(focused)
    summary = {"complete": True, "method": "Three matched pairs of fresh generated homes; one independent run per arm per pair",
               "independent_pairs": 3, "runs_per_variant": 3, "request_samples_pooled": False,
               "automatic_maintenance": False, "flush_ms": 500, "order": manifest["order"], "builds": builds,
               "driver_sha256": analyzer.digest(Path(analyzer.__file__)), "comparisons": analyzer.compare_runs(runs), "runs": runs,
               "peak_process_memory_bytes": analyzer.paired([(runs[f"before-{number}"]["peak_process_memory_bytes"],
                                                              runs[f"after-{number}"]["peak_process_memory_bytes"])
                                                             for number in analyzer.PAIRS]),
               "scope": ["Controlled automatic-maintenance-off product comparison", "Operating system cache uncontrolled",
                         "Darwin process I/O counters are not physical SSD writes", "No significance claim from three pairs",
                         "Net growth differs from new-file bytes and from process I/O", "Compaction includes concurrent probes",
                         "Full generated payload captures remain local under a hashed evidence index",
                         "Committed summaries and compact per-request timing evidence retain payload SHA-256 hashes"],
               "benchmark_round": 2, "focused_compaction": {"comparisons": comparison, "runs": focused},
               "round2_driver_sha256": analyzer.digest(ROOT / "summarize.py"),
               "analysis_driver_sha256": analyzer.digest(Path(__file__)), "build_command_equivalence": equivalence,
               "erase_benchmark": erase_benchmark.summarize(root)}
    save = lambda path, value: path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    save(root / "build-command-equivalence.json", equivalence)
    save(root / "summary.json", summary)
    text = analyzer.report(summary) + review.focus_report(comparison) + erase_benchmark.report(summary["erase_benchmark"])
    text += "\nThe analysis accepts only the exact recorded build argv after the documented wrapper and extra CLI distinction.\n"
    (root / "summary.txt").write_text(text)
    print(json.dumps({"complete": True, "pairs": 3, "matched_phases": 6, "extra_phases_separate": True,
                      "build_command_equivalence": "build-command-equivalence.json", "output": str(root / "summary.json")}))


if __name__ == "__main__":
    main()
