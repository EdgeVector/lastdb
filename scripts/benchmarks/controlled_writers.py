#!/usr/bin/env python3
"""CI-only 16 writers from identical stopped, generated 100,000-record files."""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import signal
from statistics import median

from cold_layout import stable_boundary
from cold_shared_seed import boot_boundary, inventory, physical_state
from disk_budget import require_search_off
from node import Node, file_hash, save
from run import host_details
from write_contention_workload import RECORDS, Writers
from write_gate import diagnostics, interrupt


def seed(node):
    try:
        node.start()
        require_search_off(node, node.output, "before")
        Writers(node, "shared").seed()
        save(node.output / "observed-stable.json", stable_boundary(node))
    finally:
        node.stop()
    whole, physical = inventory(node.home), inventory(node.home, physical_only=True)
    save(node.output / "inventory.json", whole)
    save(node.output / "physical-inventory.json", physical)
    return whole, physical


def measure(args, label, pair, native, template, whole, physical):
    name = "native-16" if native else "ordinary-16"
    directory = args.output / f"{name}-{pair}-{label}"
    directory.mkdir()
    node = Node(getattr(args, f"{label}_binary"), getattr(args, f"{label}_revision"), directory,
                native_timing=native, environment={"LASTDB_SEARCH_ENABLED": "0",
                "LASTDBD_STDIO_LOG_ROTATION": "0" if native else "1"})
    try:
        if template.process is not None or any(node.home.iterdir()):
            raise RuntimeError("Shared input requires a stopped source and empty owned destination")
        shutil.copytree(template.home, node.home, dirs_exist_ok=True)
        if inventory(node.home) != whole:
            raise RuntimeError("Copied writer input differs from the complete stopped seed")
        save(directory / "clone.json", {"byte_identical": True, "files": len(whole)})
        node.start()
        root, layout = boot_boundary(node, physical)
        delta = json.loads((directory / "boot-delta.json").read_text())
        append = sum(entry["bytes"] for entry in delta["added"].values())
        writes = Writers(node, "shared")
        writes.verify()
        save(directory / "settle.json", stable_boundary(node))
        physical_state(node, layout, root)
        case = writes.measure(name, 16)
        require_search_off(node, directory, "after")
        if node.call("/api/db/native-status")[0]["native_disk"].get("writer_failed") is not False:
            raise RuntimeError("The native writer is unhealthy")
    finally:
        node.close()
    diagnostics(directory, [case])
    rows = json.loads((directory / f"{name}-samples.json").read_text())
    identities = sorted((r["client"], r["request_id"], r["request_sha256"]) for r in rows)
    if len(identities) != 2048 or len({r[1] for r in identities}) != 2048:
        raise RuntimeError("The controlled writer case lacks all 2,048 unique calls")
    return {"label": label, "pair": pair, "directory": directory.name, "complete": True,
            "root": root, "boot_append_bytes": append, "case": case}, identities


def summary(output, manifest):
    lines = ["# Controlled 16-writer comparison", "", manifest["scope"], "",
             f"Driver: `{manifest['driver_revision']}`. Corpus: {RECORDS:,} generated records.",
             "Each client owns different keys in one schema and one hash. Search is off.",
             "Every clone starts from identical stopped files, then permits one bounded startup append.",
             "Each case completes 2,048 durable single-record calls with identical per-client requests.",
             "All final values and clean shutdowns are verified. Native cases use separate seed clones.", "",
             "| Pair / arm | Baseline P95 ms | Candidate P95 ms | P95 change | Base records/s | Candidate records/s |",
             "|---|---:|---:|---:|---:|---:|"]
    medians = []
    for native in (False, True):
        runs = [r for r in manifest["runs"] if r["case"]["native_timing"] == native]
        for pair in sorted({r["pair"] for r in runs}):
            values = {r["label"]: r["case"]["metrics"] for r in runs if r["pair"] == pair}
            a, b = values["baseline"], values["candidate"]
            lines.append(f"| {pair} / {'native' if native else 'ordinary'} | {a['p95_ms']:.3f} | "
                         f"{b['p95_ms']:.3f} | {(b['p95_ms']/a['p95_ms']-1)*100:+.1f}% | "
                         f"{a['records_per_second']:.2f} | {b['records_per_second']:.2f} |")
        if not native:
            for field in ("p95_ms", "records_per_second"):
                a, b = (median(r["case"]["metrics"][field] for r in runs if r["label"] == label)
                        for label in ("baseline", "candidate"))
                medians.append(f"Ordinary median {field}: {a:.3f} → {b:.3f} ({(b/a-1)*100:+.1f}%).")
    lines += ["", *medians, "", "Three alternating ordinary pairs; one separate native pair. Percentiles are not pooled.",
              "The seed uses the baseline program. This arm isolates a shared starting file history; "
              "it does not replace the independent fresh-home comparison. Speed remains advisory.",
              "Nested native preparation stages overlap. Storage sequences do not identify HTTP requests."]
    text = "\n".join(lines) + "\n"
    (output / "summary.md").write_text(text)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
        stream.write(text)
    print(text)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for label in ("baseline", "candidate"):
        parser.add_argument(f"--{label}-binary", type=Path, required=True)
        parser.add_argument(f"--{label}-revision", required=True)
    parser.add_argument("--driver-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
        parser.error("Controlled writers require a GitHub-hosted Actions runner")
    if any(not re.fullmatch(r"[0-9a-f]{40}", value) for value in
           (args.baseline_revision, args.candidate_revision, args.driver_revision)):
        parser.error("Every source revision must be a complete SHA")
    if args.output.resolve() != Path(os.environ["RUNNER_TEMP"]).resolve() / "controlled-writers":
        parser.error("Output must be the owned external CI result directory")
    args.output.mkdir(exist_ok=True)
    seed_output = args.output / "seed"
    seed_output.mkdir()
    template = Node(args.baseline_binary, args.baseline_revision, seed_output,
                    environment={"LASTDB_SEARCH_ENABLED": "0", "LASTDBD_STDIO_LOG_ROTATION": "1"})
    manifest = {"complete": False, "driver_revision": args.driver_revision, "host": host_details(),
                "records": RECORDS, "clients": 16, "runs": [], "programs": {
                    label: {"revision": getattr(args, f"{label}_revision"),
                            "sha256": file_hash(getattr(args, f"{label}_binary"))}
                    for label in ("baseline", "candidate")},
                "scope": "Identical stopped generated files; observed stable boundaries; product defaults."}
    identities, boundaries = {}, []
    signal.signal(signal.SIGTERM, interrupt)
    try:
        whole, physical = seed(template)
        for native, pairs in ((False, range(3)), (True, range(1))):
            for pair in pairs:
                order = ("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")
                for label in order:
                    print(json.dumps({"native": native, "pair": pair, "version": label}), flush=True)
                    run, actual = measure(args, label, pair, native, template, whole, physical)
                    expected = identities.setdefault(native, actual)
                    if actual != expected:
                        raise RuntimeError("Controlled cases differ in their per-client request sequences")
                    boundaries.append((run["root"], run["boot_append_bytes"]))
                    if boundaries[-1] != boundaries[0]:
                        raise RuntimeError("Shared reopened roots or startup append sizes differ")
                    manifest["runs"].append(run)
                    save(args.output / "controlled-writers.json", manifest)
        if len(manifest["runs"]) != 8:
            raise RuntimeError("The controlled comparison lacks its eight complete cases")
        manifest["complete"] = True
        summary(args.output, manifest)
    except BaseException as error:
        manifest["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        try:
            template.close()
        except BaseException as error:
            manifest["complete"] = False
            manifest["error"] = f"cleanup: {type(error).__name__}: {error}"
            raise
        finally:
            save(args.output / "controlled-writers.json", manifest)


if __name__ == "__main__":
    main()
