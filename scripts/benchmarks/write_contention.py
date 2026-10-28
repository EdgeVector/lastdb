#!/usr/bin/env python3
"""Investigate the exact writer-warning programs using generated CI homes."""

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys


DRIVER = "953a956c7607461947d24ecc11fcc2e21d2cbfaf"
BASE = "d1d3cc98a9a0c163539b825b407585503831e350"
SOURCE_ARTIFACT = 12758648156
SOURCE_RUN = 40558556490
SOURCE_DIGEST = "sha256:57cc6f30b64411ff3580d4d5926a470c76a4afa5d817b346e4387c2f862682e8"


def source_metadata(path):
    value = json.loads(path.read_text())
    if (value["id"] != SOURCE_ARTIFACT or value["expired"]
            or value["digest"] != SOURCE_DIGEST or value["size_in_bytes"] != 47325843
            or value["workflow_run"]["id"] != SOURCE_RUN
            or value["workflow_run"]["head_sha"] != DRIVER):
        raise ValueError("The original program artifact identity differs or expired")


def interrupt(_signum, _frame):
    raise KeyboardInterrupt("The writer investigation received an interruption")


def investigate(args):
    # The old driver owns the original program protocol and generated-home lifecycle.
    sys.path.insert(0, str(args.driver / "scripts/benchmarks"))
    from node import Node, save, file_hash
    from write_contention_workload import Writers, RECORDS
    from write_contention_report import native_events
    from disk_budget import require_search_off
    source_metadata(args.source_metadata)
    if subprocess.check_output(["git", "-C", str(args.driver), "rev-parse", "HEAD"], text=True).strip() != DRIVER:
        raise ValueError("The historical measurement driver differs")
    parent = Path(os.environ["BENCH_OUTPUT"]).resolve(strict=True)
    if (args.output.exists() or args.output.is_symlink() or args.output.parent.resolve() != parent
            or parent.parent != Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)):
        raise ValueError("The investigation requires a fresh child of its external CI result directory")
    args.output.mkdir()
    programs = {"A": args.programs / BASE / "lastdbd", "B": args.programs / DRIVER / "lastdbd"}
    manifest = {"complete": False, "controller_revision": os.environ["CONTROLLER_SHA"],
                "driver_revision": DRIVER, "original_run": SOURCE_RUN,
                "source_artifact": SOURCE_ARTIFACT, "records": RECORDS, "host_cpus": os.cpu_count(),
                "programs": {k: {"path": str(p), "sha256": file_hash(p)} for k, p in programs.items()},
                "homes": [], "scope": "Focused diagnostic; no nightly prehistory replay or regression verdict"}
    plan = [("A", "shared"), ("B", "shared"), ("B", "hashes"), ("A", "hashes"),
            ("A", "schemas"), ("B", "schemas"), ("A", "shared")]
    try:
        for ordinal, (label, layout) in enumerate(plan):
            directory = args.output / f"{ordinal}-{label}-{layout}"
            directory.mkdir()
            node = Node(programs[label], BASE if label == "A" else DRIVER, directory,
                        native_timing=layout != "shared", environment={"LASTDB_SEARCH_ENABLED": "0"})
            home = {"directory": directory.name, "label": label, "layout": layout,
                    "complete": False, "cases": []}
            manifest["homes"].append(home)
            save(args.output / "investigation.json", manifest)
            try:
                node.start()
                require_search_off(node, directory, "before")
                writes = Writers(node, layout)
                writes.seed()
                if layout == "shared":
                    home["cases"].append(writes.measure("ordinary-16", 16))
                    node.stop()
                    node.native_timing = True
                    node.start()
                    writes.verify()
                    home["cases"].append(writes.measure("native-1", 1, requests=256))
                home["cases"].append(writes.measure("native-16-first", 16))
                home["cases"].append(writes.measure("native-16-repeat", 16))
                if layout == "shared":
                    node.stop()
                    node.workers = 16
                    node.start()
                    writes.verify()
                    home["cases"].append(writes.measure("workers-16", 16))
                    node.stop()
                    node.workers = 0
                    node.start()
                    writes.verify()
                    home["cases"].append(writes.measure("batch-16", 16, batch=16, requests=128))
                require_search_off(node, directory, "after")
                if node.call("/api/db/native-status")[0]["native_disk"].get("writer_failed") is not False:
                    raise ValueError("The native writer is unhealthy")
            finally:
                node.close()
            native_events(directory, home["cases"])
            home["complete"] = True
            save(args.output / "investigation.json", manifest)
        manifest["complete"] = True
    except BaseException as error:
        manifest["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        save(args.output / "investigation.json", manifest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("driver", "programs", "output", "source-metadata"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
        parser.error("The writer investigation requires a GitHub-hosted Actions runner")
    signal.signal(signal.SIGTERM, interrupt)
    investigate(args)


if __name__ == "__main__":
    main()
