#!/usr/bin/env python3
"""Replay the exact #737 programs on fresh homes with balanced same-binary controls."""

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
from types import SimpleNamespace

BASE = "d87a37c4b0b46c5682a4bc9eebfeb425fe483848"
CANDIDATE = "b74a9f8d53034066b256186dce6fbce1a33736d4"
DRIVER = BASE
ARTIFACT = 12914130316
SOURCE_RUN = 40909174018
DIGEST = "sha256:b219ec0e09770ff6e32d3cc8621bf3ec23cdda880a590635dbb31b1e3f2968c7"
HASHES = {"A": "16dca72582529530819b931737fff9cdb65bb86790aca842fef7117531562a47",
          "B": "157ae4250c7997960731241e9128b74df6e7ec164f5fb9a31d182fcb424b57fc"}
ORDERS = (("A1", "B1", "B2", "A2"), ("B1", "A2", "A1", "B2"),
          ("A2", "B2", "B1", "A1"), ("B2", "A1", "A2", "B1"))


def schedule(replica):
    orders = ORDERS if replica == 0 else tuple(tuple(reversed(row)) for row in reversed(ORDERS))
    plan = [{"mode": "ordinary", "block": block, "position": position, "label": label}
            for block, order in enumerate(orders) for position, label in enumerate(order)]
    plan += [{"mode": "native", "block": 0, "position": position, "label": label}
             for position, label in enumerate(orders[0])]
    return [{**entry, "directory": f"{number:02d}-{entry['mode']}-{entry['label']}",
             "complete": False} for number, entry in enumerate(plan)]


def validate(args):
    source = json.loads(args.source_metadata.read_text())
    if (source["id"] != ARTIFACT or source["expired"] or source["digest"] != DIGEST
            or source["size_in_bytes"] != 53075846 or source["workflow_run"]["id"] != SOURCE_RUN
            or source["workflow_run"]["head_sha"] != DRIVER):
        raise ValueError("The exact archived source programs differ or expired")
    revision = subprocess.check_output(["git", "-C", str(args.driver), "rev-parse", "HEAD"], text=True).strip()
    if revision != DRIVER:
        raise ValueError("The historical benchmark driver differs")
    parent = Path(os.environ["BENCH_OUTPUT"]).resolve(strict=True)
    if (args.output.exists() or args.output.is_symlink() or args.output.parent.resolve() != parent
            or parent.parent != Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)):
        raise ValueError("Campaign requires a fresh child of the external CI output directory")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("driver", "programs", "source-metadata", "output"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--replica", type=int, choices=(0, 1), required=True)
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
        parser.error("Fresh-writer controls require a GitHub-hosted Actions runner")
    validate(args)
    sys.path.insert(0, str(args.driver / "scripts/benchmarks"))
    from build import verify
    from node import save
    from run import host_details
    from write_gate import interrupt, measure_home
    from fresh_writer_io import locations, probe_locations
    programs = {label: verify(args.programs / revision, revision, os.environ["BUILD_CONTEXT"])
                for label, revision in (("A", BASE), ("B", CANDIDATE))}
    if any(programs[label]["sha256"] != HASHES[label] for label in HASHES):
        raise ValueError("An executable differs from the original measured program")
    args.output.mkdir()
    manifest = {"complete": False, "controller_revision": os.environ["CONTROLLER_SHA"],
                "driver_revision": DRIVER, "source_run": SOURCE_RUN, "source_artifact": ARTIFACT,
                "replica": args.replica, "host": host_details(), "programs": programs,
                "locations": locations(), "runs": schedule(args.replica),
                "scope": "Fixed two-runner campaign; 16 ordinary fresh homes and four separate native homes per runner",
                "limits": "Fresh layouts and OS pages vary. Native cases follow their own ordinary pass and restart."}
    save(args.output / "controls.json", manifest)
    signal.signal(signal.SIGTERM, interrupt)
    try:
        for entry in manifest["runs"]:
            output = args.output / entry["directory"]
            output.mkdir()
            options = SimpleNamespace(output=output, baseline=args.programs / BASE / "lastdbd",
                candidate=args.programs / CANDIDATE / "lastdbd", baseline_revision=BASE,
                candidate_revision=CANDIDATE)
            label = "baseline" if entry["label"][0] == "A" else "candidate"
            print(json.dumps({"replica": args.replica, **entry}), flush=True)
            entry["home"] = measure_home(options, label, 2 if entry["mode"] == "native" else 0)
            entry["complete"] = entry["home"]["complete"]
            save(args.output / "controls.json", manifest)
        # No observer or synthetic I/O runs inside the database measurement intervals.
        manifest["io_probes"] = probe_locations(args.replica)
        manifest["complete"] = True
    except BaseException as failure:
        manifest["error"] = f"{type(failure).__name__}: {failure}"
        raise
    finally:
        save(args.output / "controls.json", manifest)


if __name__ == "__main__":
    main()
