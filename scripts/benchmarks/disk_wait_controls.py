#!/usr/bin/env python3
"""Replay PR #702's archived programs with balanced same-program controls in CI."""

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
from types import SimpleNamespace


BASE = "4898273d73b70c50fa7210d17116f7394dd3c129"
CANDIDATE = "8def278b037d25a73a382c1366ac2822ab16cc3d"
DRIVER = "9041e1521ad552dad274ab41ef896193b5ff0cf3"
SOURCE_RUN = 40814490993
SOURCE_ARTIFACT = 12872265708
SOURCE_DIGEST = "sha256:ec8acf7ec709c5c01a94760a561ab71a015e943aaab298e47ce6f62ad9fb9d76"
ORDERS = (("A1", "B1", "B2", "A2"), ("B1", "A2", "A1", "B2"),
          ("A2", "B2", "B1", "A1"), ("B2", "A1", "A2", "B1"))


def validate(args):
    metadata = json.loads(args.source_metadata.read_text())
    if (metadata["id"] != SOURCE_ARTIFACT or metadata["expired"]
            or metadata["digest"] != SOURCE_DIGEST or metadata["size_in_bytes"] != 50791468
            or metadata["workflow_run"]["id"] != SOURCE_RUN
            or metadata["workflow_run"]["head_sha"] != DRIVER):
        raise ValueError("The original program artifact differs or expired")
    revision = subprocess.check_output(
        ["git", "-C", str(args.driver), "rev-parse", "HEAD"], text=True).strip()
    if revision != DRIVER:
        raise ValueError("The historical measurement driver differs")
    parent = Path(os.environ["BENCH_OUTPUT"]).resolve(strict=True)
    if (args.output.exists() or args.output.is_symlink() or args.output.parent.resolve() != parent
            or parent.parent != Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)):
        raise ValueError("Measurements require a fresh child of the external CI result directory")


def schedule(replica):
    # Each label occupies each position once; every directed neighbour pair occurs once.
    # A1/A2 use one identical executable, as do B1/B2. No rebuild or automatic retry.
    orders = ORDERS if replica == 0 else tuple(tuple(reversed(row)) for row in reversed(ORDERS))
    plan = [{"mode": "ordinary", "block": block, "position": position, "label": label}
            for block, order in enumerate(orders) for position, label in enumerate(order)]
    plan.extend({"mode": "native", "block": 0, "position": position, "label": label}
                for position, label in enumerate(ORDERS[replica]))
    return [{**entry, "directory": f"{number:02d}-{entry['mode']}-{entry['label']}",
             "complete": False} for number, entry in enumerate(plan)]


def investigate(args):
    validate(args)
    sys.path.insert(0, str(args.driver / "scripts/benchmarks"))
    from build import verify
    from node import save
    from run import host_details, interrupted, measure, recipe_for
    signal.signal(signal.SIGTERM, interrupted)
    programs = {label: {**verify(args.programs / revision, revision, os.environ["BUILD_CONTEXT"]),
                        "daemon": str(args.programs / revision / "lastdbd")}
                for label, revision in (("A", BASE), ("B", CANDIDATE))}
    options = SimpleNamespace(profile="standard", pairs=4, native_timing=False,
        growth_only=False, disk_budget_only=False, seed_with_baseline=False,
        minimum_retired=0, baseline_workers=0, candidate_workers=0)
    recipes = {"ordinary": recipe_for(options)}
    options.native_timing, options.pairs = True, 1
    recipes["native"] = recipe_for(options)
    args.output.mkdir()
    manifest = {"complete": False, "controller_revision": os.environ["CONTROLLER_SHA"],
        "driver_revision": DRIVER, "source_run": SOURCE_RUN, "source_artifact": SOURCE_ARTIFACT,
        "replica": args.replica, "host": host_details(), "runner": os.environ["RUNNER_NAME"],
        "programs": programs, "recipes": recipes, "runs": schedule(args.replica),
        "scope": "Fixed two-runner campaign; 16 ordinary runs and four separate native runs per runner",
        "limits": "Generated homes vary; OS pages uncontrolled; native timings are separate diagnostics",
        "primary_cases": ["concurrent-write/1", "concurrent-write/4",
                          "concurrent-mixed/1/write", "concurrent-mixed/4/write"]}
    save(args.output / "controls.json", manifest)
    try:
        for entry in manifest["runs"]:
            program = programs[entry["label"][0]]
            output = args.output / entry["directory"]
            output.mkdir()
            print(json.dumps({"replica": args.replica, **entry}), flush=True)
            entry.update(measure(Path(program["daemon"]), program["revision"],
                                 recipes[entry["mode"]], output, 0))
            save(args.output / "controls.json", manifest)
            if not entry["complete"]:
                raise RuntimeError(f"Incomplete run: {entry['directory']}: {entry['error']}")
        manifest["complete"] = True
    except BaseException as failure:
        manifest["error"] = f"{type(failure).__name__}: {failure}"
        raise
    finally:
        save(args.output / "controls.json", manifest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("driver", "programs", "source-metadata", "output"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--replica", type=int, choices=(0, 1), required=True)
    args = parser.parse_args()
    if (os.environ.get("GITHUB_ACTIONS") != "true"
            or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"):
        parser.error("The disk-wait controls require a GitHub-hosted Actions runner")
    investigate(args)


if __name__ == "__main__":
    main()
