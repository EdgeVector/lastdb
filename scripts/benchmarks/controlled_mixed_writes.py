#!/usr/bin/env python3
"""CI-only mixed writes: independent homes versus one verified stopped history."""

import argparse
import json
import os
from pathlib import Path
import re
import shutil

from cold_layout import stable_boundary
from cold_shared_seed import boot_boundary, inventory, physical_state
from mixed_layout_support import CASE, RECIPE, Trace, audit, observe, precondition, warm
from node import Node, file_hash, save
from report import compare
from workload import Workload

PAIRS = 4
MODES = ("independent-timing", "shared-timing", "shared-trace")


def seed_template(node, output):
    try:
        node.start()
        precondition(node, output / "precondition")
        stable = stable_boundary(node)
        save(output / "observed-stable.json", stable)
    finally:
        node.stop()
    whole, physical = inventory(node.home), inventory(node.home, physical_only=True)
    save(output / "inventory.json", whole)
    save(output / "physical-inventory.json", physical)
    return whole, physical, {name: stable[-1][name] + 2 for name in ("generation", "frontier")}


def prepare(node, mode, template, whole, physical, expected_root):
    boot = None
    if mode == "independent-timing":
        node.start()
        precondition(node, node.output / "precondition")
    else:
        if template.process is not None or any(node.home.iterdir()):
            raise RuntimeError("Shared input requires a stopped source and empty owned destination")
        shutil.copytree(template.home, node.home, dirs_exist_ok=True)
        if inventory(node.home) != whole:
            raise RuntimeError("The complete copied home differs from the shared seed")
        save(node.output / "clone.json", {"byte_identical": True, "files": len(whole)})
        node.start()
        boot, layout = boot_boundary(node, physical)
        if boot != expected_root:
            raise RuntimeError("Unexpected shutdown/boot publications in a shared clone")
    if node.disk_format() != 2:
        raise RuntimeError("Comparison requires disk format v2")
    logical = warm(node)
    if mode != "independent-timing":
        save(node.output / "settle.json", stable_boundary(node))
        physical_state(node, layout, boot)
    # Warm-up and the physical observation stay outside the timed case.
    save(node.output / "before.json", observe(node))
    return logical


def measure(binary, revision, output, mode, template, whole, physical, expected_root):
    output.mkdir()
    node = Node(binary, revision, output)
    work, tracer, error, identities, logical = None, None, None, None, None
    try:
        logical = prepare(node, mode, template, whole, physical, expected_root)
        work = Workload(node, RECIPE, output)
        if mode == "shared-trace":
            tracer = Trace(node, output)
            tracer.start()
        try:
            work.measure(CASE, "mixed", clients=4)
        finally:
            if tracer:
                tracer.finish()
        work.raw.close()
        save(output / "after.json", observe(node))
        identities = audit(node, output, tracer)
        native = node.call("/api/db/native-status")[0]
        save(output / "native-status.json", native)
        if native["native_disk"].get("writer_failed") is not False:
            raise RuntimeError("The native writer is unhealthy")
    except BaseException as failure:
        error = f"{type(failure).__name__}: {failure}"
    finally:
        if work:
            work.raw.close()
        try:
            node.close()
        except BaseException as failure:
            error = f"{error or ''}; cleanup: {type(failure).__name__}: {failure}"
    result = {"complete": error is None, "error": error, "mode": mode,
              "logical_input_sha256": logical}
    save(output / "result.json", result)
    if error:
        raise RuntimeError(error)
    return result, identities


def summaries(output, manifest):
    lines = ["## Controlled mixed/4 writes", "", manifest["scope"], "",
             "| Arm | Baseline write P95 ms | Candidate write P95 ms | Change | Pair changes |",
             "|---|---:|---:|---:|---|"]
    for mode in MODES:
        runs = [run for run in manifest["runs"] if run["mode"] == mode]
        comparison = compare(output, {**manifest, "runs": runs})
        save(output / f"comparison-{mode}.json", comparison)
        item = comparison[f"{CASE}/write"]
        pairs = ", ".join(f"{v:+.1f}%" for v in item["paired_p95_changes_percent"])
        lines.append(f"| {mode} | {item['baseline']['p95_ms']:.3f} | "
                     f"{item['candidate']['p95_ms']:.3f} | "
                     f"{item['changes_percent']['p95_ms']:+.1f}% | {pairs} |")
    lines += ["", "Each arm retains 4 alternating pairs, 256 calls/run (128 reads/128 writes).",
              "Independent timing replays its own preceding standard history without a stability wait.",
              "Shared timing verifies identical stopped files and settles/warms clones before timing.",
              "Shared trace is intrusive and diagnostic only; its P95 must not replace timing results.",
              "Writes can legitimately change layout. Observations are not atomic and can miss short merges.",
              "OS pages are warmed by verification; new mutation IDs and physical output are not identical."]
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
        stream.write("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for label in ("baseline", "candidate"):
        parser.add_argument(f"--{label}-binary", type=Path, required=True)
        parser.add_argument(f"--{label}-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    revisions = {label: getattr(args, f"{label}_revision") for label in ("baseline", "candidate")}
    if os.environ.get("GITHUB_ACTIONS") != "true" or any(
            not re.fullmatch(r"[0-9a-f]{40}", revision) for revision in revisions.values()):
        raise RuntimeError("Run only in GitHub Actions with full product revision SHAs")
    binaries = {label: getattr(args, f"{label}_binary").resolve() for label in revisions}
    args.output.mkdir(parents=True, exist_ok=True)
    manifest = {"complete": False, "recipe": {"pairs": PAIRS}, "runs": [],
                "driver_revision": os.environ["DRIVER_SHA"],
                "binaries": {label: {"revision": revisions[label], "sha256": file_hash(binary)}
                             for label, binary in binaries.items()},
                "scope": "Same exact programs and keyed mixed/4 recipe on one hosted machine. "
                         "Compare independent standard histories with byte-identical clones of one "
                         "stopped baseline history, then trace separate clones. This controls starting "
                         "files and logical values, not thread schedules, device state or new output bytes."}
    save(args.output / "benchmark.json", manifest)
    seed_output = args.output / "seed"
    seed_output.mkdir()
    template = Node(binaries["baseline"], revisions["baseline"], seed_output)
    reference, logical = None, None
    try:
        whole, physical, expected_root = seed_template(template, seed_output)
        manifest["seed_inventory_sha256"] = file_hash(seed_output / "inventory.json")
        manifest["expected_boot_root"] = expected_root
        for pair in range(PAIRS):
            # Rotate arm order and reverse version order every pair.
            for mode in MODES[pair % 3:] + MODES[:pair % 3]:
                for label in (("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")):
                    directory = f"{mode}-{pair}-{label}"
                    result, identities = measure(binaries[label], revisions[label], args.output / directory,
                                                 mode, template, whole, physical, expected_root)
                    if reference is not None and (identities != reference or result["logical_input_sha256"] != logical):
                        raise RuntimeError("Target requests or logical starting values differ between arms")
                    reference, logical = identities, result["logical_input_sha256"]
                    manifest["runs"].append({"pair": pair, "label": label, "directory": directory, **result})
                    save(args.output / "benchmark.json", manifest)
        if inventory(template.home) != whole:
            raise RuntimeError("The stopped seed was modified")
        summaries(args.output, manifest)
    finally:
        template.close()
    manifest["complete"] = True
    save(args.output / "benchmark.json", manifest)


if __name__ == "__main__":
    main()
