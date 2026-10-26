#!/usr/bin/env python3
"""Investigation-only concurrent reads from one fixed post-write generated layout."""

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import time

from cold_layout import snapshot, stable_boundary
from node import Node, file_hash, save
from report import compare
from workload import PROFILES, Workload, read_request, validate_read

PAIRS = 4
CASES = {"concurrent-read/1", "concurrent-read/32"}
COSTS = ("requested_keys", "read_operations", "file_opens", "footer_bytes", "payload_bytes",
         "decoded_bytes", "logical_warm_hits")


def inventory(home, physical_only=False):
    """Hash only an owned, stopped template or a clone; never follow links."""
    files, entries = {}, 0
    started = time.monotonic()
    for parent, directories, names in os.walk(home, followlinks=False):
        for name in directories + names:
            path = Path(parent) / name
            entries += 1
            if entries > 100_000 or time.monotonic() - started > 30:
                raise RuntimeError("Generated-home inventory exceeded its bound")
            mode = path.lstat().st_mode
            if not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
                raise RuntimeError("Generated snapshot contains a link or nonregular entry")
            if stat.S_ISREG(mode) and (not physical_only or name == "CURRENT" or name.endswith(".ldrun")):
                files[str(path.relative_to(home))] = {"bytes": path.stat().st_size,
                                                       "sha256": file_hash(path)}
    if not files or physical_only and not any(Path(p).name == "CURRENT" for p in files):
        raise RuntimeError("The generated snapshot lacks its physical manifests")
    return files


def physical_state(node, expected_layout, expected_root=None):
    state = snapshot(node)
    root = {name: state[name] for name in ("generation", "frontier")}
    observed = inventory(node.home, physical_only=True)
    save(node.output / "last-physical-observation.json", {"root": root, "files": observed})
    if observed != expected_layout:
        raise RuntimeError("The clone's run files or CURRENT changed during the controlled boundary")
    if expected_root is not None and root != expected_root:
        raise RuntimeError("The clone changed its generation or write frontier")
    return root


def boot_boundary(node, seed_layout):
    """Allow only the startup stamp append; freeze the resulting layout for timing."""
    state = snapshot(node)
    observed = inventory(node.home, physical_only=True)
    removed = sorted(seed_layout.keys() - observed.keys())
    added = {k: observed[k] for k in observed.keys() - seed_layout.keys()}
    changed = sorted(k for k in seed_layout.keys() & observed.keys() if seed_layout[k] != observed[k])
    root = {name: state[name] for name in ("generation", "frontier")}
    save(node.output / "boot-delta.json", {"removed": removed, "added": added,
                                          "changed": changed, "root": root, "files": observed})
    # hydrate_from_snapshot writes one keep-small header to re-arm clean_stop.
    # A small append alone does not authenticate its contents; retain this explicit
    # residual startup difference and audit exact query work across the versions.
    if (removed or changed != ["data/disk-run/CURRENT"] or len(added) != 1
            or any(not re.fullmatch(r"data/disk-run/runs/[0-9a-f-]+\.ldrun", k)
                   or not 0 < v["bytes"] <= 4096 for k, v in added.items())):
        raise RuntimeError("Startup changed more than one bounded metadata append; shared layout refused")
    return root, observed


def prepare_history(work):
    """Replay the standard recipe up to, but not including, concurrent-read/32."""
    work.seed()
    work.cold_next = 0
    work.measure("cold-read/1", "cold")
    work.measure("cold-read/32", "cold", batch=32)
    hot = list(range(work.recipe["records"] - 256, work.recipe["records"]))
    for start in range(0, len(hot), 64):
        numbers = hot[start:start + 64]
        result, _, _ = work.node.call("/api/query", read_request(numbers))
        validate_read(result, numbers)
    work.measure("hot-read/1", "hot")
    work.measure("hot-read/32", "hot", batch=32)
    for mode in ("insert", "update"):
        for batch in (1, 128):
            count = (work.recipe["single_write_samples"] if batch == 1 else
                     work.recipe["batch_update_samples"] if mode == "update" else
                     work.recipe["write_samples"])
            work.measure(f"{mode}/{batch}", mode, batch=batch, samples=count)
    for clients in (1, 4, 16):
        for mode in ("read", "write", "mixed"):
            work.measure(f"concurrent-{mode}/{clients}", mode, clients=clients)


def prepare_seed(node, output):
    work = None
    try:
        node.start()
        if node.disk_format() != 2:
            raise RuntimeError("The shared seed must use disk format v2")
        work = Workload(node, {**PROFILES["standard"], "native_timing": False}, output)
        prepare_history(work)
        save(output / "observed-stable.json", stable_boundary(node))
    finally:
        if work:
            work.raw.close()
        node.stop()  # Requires the normal successful durable shutdown receipt.
    whole, physical = inventory(node.home), inventory(node.home, physical_only=True)
    save(output / "inventory.json", whole)
    save(output / "physical-inventory.json", physical)
    return whole, physical


def audit_samples(output):
    samples = [json.loads(line) for line in gzip.decompress((output / "samples.jsonl.gz").read_bytes()).splitlines()]
    if len(samples) != 512 or {r["case"] for r in samples} != CASES:
        raise RuntimeError("The controlled concurrency cases have incomplete samples")
    for case in CASES:
        if sum(r["case"] == case for r in samples) != 256:
            raise RuntimeError("A controlled concurrency case lacks its 256 calls")
        clients = int(case.rsplit("/", 1)[1])
        selected = [r for r in samples if r["case"] == case]
        if {r["client"] for r in selected} != set(range(clients)) or any(
                sorted(r["client_request"] for r in selected if r["client"] == client)
                != list(range(256 // clients)) for client in range(clients)):
            raise RuntimeError("The controlled case lacks complete client sequences")
    for row in samples:
        cost = row.get("read_receipt")
        if ("error" in row or not cost or cost["read_operations"] <= 0
                or cost["payload_bytes"] <= 0 or cost["logical_warm_hits"] != 0):
            raise RuntimeError("A controlled cold read lacks a cold, successful disk payload receipt")
    return [{"case": r["case"], "client": r["client"], "request": r["client_request"],
             "sha256": r["request_sha256"],
             "costs": {name: r["read_receipt"].get(name) for name in COSTS}} for r in samples]


def measure(binary, revision, output, template, whole, physical):
    output.mkdir()
    node = Node(binary, revision, output, environment={"LASTDB_DISK_FORMAT": "2"})
    work, error, samples, root = None, None, None, None
    try:
        # The template's daemon is stopped; node owns its separate destination root.
        if template.process is not None or any(node.home.iterdir()):
            raise RuntimeError("Clone requires a stopped template and an empty owned destination")
        shutil.copytree(template.home, node.home, dirs_exist_ok=True)
        if inventory(node.home) != whole:
            raise RuntimeError("The complete copied home is not byte-identical to the seed")
        save(output / "clone.json", {"byte_identical": True, "files": len(whole)})
        node.start()
        if node.disk_format() != 2:
            raise RuntimeError("A product cannot open the shared disk format")
        root, timing_layout = boot_boundary(node, physical)
        save(output / "root.json", root)
        work = Workload(node, {**PROFILES["standard"], "native_timing": False}, output)
        for clients in (1, 32):
            boundary = node.call("/api/admin/shed", {})[0]["shed"]
            save(output / f"boundary-{clients}.json", boundary)
            if (boundary.get("error") or boundary.get("target_met") is not True
                    or boundary["native_memory"]["logical_records"] != 0):
                raise RuntimeError("The clone did not establish an empty logical warm set")
            physical_state(node, timing_layout, root)
            work.measure(f"concurrent-read/{clients}", "read", clients=clients)
            physical_state(node, timing_layout, root)
        work.raw.close()
        samples = audit_samples(output)
    except BaseException as failure:
        error = f"{type(failure).__name__}: {failure}"
    finally:
        if work:
            work.raw.close()
        try:
            node.close()
        except BaseException as failure:
            error = f"{error or ''}; cleanup: {type(failure).__name__}: {failure}"
    result = {"complete": error is None, "error": error, "root": root}
    save(output / "result.json", result)
    if error:
        raise RuntimeError(error)
    return result, samples


def retain_comparison(output, manifest, samples):
    requests = [[r["case"], r["client"], r["request"], r["sha256"]]
                for r in samples["0-baseline"]]
    mismatches = []
    for run in manifest["runs"]:
        rows = samples[run["directory"]]
        if [[r["case"], r["client"], r["request"], r["sha256"]] for r in rows] != requests:
            raise RuntimeError("The clones did not receive identical request sequences")
        if run["label"] == "candidate":
            base = samples[f"{run['pair']}-baseline"]
            differences = [{"case": a["case"], "client": a["client"], "request": a["request"],
                            "baseline": a["costs"], "candidate": b["costs"]}
                           for a, b in zip(base, rows) if a["costs"] != b["costs"]]
            mismatches.append({"pair": run["pair"], "count": len(differences), "requests": differences})
    save(output / "work-differences.json", mismatches)
    comparison = compare(output, manifest)
    save(output / "comparison.json", comparison)
    return comparison


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for label in ("baseline", "candidate"):
        parser.add_argument(f"--{label}-binary", type=Path, required=True)
        parser.add_argument(f"--{label}-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    revisions = {label: getattr(args, f"{label}_revision") for label in ("baseline", "candidate")}
    if os.environ.get("GITHUB_ACTIONS") != "true" or any(
            not re.fullmatch(r"[0-9a-f]{40}", value) for value in revisions.values()):
        raise RuntimeError("Run only in GitHub Actions with full product revision SHAs")
    binaries = {label: getattr(args, f"{label}_binary").resolve() for label in revisions}
    args.output.mkdir(parents=True, exist_ok=True)
    manifest = {"complete": False, "recipe": {"pairs": PAIRS}, "runs": [],
                "driver_revision": os.environ["DRIVER_SHA"],
                "binaries": {label: {"revision": revisions[label], "sha256": file_hash(binary)}
                             for label, binary in binaries.items()},
                "scope": "One stopped generated seed; four alternating pairs of independent byte-identical "
                         "clones. Both formats must be v2. One bounded boot metadata append is recorded; "
                         "all inherited run bytes stay identical and post-boot layout is fixed during timing. "
                         "All opening samples retained. "
                         "Content verification reads files before timing: filesystem pages are not cold. "
                         "This isolates 1/32-client reads on a fixed post-write layout, not the original "
                         "history-dependent overlap with maintenance or physical-device cold latency."}
    save(args.output / "benchmark.json", manifest)
    seed_output = args.output / "seed"
    seed_output.mkdir()
    template = Node(binaries["baseline"], revisions["baseline"], seed_output,
                    environment={"LASTDB_DISK_FORMAT": "2"})
    samples, comparison = {}, None
    try:
        whole, physical = prepare_seed(template, seed_output)
        manifest["seed_inventory_sha256"] = file_hash(seed_output / "inventory.json")
        manifest["physical_inventory_sha256"] = file_hash(seed_output / "physical-inventory.json")
        for pair in range(PAIRS):
            for label in (("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")):
                directory = f"{pair}-{label}"
                result, samples[directory] = measure(binaries[label], revisions[label], args.output / directory,
                                                      template, whole, physical)
                manifest["runs"].append({"pair": pair, "label": label, "directory": directory, **result})
                save(args.output / "benchmark.json", manifest)
        roots = [r["root"] for r in manifest["runs"]]
        if any(root != roots[0] for root in roots) or inventory(template.home) != whole:
            raise RuntimeError("Runs did not hold one shared root, or the stopped seed was modified")
        seed_live = json.loads((seed_output / "observed-stable.json").read_text())[-1]
        # One clean header commit on shutdown, one re-arm header commit on boot.
        expected_root = {name: seed_live[name] + 2 for name in ("generation", "frontier")}
        if roots[0] != expected_root:
            raise RuntimeError("The shared root includes unexpected shutdown/startup publications")
        manifest["expected_boot_root"] = expected_root
        boot_sizes = [list(json.loads((args.output / r["directory"] / "boot-delta.json").read_text())["added"].values())[0]["bytes"]
                      for r in manifest["runs"]]
        if len(set(boot_sizes)) != 1:
            raise RuntimeError("The versions appended different sizes of boot metadata")
        manifest["boot_append_bytes"] = boot_sizes[0]
        comparison = retain_comparison(args.output, manifest, samples)
    finally:
        template.close()
    manifest["complete"] = True
    save(args.output / "benchmark.json", manifest)
    lines = ["## Fixed post-write layout: envelope concurrency investigation", "", manifest["scope"], "",
             f"Baseline: `{revisions['baseline']}`  ", f"Candidate: `{revisions['candidate']}`", "",
             "| Case | Base P95 ms | Candidate P95 ms | Change | Pair changes |", "|---|---:|---:|---:|---|"]
    for case, item in comparison.items():
        pairs = ", ".join(f"{value:+.1f}%" for value in item["paired_p95_changes_percent"])
        lines.append(f"| {case} | {item['baseline']['p95_ms']:.3f} | {item['candidate']['p95_ms']:.3f} | "
                     f"{item['changes_percent']['p95_ms']:+.1f}% | {pairs} |")
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
        stream.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
