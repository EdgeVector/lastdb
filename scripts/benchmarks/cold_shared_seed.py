#!/usr/bin/env python3
"""Temporary CI driver: measure the single-mutation endpoint, not the batch route."""

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

from node import Node, file_hash, save
from report import compare
from workload import HASH, PROFILES, SCHEMA, Workload, describe, encoded, fields, key, read_request

PAIRS, SAMPLES, SEED_RECORDS = 4, 256, 256
CASES = (("create/1024", "create", 1024, 256),
         ("update/1024", "update", 1024, 0),
         ("create/32768", "create", 32768, 512),
         ("update/32768", "update", 32768, 0),
         ("delete-prefix/one-key", "delete", 0, 0))


def inventory(home):
    """Hash this owned, stopped generated seed or clone within a fixed bound."""
    files, entries, started = {}, 0, time.monotonic()
    for parent, directories, names in os.walk(home, followlinks=False):
        for name in directories + names:
            path = Path(parent) / name
            entries += 1
            if entries > 100_000 or time.monotonic() - started > 30:
                raise RuntimeError("Generated-home inventory exceeded its bound")
            mode = path.lstat().st_mode
            if not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
                raise RuntimeError("The generated home contains a nonregular entry")
            if stat.S_ISREG(mode):
                files[str(path.relative_to(home))] = {"bytes": path.stat().st_size,
                                                       "sha256": file_hash(path)}
    if not files:
        raise RuntimeError("The generated seed is empty")
    return files


def request(number, case, mode, size):
    value = fields(number, case)
    value["body"] = hashlib.shake_256(value["title"].encode()).hexdigest(size // 2)
    selected = value if mode == "create" else {name: value[name] for name in ("title", "body")}
    body = {"type": "mutation", "schema": SCHEMA, "mutation_type": mode,
            "durability": "durable", "fields_and_values": selected,
            "key_value": {"hash": HASH, "range": key(number)}}
    if mode == "delete":
        body.update(fields_and_values={}, key_value={"hash": HASH}, key_range_prefix=key(number))
    return encoded(body), value


def verify_values(node, numbers, expected, deleted):
    """Read known keys in bounded batches after the timed writes."""
    for start in range(0, len(numbers), 64):
        selected = numbers[start:start + 64]
        result, _, _ = node.call("/api/query", read_request(selected))
        rows = result.get("results", [])
        if deleted:
            if rows or result.get("returned_count") != 0:
                raise RuntimeError("A committed prefix delete still returns a record")
            continue
        if len(rows) != len(selected) or result.get("returned_count") != len(selected):
            raise RuntimeError("The read-back lacks committed records")
        wanted, found = {key(n) for n in selected}, set()
        for row in rows:
            coordinate = row.get("key", {})
            name = coordinate.get("range")
            if (coordinate.get("hash") != HASH or name not in wanted
                    or name in found or row.get("fields") != expected[name]):
                raise RuntimeError("The read-back differs from the committed generated values")
            found.add(name)


def measure_cases(node, output):
    metrics, identities, mutation_ids = {}, [], set()
    with gzip.open(output / "samples.jsonl.gz", "wt") as raw:
        for case, mode, size, first in CASES:
            samples, expected = [], {}
            numbers = list(range(first, first + SAMPLES))
            started = time.perf_counter()
            for index, number in enumerate(numbers):
                body, value = request(number, case, mode, size)
                request_id = f"single-{len(metrics)}-{index:06d}"
                result, elapsed, _ = node.call("/api/mutation", body, request_id=request_id)
                mutation_id = result.get("mutation_id")
                if (result.get("success") is not True or result.get("durability") != "durable"
                        or not isinstance(mutation_id, str) or not mutation_id
                        or mutation_id in mutation_ids or result.get("revision") is None
                        or not isinstance(result.get("resident_stages_us"), dict)):
                    raise RuntimeError("A single mutation lacks its unique durable commit receipt")
                mutation_ids.add(mutation_id)
                expected[key(number)] = value
                digest = hashlib.sha256(body).hexdigest()
                sample = {"case": case, "kind": "write", "records": 1, "client": 0,
                          "client_request": index, "request_id": request_id,
                          "request_sha256": digest, "elapsed_ms": elapsed,
                          "resident_stages_us": result["resident_stages_us"]}
                samples.append(sample)
                identities.append([case, index, digest])
                raw.write(json.dumps(sample) + "\n")
            elapsed = time.perf_counter() - started
            verify_values(node, numbers, expected, mode == "delete")
            metrics[case] = {**describe(samples, elapsed), "verified_records": len(numbers)}
            save(output / "metrics.json", metrics)
            print(json.dumps({"case": case, **metrics[case]}), flush=True)
    return identities


def prepare_seed(node, output):
    work = None
    try:
        node.start()
        if node.disk_format() != 2:
            raise RuntimeError("The generated seed must use disk format v2")
        work = Workload(node, {**PROFILES["standard"], "records": SEED_RECORDS,
                               "native_timing": False}, output)
        work.seed()
    finally:
        if work:
            work.raw.close()
        node.stop()
    files = inventory(node.home)
    save(output / "inventory.json", files)
    return files


def measure(binary, revision, output, template, files):
    output.mkdir()
    node = Node(binary, revision, output, environment={"LASTDB_DISK_FORMAT": "2"})
    result, identities = {"complete": False, "error": None}, None
    try:
        if template.process is not None or any(node.home.iterdir()):
            raise RuntimeError("Cloning requires a stopped seed and an empty owned destination")
        shutil.copytree(template.home, node.home, dirs_exist_ok=True)
        if inventory(node.home) != files:
            raise RuntimeError("The copied generated home differs from the stopped seed")
        save(output / "clone.json", {"byte_identical": True, "files": len(files)})
        node.start()
        if node.disk_format() != 2:
            raise RuntimeError("A measured program did not open disk format v2")
        identities = measure_cases(node, output)
        result["complete"] = True
    except BaseException as failure:
        result.update(complete=False, error=f"{type(failure).__name__}: {failure}")
    finally:
        try:
            node.close()
        except BaseException as failure:
            result.update(complete=False, error=f"{result['error'] or ''}; cleanup: {failure}")
        save(output / "result.json", result)
    if not result["complete"]:
        raise RuntimeError(result["error"])
    return result, identities


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
        raise RuntimeError("Run only in GitHub Actions with exact program revisions")
    binaries = {label: getattr(args, f"{label}_binary").resolve() for label in revisions}
    args.output.mkdir(parents=True, exist_ok=True)
    manifest = {"complete": False, "runs": [], "driver_revision": os.environ["DRIVER_SHA"],
                "binaries": {label: {"revision": revisions[label], "sha256": file_hash(binary)}
                             for label, binary in binaries.items()},
                "recipe": {"pairs": PAIRS, "samples": SAMPLES, "seed_records": SEED_RECORDS,
                           "endpoint": "/api/mutation", "cases": [list(c) for c in CASES]},
                "scope": "Direct single-mutation RPC through response bytes. Payload construction, "
                         "decode, receipt validation and read-back are outside each RPC timer. "
                         "Rate includes client preparation, receipt checks and sample recording. Each run starts from "
                         "an independent byte-identical copy of one stopped generated seed. Writes "
                         "change physical layout; maintenance, OS cache and scheduling are uncontrolled. "
                         "No primary data, aggregate/CAS or multi-key-prefix coverage, total-memory or power-loss claim."}
    save(args.output / "benchmark.json", manifest)
    seed_output = args.output / "seed"
    seed_output.mkdir()
    template = Node(binaries["baseline"], revisions["baseline"], seed_output,
                    environment={"LASTDB_DISK_FORMAT": "2"})
    expected, comparison = None, None
    try:
        files = prepare_seed(template, seed_output)
        manifest["seed_inventory_sha256"] = file_hash(seed_output / "inventory.json")
        for pair in range(PAIRS):
            for label in (("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")):
                directory = f"{pair}-{label}"
                result, inputs = measure(binaries[label], revisions[label], args.output / directory,
                                         template, files)
                if expected is None:
                    expected = inputs
                if inputs != expected:
                    raise RuntimeError("The measured programs received different request sequences")
                manifest["runs"].append({"pair": pair, "label": label, "directory": directory, **result})
                save(args.output / "benchmark.json", manifest)
        if inventory(template.home) != files:
            raise RuntimeError("The stopped generated seed changed")
        comparison = compare(args.output, manifest)
        save(args.output / "comparison.json", comparison)
        manifest.update(complete=True, identical_input_requests=len(expected))
    finally:
        try:
            template.close()
        except BaseException:
            manifest["complete"] = False
            raise
        finally:
            save(args.output / "benchmark.json", manifest)
    lines = ["## Temporary direct single-mutation comparison", "",
             f"Baseline: `{revisions['baseline']}`. Candidate: `{revisions['candidate']}`.", "",
             "| Case | Base P50/P95/P99 ms | Candidate P50/P95/P99 ms | P95 change |",
             "|---|---:|---:|---:|"]
    for case, values in comparison.items():
        text = lambda label: " / ".join(f"{values[label][name]:.3f}"
                                        for name in ("p50_ms", "p95_ms", "p99_ms"))
        lines.append(f"| {case} | {text('baseline')} | {text('candidate')} | "
                     f"{values['changes_percent']['p95_ms']:+.2f}% |")
    lines.extend(["", "Medians of four per-run statistics, not pooled percentiles.", manifest["scope"]])
    summary = "\n".join(lines) + "\n"
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
        stream.write(summary)
    print(summary, end="")


if __name__ == "__main__":
    main()
