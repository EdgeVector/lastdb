#!/usr/bin/env python3
"""Capture current external keys shared by native leaf and parent pages."""

import argparse
import hashlib
import json
from pathlib import Path
import struct
import sys

sys.dont_write_bytecode = True
from reuse import original
from benchmark_support import file_hash, save
from edge_capture import command
import overflow_pages

COLLECTION = "receipt-pruning-overflow-generated"
PREFIX = "overflow\0"
COUNT = 64


def fnv(value):
    result = 14_695_981_039_346_656_037
    for byte in value:
        result = ((result ^ byte) * 1_099_511_628_211) & 0xffff_ffff_ffff_ffff
    return result


def choose_keys(home):
    descriptor = (home / "laststore-pack-home-v1").read_bytes()
    fanout = struct.unpack_from("<H", descriptor, 24)[0]
    if fanout not in (1, 2, 4, 8, 16):
        raise RuntimeError("The generated native home has an unsupported finite hash fanout")
    buckets = [[] for _ in range(fanout)]
    for candidate in range(1024):
        suffix = f"key-{candidate:06d}-" + "x" * 20_000
        offset = fnv(suffix.encode()) & (fanout - 1)
        buckets[offset].append(PREFIX + suffix)
        if len(buckets[offset]) == COUNT:
            group = ((fnv(PREFIX.encode()) & 1023) + offset) & 1023
            return buckets[offset], group, {"fanout": fanout, "candidate_limit": 1024,
                                           "candidates_used": candidate + 1, "selected_offset": offset}
    raise RuntimeError("The bounded key enumeration does not produce 64 keys in one physical group")


def readback(capture, binary, label, keys, expected):
    requested = keys + [PREFIX + "never-offered-" + "x" * 20_000]
    exact = command(capture, binary, label + "-exact", "read", {
        "coordinates": [{"collection": COLLECTION, "key": key} for key in requested]})
    if exact["results"] != [{"body_hex": expected.get(key)} for key in requested]:
        raise RuntimeError("The exact external-key values or deleted absence differ")
    ranged = command(capture, binary, label + "-range", "range", {
        "collection": COLLECTION, "prefix": PREFIX, "page_size": 7, "max_pages": 16})
    rows = [row for page in ranged["pages"] for row in page["records"]]
    if (ranged["exhausted"] is not True or any(len(page["records"]) > 7 for page in ranged["pages"])
            or [row["key"] for row in rows] != sorted(expected)
            or {row["key"]: row["body_hex"] for row in rows} != expected):
        raise RuntimeError("The external-key range values, order, or page bounds differ")
    capture.remember("overflow-readback", label=label, known_keys=len(requested), live_keys=len(expected),
                     range_page_size=7, range_pages=len(ranged["pages"]))


def inspect(capture, label, keys, group):
    result = overflow_pages.inspect(capture.home / "data" / "disk-pack", COLLECTION, group, keys)
    save(capture.output / f"{label}-reachable-pages.json", result)
    return result


def run(capture, binaries):
    capture.start()
    capture.flush("overflow-empty-home")
    capture.stop()
    home = capture.home / "data" / "disk-pack"
    keys, group, selection = choose_keys(home)
    save(capture.output / "offered-keys.json", {"collection": COLLECTION, "prefix": PREFIX,
         "group": group, "selection": selection, "keys": keys,
         "key_bytes": [len(key.encode()) for key in keys]})
    expected = {key: hashlib.sha256(f"overflow:{number}:0".encode()).hexdigest()
                for number, key in enumerate(keys)}
    command(capture, binaries["disk"], "overflow-seed", "write", [
        {"collection": COLLECTION, "key": key, "body_hex": value} for key, value in expected.items()])
    seed = inspect(capture, "seed", keys, group)
    readback(capture, binaries["disk"], "overflow-seed", keys, expected)
    changes = [{"collection": COLLECTION, "key": key, "deleted": True} for key in keys[::4]]
    for key in keys[::4]:
        expected.pop(key)
    for number in range(1, COUNT, 4):
        value = hashlib.sha256(f"overflow:{number}:1".encode()).hexdigest()
        changes.append({"collection": COLLECTION, "key": keys[number], "body_hex": value})
        expected[keys[number]] = value
    command(capture, binaries["disk"], "overflow-replace-delete", "write", changes)
    command(capture, binaries["maintenance"], "overflow-reclaim", "reclaim", grace_ms=0)
    readback(capture, binaries["disk"], "overflow-reclaimed", keys, expected)
    changed = inspect(capture, "reclaimed", keys, group)
    capture.start()
    capture.flush("external-parent-leaf-reopen")
    capture.stop()
    readback(capture, binaries["disk"], "overflow-reopened", keys, expected)
    reopened = inspect(capture, "reopened", keys, group)
    return {"complete": True, "generated_keys": COUNT, "records_after_delete": len(expected),
            "physical_group": group, "selection": selection,
            "seed_locator_height": seed["locator_height"], "final_locator_height": reopened["locator_height"],
            "external_keys_in_current_parent_and_leaves": True,
            "shared_parent_leaf_overflow_references": changed["shared_parent_leaf_overflow_references"],
            "reclaim_without_compaction": True, "exact_range_reopen_verified": True,
            "scope": "Generated physical keyed operations; no map-part-one, cloud, or power-loss claim"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("binary", "disk-binary", "maintenance-binary", "scratch", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--source-revision", required=True)
    args = parser.parse_args()
    args.label = "receipt-overflow-operational"
    capture = original("benchmark").Benchmark(args)
    binaries = {"disk": args.disk_binary.resolve(strict=True), "maintenance": args.maintenance_binary.resolve(strict=True)}
    save(capture.output / "inputs.json", {"revision": args.source_revision,
         "commands": {name: {"binary": str(path), "sha256": file_hash(path)}
                      for name, path in {**binaries, "node": capture.binary}.items()},
         "helpers_sha256": {str(path): file_hash(path) for path in (
             Path(__file__), Path(overflow_pages.__file__), Path(__file__).with_name("edge_capture.py"))},
         "scope": "Fresh owned generated physical data; no primary or archived data"})
    summary = {"complete": False}
    try:
        summary.update(run(capture, binaries))
    except BaseException as failure:
        summary["error"] = str(failure)
        raise
    finally:
        try:
            capture.stop()
        except BaseException as failure:
            summary.update(complete=False, shutdown_error=str(failure))
            raise
        finally:
            capture.save_receipts()
            summary.update(all_owned_processes_stopped=capture.process is None,
                           scratch=str(capture.root), home=str(capture.home), scratch_retained=True)
            save(capture.output / "summary.json", summary)
            print(json.dumps(summary, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
