#!/usr/bin/env python3
"""Observe a repeated automatic orphan sweep after a real native duplicate batch."""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import struct
import sys
import time

sys.dont_write_bytecode = True
from reuse import original
import benchmark_support as support
from benchmark_support import file_hash, save
from auto_reclaim_capture import AutoCapture, dependencies, native, readback
from edge_capture import command as physical
from payload_capture import decode
from retention_capture import command

COLLECTION = "receipt-orphan-generated"
PREFIX = "orphan\0"
MIN_AGE_MS = 10000
BODY_BYTES = 768 * 1024


class OrphanCapture(AutoCapture):
    def start(self):
        previous = support.SETTINGS
        support.SETTINGS = {**previous, "LASTDB_ORPHAN_MIN_AGE_MS": str(MIN_AGE_MS),
                            "RUST_LOG": "warn,fold_db::storage::disk_pack::parts::worker=info"}
        try:
            super().start()
        finally:
            support.SETTINGS = previous


def payload(path, key, body):
    decoder_path = shutil.which("zstd")
    if not decoder_path:
        raise RuntimeError("The actual generated payload needs the installed zstd decoder")
    decoder = Path(decoder_path).resolve(strict=True)
    raw = bytearray()
    with path.open("rb") as source:
        header = source.read(128)
        check = header[:80] + bytes(32) + header[112:]
        if (header[:8] != b"LDPART1\0" or hashlib.sha256(check).digest() != header[80:112]
                or struct.unpack_from("<I", header, 44)[0] != path.stat().st_size):
            raise RuntimeError("The actual orphan payload header or checksum differs")
        for _ in range(struct.unpack_from("<Q", header, 56)[0]):
            prefix = source.read(80)
            length = struct.unpack_from("<I", prefix, 12)[0]
            if not 80 < length <= 4176:
                raise RuntimeError("The orphan frame exceeds its actual block bound")
            raw.extend(decode(prefix + source.read(length - 80), decoder))
            if len(raw) > 1024 ** 2:
                raise RuntimeError("The generated orphan payload exceeds one MiB")
        if source.read(1):
            raise RuntimeError("The generated orphan contains bytes outside its frames")
    key_bytes = struct.unpack_from("<I", raw, 12)[0]
    body_bytes = struct.unpack_from("<Q", raw, 16)[0]
    if (raw[:8] != b"LDREC1\0\0" or raw[96:96 + key_bytes] != key.encode()
            or raw[96 + key_bytes:] != body or body_bytes != len(body)
            or raw[40:72] != hashlib.sha256(key.encode()).digest()
            or raw[72:88] != hashlib.sha256(COLLECTION.encode()).digest()[:16]):
        raise RuntimeError("The actual orphan payload differs from the offered native Put")
    return {"path": str(path), "bytes": path.stat().st_size, "sha256": file_hash(path),
            "creation_generation": struct.unpack_from("<Q", header, 72)[0],
            "body_bytes": len(body), "body_sha256": hashlib.sha256(body).hexdigest(),
            "decoder": str(decoder), "decoder_sha256": file_hash(decoder)}


def duplicate(capture, binaries, suffix):
    home = capture.home / "data" / "disk-pack"
    before = support.bounded_inventory(capture.home)["_files"]
    value = hashlib.shake_256(("orphan:" + suffix).encode()).digest(BODY_BYTES)
    key = PREFIX + suffix
    physical(capture, binaries["disk"], suffix + "-put-delete", "write", [
        {"collection": COLLECTION, "key": key, "body_hex": value.hex()},
        {"collection": COLLECTION, "key": key, "deleted": True}])
    after = support.bounded_inventory(capture.home)["_files"]
    current = dependencies(capture, binaries["maintenance"], home, suffix + "-current")
    offered = [name for name in set(after) - set(before)
               if name.startswith("packs/") and "/map/" not in name]
    if len(offered) != 1 or offered[0] in current["files"]:
        raise RuntimeError("The real duplicate batch does not create one never-receipted payload part")
    name = offered[0]
    actual = payload(home / name, key, value)
    earlier = [int(Path(path).parts[1]) for path in before if path.startswith("generations/")]
    if (max(earlier) >= actual["creation_generation"]
            or actual["creation_generation"] != current["generation"]):
        raise RuntimeError("The orphan creation does not follow all earlier generated receipts")
    evidence = {**actual, "relative_path": name, "current_generation": current["generation"],
                "omitted_from_current_inventory": True, "earlier_receipts_precede_part_creation": True,
                "offered_operation": "Put(key,value), Delete(key) in one supported native write command"}
    save(capture.output / f"{suffix}-orphan.json", evidence)
    return name, evidence


def wait_old(capture, home, old, young, young_generation):
    started = time.monotonic()
    while time.monotonic() - started < 8:
        value = native(capture)
        if value["generation"] <= young_generation:
            raise RuntimeError("The startup generation floor does not exceed the young payload creation")
        if not (home / young).is_file():
            raise RuntimeError("The young orphan disappears before the missed-startup proof")
        if not (home / old).exists():
            result = {"old_removed": True, "young_retained": True, "elapsed_seconds": time.monotonic() - started,
                      "young_age_seconds": time.time() - (home / young).stat().st_mtime,
                      "startup_generation": value["generation"], "young_creation_generation": young_generation}
            if result["young_age_seconds"] >= MIN_AGE_MS / 1000:
                raise RuntimeError("The first sweep observation exceeds the young-part age cutoff")
            save(capture.output / "startup-sweep.json", result)
            return result
        time.sleep(0.1)
    raise RuntimeError("The startup sweep does not remove its eligible old orphan within eight seconds")


def wait_young(capture, home, target, creation_generation):
    started, deadline = time.monotonic(), min(capture.deadline, time.monotonic() + 90)
    polls = []
    while time.monotonic() < deadline:
        value = native(capture)
        if value["generation"] <= creation_generation:
            raise RuntimeError("The later live mutation does not advance the captured generation floor")
        exists = (home / target).exists()
        polls.append({"elapsed_seconds": time.monotonic() - started, "exists": exists, "native": value})
        if len(polls) % 5 == 1 or not exists:
            readback(capture, "orphan-repeat-" + str(len(polls)))
        if not exists:
            result = {"complete": True, "elapsed_seconds": time.monotonic() - started, "polls": polls,
                      "manual_reclaim_or_pack_calls": 0}
            save(capture.output / "periodic-orphan-sweep.json", result)
            return result
        time.sleep(min(1, capture.remaining()))
    save(capture.output / "periodic-orphan-sweep.json", {"complete": False, "polls": polls})
    raise RuntimeError("The repeated orphan sweep misses its ninety-second deadline")


def physical_readback(capture, binary, home, label):
    keys = [PREFIX + suffix for suffix in ("keeper", "old", "young")]
    exact = command(capture, binary, label + "-native-exact", "read", home,
                    {"coordinates": [{"collection": COLLECTION, "key": key} for key in keys]})
    if exact["results"] != [{"body_hex": "aabb"}, {"body_hex": None}, {"body_hex": None}]:
        raise RuntimeError("The native orphan keys or live keeper value differ")
    ranged = command(capture, binary, label + "-native-range", "range", home,
                     {"collection": COLLECTION, "prefix": PREFIX, "start": "keeper", "end": "zz",
                      "page_size": 2, "max_pages": 4})
    rows = [row for page in ranged["pages"] for row in page["records"]]
    projected = [{"key": row["key"], "body_hex": row["body_hex"]} for row in rows]
    if ranged["exhausted"] is not True or projected != [{"key": keys[0], "body_hex": "aabb"}]:
        raise RuntimeError("The native orphan range differs from its one live keeper")


def run(capture, binaries):
    capture.start()
    capture.install()
    capture.write(list(range(8)), 0, create=True)
    capture.checkpoint("orphan-seeded")
    capture.stop()
    home = capture.home / "data" / "disk-pack"
    old, old_evidence = duplicate(capture, binaries, "old")
    physical(capture, binaries["disk"], "advance-old-floor", "write", [
        {"collection": COLLECTION, "key": PREFIX + "keeper", "body_hex": "aa00"}])
    age_deadline = time.monotonic() + 12
    while time.monotonic() < age_deadline:
        capture.remaining()
        time.sleep(min(0.25, age_deadline - time.monotonic()))
    young, young_evidence = duplicate(capture, binaries, "young")
    physical(capture, binaries["disk"], "advance-young-startup-floor", "write", [
        {"collection": COLLECTION, "key": PREFIX + "keeper", "body_hex": "aabb"}])
    auto_log_offset = (capture.output / "node.log").stat().st_size
    capture.automatic = True
    capture.start()
    capture.checkpoint("orphan-auto-started")
    startup = wait_old(capture, home, old, young, young_evidence["creation_generation"])
    readback(capture, "orphan-startup-kept-young")
    capture.write([0], 900)
    capture.checkpoint("orphan-live-floor-advanced")
    current = dependencies(capture, binaries["maintenance"], home, "orphan-live-current")
    if young in current["files"]:
        raise RuntimeError("A later current receipt unexpectedly names the unused orphan payload")
    periodic = wait_young(capture, home, young, young_evidence["creation_generation"])
    capture.stop()
    log = (capture.output / "node.log").read_bytes()[auto_log_offset:].decode()
    events = [line for line in log.splitlines() if "native storage removed unreferenced part files" in line]
    if len(events) < 2:
        raise RuntimeError("The same AUTO process does not report both successful orphan removal sweeps")
    save(capture.output / "automatic-sweep-log.json", {"same_process": True, "removal_events": events,
         "log_offset": auto_log_offset, "log_bytes": len(log.encode()), "log_sha256": hashlib.sha256(log.encode()).hexdigest()})
    physical_readback(capture, binaries["disk"], home, "before-reopen")
    capture.start()
    capture.checkpoint("orphan-reopened")
    native(capture)
    readback(capture, "orphan-reopened")
    capture.stop()
    physical_readback(capture, binaries["disk"], home, "after-reopen")
    return {"complete": True, "known_live_keys": 8, "native_keeper_keys": 1,
            "native_duplicate_batches": 2, "orphan_payload_bytes_each": BODY_BYTES,
            "minimum_age_ms": MIN_AGE_MS, "expected_repeat_interval_seconds": 30,
            "startup_old_removed_young_retained": True, "young_predates_node_start": True,
            "same_process_successful_orphan_removal_events": len(events),
            "startup_seconds": startup["elapsed_seconds"], "periodic_seconds": periodic["elapsed_seconds"],
            "old_orphan": old_evidence, "young_orphan": young_evidence,
            "maintenance_runs": 0, "manual_reclaim_or_pack_calls": 0,
            "exact_range_reopen_verified": True,
            "scope": "Actual native duplicate batches before startup; periodic retry after an age-cutoff miss"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("binary", "disk-binary", "maintenance-binary", "scratch", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--source-revision", required=True)
    args = parser.parse_args()
    args.label = "receipt-orphan-repeat-operational"
    capture = OrphanCapture(args)
    binaries = {"disk": args.disk_binary.resolve(strict=True), "maintenance": args.maintenance_binary.resolve(strict=True)}
    save(capture.output / "inputs.json", {"revision": args.source_revision, "binary": str(capture.binary),
         "binary_sha256": file_hash(capture.binary), "minimum_orphan_age_ms": MIN_AGE_MS,
         "helpers_sha256": {str(Path(__file__)): file_hash(Path(__file__))},
         "physical_binaries": {name: {"path": str(path), "sha256": file_hash(path)} for name, path in binaries.items()},
         "stages": ["AUTO off seed", "stopped old native duplicate batch", "twelve-second age wait",
                    "stopped young native duplicate batch", "AUTO startup removes old and keeps young",
                    "live update advances floor", "periodic sweep removes young", "exact keys, ranges, reopen"],
         "limits": {"run_seconds": 300, "periodic_seconds": 90, "native_files": 20000,
                    "inventory_entries": 40000, "inventory_depth": 8, "generated_body_bytes": BODY_BYTES}})
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
