#!/usr/bin/env python3
"""Capture old-format bootstrap and shared multi-part native payload lifetimes."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import time

sys.dont_write_bytecode = True
from reuse import original
from benchmark_support import file_hash, save
import payload_capture

COLLECTION = "receipt-pruning-large-generated"
PREFIX = "edge\0"
COUNT = 16
BODY_BYTES = 768 * 1024
BLOCK_BYTES = 4096


def key(number):
    suffix = "a" * 20_000 if number == 4 else "b" * 40_000 if number == 5 else ""
    return PREFIX + f"key-{number:06d}-" + suffix


def body(number, revision):
    return hashlib.shake_256(f"receipt-edge:{number}:{revision}".encode()).digest(BODY_BYTES).hex()


def compact(value):
    if isinstance(value, list):
        return [compact(item) for item in value]
    if not isinstance(value, dict):
        return value
    result = {}
    for name, item in value.items():
        if name == "body_hex" and isinstance(item, str):
            raw = bytes.fromhex(item)
            result.update(body_bytes=len(raw), body_sha256=hashlib.sha256(raw).hexdigest())
        else:
            result[name] = compact(item)
    return result


def command(capture, binary, label, verb, request=None, **flags):
    home = capture.home / "data" / "disk-pack"
    argv = [str(binary), verb, "--home", str(home)]
    input_receipt = None
    if request is not None:
        directory = capture.root / "physical-inputs"
        directory.mkdir(mode=0o700, exist_ok=True)
        path = directory / f"{label}.json"
        text = ("".join(json.dumps(item, sort_keys=True) + "\n" for item in request)
                if verb == "write" else json.dumps(request, sort_keys=True))
        raw = text.encode()
        if len(raw) > 64 * 1024 * 1024:
            raise RuntimeError("The generated native input exceeds 64 MiB")
        path.write_bytes(raw)
        input_receipt = {"path": str(path), "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
        argv += ["--input", str(path)]
    for name, value in flags.items():
        argv += ["--" + name.replace("_", "-"), str(value)]
    environment = {name: value for name, value in os.environ.items()
                   if not name.startswith(("LASTDB", "FOLD", "SENTRY", "OBS_SENTRY"))}
    started = time.monotonic_ns()
    result = subprocess.run(argv, env=environment, stdin=subprocess.DEVNULL, capture_output=True,
                            text=True, timeout=min(120, capture.remaining()), check=False)
    receipt = {"argv": argv, "exit_code": result.returncode, "input": input_receipt,
               "elapsed_nanos": time.monotonic_ns() - started,
               "stdout_bytes": len(result.stdout.encode()),
               "stdout_sha256": hashlib.sha256(result.stdout.encode()).hexdigest(), "stderr": result.stderr}
    if result.returncode != 0:
        capture.remember("large-native-command", label=label, **receipt)
        capture.save_receipts()
        raise RuntimeError(f"The large native {label} command failed")
    value = json.loads(result.stdout)
    receipt["result"] = compact(value)
    save(capture.output / f"{label}-command.json", receipt)
    capture.remember("large-native-command", label=label, **receipt)
    return value.get("result", value)


def inventory(capture, binaries, label):
    value = command(capture, binaries["maintenance"], label, "inventory")
    home = capture.home / "data" / "disk-pack"
    rows = value["files"]
    if len(rows) > 20000 or sum(row["bytes"] for row in rows) > 1024 ** 3:
        raise RuntimeError("The large generated inventory exceeds its finite cap")
    result = {}
    for row in rows:
        path = home / row["path"]
        if (path.is_symlink() or not path.is_relative_to(home) or not path.is_file()
                or path.stat().st_size != row["bytes"]):
            raise RuntimeError("A generated inventory path differs from its exact native part")
        result[row["path"]] = {"bytes": row["bytes"], "sha256": file_hash(path)}
    save(capture.output / f"{label}-hashes.json", result)
    return result


def readback(capture, binary, label, expected):
    exact = command(capture, binary, label + "-exact", "read", {
        "coordinates": [{"collection": COLLECTION, "key": key(number)} for number in range(COUNT + 1)]})
    if exact["results"] != [{"body_hex": expected.get(key(number))} for number in range(COUNT + 1)]:
        raise RuntimeError("Large native exact reads differ from known generated values")
    ranged = command(capture, binary, label + "-range", "range", {
        "collection": COLLECTION, "prefix": PREFIX, "start": "key-000000", "end": "key-000016",
        "page_size": 3, "max_pages": 8})
    rows = [row for page in ranged["pages"] for row in page["records"]]
    if (ranged["exhausted"] is not True or any(len(page["records"]) > 3 for page in ranged["pages"])
            or [row["key"] for row in rows] != sorted(expected)
            or {row["key"]: row["body_hex"] for row in rows} != expected):
        raise RuntimeError("Large native range values, order, or page bounds differ")
    capture.remember("large-native-readback", label=label, known_keys=COUNT + 1,
                     live_keys=len(expected), range_page_size=3, verified_body_bytes=len(expected) * BODY_BYTES)


def spanning(capture, seed, expected):
    home = capture.home / "data" / "disk-pack"
    result = payload_capture.inspect(home, seed, COLLECTION, [key(number) for number in range(COUNT)], expected)
    save(capture.output / "physical-boundaries.json", result)
    return [row["number"] for row in result["records"]], result["spanning_record_numbers"], result


def run(capture, binaries, before):
    after = capture.binary
    capture.binary = before
    capture.start()
    capture.flush("older-format-empty-home")
    capture.stop()
    capture.binary = after
    home = capture.home / "data" / "disk-pack"
    features = struct.unpack_from("<Q", (home / "CURRENT").read_bytes(), 112)[0]
    if features & 256 == 0 or features & 512:
        raise RuntimeError("The baseline node did not create the expected uncounted snapshot")
    initial = inventory(capture, binaries, "older-format-inventory")
    command(capture, binaries["disk"], "large-seed", "write", [
        {"collection": COLLECTION, "key": key(number), "body_hex": body(number, 0)} for number in range(COUNT)])
    counted = struct.unpack_from("<Q", (home / "CURRENT").read_bytes(), 112)[0]
    if counted & 512 == 0:
        raise RuntimeError("The first new write did not create a counted complete receipt")
    expected = {key(number): body(number, 0) for number in range(COUNT)}
    seed = inventory(capture, binaries, "large-seed-inventory")
    offered = {name: value for name, value in seed.items() if name not in initial and name.startswith("packs/")}
    readback(capture, binaries["disk"], "large-seed", expected)
    order, crossing, physical = spanning(capture, offered, expected)
    removed = next(number for number in order if number not in crossing)
    replaced = next(number for number in reversed(order) if number not in crossing and number != removed)
    command(capture, binaries["disk"], "partial-large-change", "write", [
        {"collection": COLLECTION, "key": key(removed), "deleted": True},
        {"collection": COLLECTION, "key": key(replaced), "body_hex": body(replaced, 1)}])
    expected.pop(key(removed))
    expected[key(replaced)] = body(replaced, 1)
    mixed_parts = [{"part": part["part"],
                    "old_live_records": [row["number"] for row in physical["records"]
                                         if part["part"] in row["data_parts"] and row["number"] not in (removed, replaced)],
                    "old_dead_records": [row["number"] for row in physical["records"]
                                         if part["part"] in row["data_parts"] and row["number"] in (removed, replaced)]}
                   for part in physical["data_parts"]]
    if not all(part["old_live_records"] and part["old_dead_records"] for part in mixed_parts):
        raise RuntimeError("The offered change does not leave both live and obsolete records in each original part")
    save(capture.output / "mixed-payload-membership.json", mixed_parts)
    shared = inventory(capture, binaries, "mixed-live-dead-inventory")
    payload = offered
    if any(shared.get(name) != value for name, value in payload.items()):
        raise RuntimeError("A partly live payload or its block map lost its shared part")
    command(capture, binaries["maintenance"], "shared-reclaim", "reclaim", grace_ms=0)
    readback(capture, binaries["disk"], "shared-reclaimed", expected)
    leftovers = [number for number in range(COUNT) if number not in (removed, replaced)]
    command(capture, binaries["disk"], "retire-original-values", "write", [
        {"collection": COLLECTION, "key": key(number), "deleted": True} for number in leftovers])
    expected = {key(replaced): body(replaced, 1)}
    final = inventory(capture, binaries, "original-payload-retired")
    if set(payload).intersection(final):
        raise RuntimeError("The final receipt still carries unreferenced original payload parts")
    command(capture, binaries["maintenance"], "final-reclaim", "reclaim", grace_ms=0)
    if any((home / name).exists() for name in payload):
        raise RuntimeError("An obsolete original payload or map part remains after reclaim")
    readback(capture, binaries["disk"], "final-reclaimed", expected)
    capture.start()
    capture.flush("large-native-reopen")
    capture.stop()
    readback(capture, binaries["disk"], "large-reopened", expected)
    return {"complete": True, "baseline_features": features, "first_write_features": counted,
            "generated_records": COUNT, "generated_body_bytes": BODY_BYTES,
            "key_bytes": [len(key(number).encode()) for number in range(COUNT)],
            "spanning_records_preserved": crossing, "partly_live_payload_parts": list(payload),
            "all_original_payload_parts_reclaimed": True, "exact_range_reopen_verified": True,
            "identity_bytes_read_or_copied": False, "raw_offered_inputs_retained_only_in_owned_scratch": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("binary", "before-binary", "disk-binary", "maintenance-binary", "scratch", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--source-revision", required=True)
    args = parser.parse_args()
    args.label = "receipt-large-operational"
    capture = original("benchmark").Benchmark(args)
    before = args.before_binary.resolve(strict=True)
    binaries = {"disk": args.disk_binary.resolve(strict=True), "maintenance": args.maintenance_binary.resolve(strict=True)}
    save(capture.output / "inputs.json", {"revision": args.source_revision,
         "commands": {name: {"binary": str(path), "sha256": file_hash(path)}
                      for name, path in {**binaries, "before": before, "after": capture.binary}.items()},
         "helpers_sha256": {str(path): file_hash(path) for path in (Path(__file__), Path(payload_capture.__file__))},
         "offered_input_cap_bytes": 64 * 1024 * 1024,
         "scope": "Fresh owned generated native data; no primary or archived data"})
    summary = {"complete": False}
    try:
        summary.update(run(capture, binaries, before))
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
