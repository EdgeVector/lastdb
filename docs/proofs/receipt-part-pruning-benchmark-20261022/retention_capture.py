#!/usr/bin/env python3
"""Capture native reads, retained roots, reclaim, and backup on generated data."""

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
from reuse import ROOT, original
from benchmark_support import file_hash, save

COLLECTION = "receipt-pruning-generated"
PREFIX = "receipt\0"
COUNT = 64


def key(number):
    return PREFIX + f"key-{number:06d}"


def body(number, revision):
    return "".join(hashlib.sha256(f"receipt:{number}:{revision}:{part}".encode()).hexdigest()
                   for part in range(16)).encode().hex()


def mutation(number, revision=None):
    value = {"collection": COLLECTION, "key": key(number)}
    if revision is None:
        value["deleted"] = True
    else:
        value["body_hex"] = body(number, revision)
    return value


def command(capture, binary, label, verb, home, request=None, **flags):
    if not home.is_relative_to(capture.root) or home.is_symlink():
        raise ValueError("Physical operations require the exact owned generated subtree")
    argv = [str(binary), verb, "--home", str(home)]
    if request is not None:
        path = capture.output / f"{label}-input.json"
        if verb == "write":
            path = path.with_suffix(".jsonl")
            path.write_text("".join(json.dumps(item, sort_keys=True) + "\n" for item in request))
        else:
            save(path, request)
        argv += ["--input", str(path)]
    for name, value in flags.items():
        argv += ["--" + name.replace("_", "-"), str(value)]
    environment = {name: value for name, value in os.environ.items()
                   if not name.startswith(("LASTDB", "FOLD", "SENTRY", "OBS_SENTRY"))}
    started = time.monotonic_ns()
    result = subprocess.run(argv, env=environment, stdin=subprocess.DEVNULL, capture_output=True,
                            text=True, timeout=min(120, capture.remaining()), check=False)
    capture.remember("physical-command", label=label, argv=argv, exit_code=result.returncode,
                     elapsed_nanos=time.monotonic_ns() - started,
                     stdout=result.stdout, stderr=result.stderr)
    (capture.output / f"{label}-stderr.txt").write_text(result.stderr)
    if result.returncode != 0:
        capture.save_receipts()
        raise RuntimeError(f"The physical {label} command failed; inspect its receipt")
    value = json.loads(result.stdout)
    save(capture.output / f"{label}-response.json", value)
    return value.get("result", value)


def inventory(capture, binaries, home, label):
    value = command(capture, binaries["maintenance"], label, "inventory", home)
    rows = value["files"]
    if len(rows) > 20000 or sum(row["bytes"] for row in rows) > 1024 ** 3:
        raise RuntimeError("Generated inventory exceeds its file or byte cap")
    actual = {}
    for row in rows:
        path = home / row["path"]
        if (not path.is_relative_to(home) or path.is_symlink() or not path.is_file()
                or path.stat().st_size != row["bytes"]):
            raise RuntimeError("A native inventory path differs from its exact file")
        actual[row["path"]] = {"bytes": row["bytes"], "sha256": file_hash(path)}
    current = (home / "CURRENT").read_bytes()
    features = struct.unpack_from("<Q", current, 112)[0]
    if len(current) != 4096 or features & 2 or features & 256 == 0:
        raise RuntimeError("The generated home does not use the journal-free snapshot format")
    result = {"generation": value["generation"], "frontier": value["frontier"],
              "features": features, "current_sha256": hashlib.sha256(current).hexdigest(),
              "files": actual}
    save(capture.output / f"{label}-verified.json", result)
    return result


def readback(capture, binary, home, label, expected):
    request = {"coordinates": [{"collection": COLLECTION, "key": key(number)}
                                for number in range(COUNT + 1)]}
    exact = command(capture, binary, label + "-exact", "read", home, request)
    wanted = [{"body_hex": expected.get(key(number))} for number in range(COUNT + 1)]
    if exact["results"] != wanted:
        raise RuntimeError("Native exact reads differ from the known generated keys")
    request = {"collection": COLLECTION, "prefix": PREFIX, "start": "key-000000",
               "end": "key-000064", "page_size": 7, "max_pages": 16}
    ranged = command(capture, binary, label + "-range", "range", home, request)
    pages = ranged["pages"]
    rows = [row for page in pages for row in page["records"]]
    values = {row["key"]: row["body_hex"] for row in rows}
    if (ranged["exhausted"] is not True or any(len(page["records"]) > 7 for page in pages)
            or [row["key"] for row in rows] != sorted(expected) or values != expected
            or (ranged["generation"], ranged["frontier"]) != (exact["generation"], exact["frontier"])):
        raise RuntimeError("Native range values, order, page bound, or root differ")
    capture.remember("native-readback", label=label, known_keys=COUNT + 1, live_keys=len(expected),
                     range_page_size=7, range_pages=len(pages), generation=exact["generation"],
                     frontier=exact["frontier"], expected_sha256=hashlib.sha256(
                         json.dumps(expected, sort_keys=True).encode()).hexdigest())


def retained_files(home, prior):
    for relative, expected in prior["files"].items():
        path = home / relative
        if not path.is_file() or path.stat().st_size != expected["bytes"] or file_hash(path) != expected["sha256"]:
            raise RuntimeError("A retained root dependency changed or disappeared")


def run(capture, binaries):
    capture.start()
    capture.flush("empty-home-created")
    capture.stop()
    home = capture.home / "data" / "disk-pack"
    command(capture, binaries["disk"], "seed", "write", home,
            [mutation(number, 0) for number in range(COUNT)])
    expected_before = {key(number): body(number, 0) for number in range(COUNT)}
    prior = inventory(capture, binaries, home, "retained-root")
    if prior["features"] & 512 == 0:
        raise RuntimeError("The operational proof requires precise current-part accounting")
    readback(capture, binaries["disk"], home, "seed", expected_before)
    reference = command(capture, binaries["maintenance"], "reference", "reference", home, purpose="rollback")
    bundle = capture.root / "earlier-backup"
    backup = command(capture, binaries["recovery"], "backup-earlier", "backup", home, backup=bundle)
    if backup.get("complete") is not True or backup.get("includes_identity") is not False:
        raise RuntimeError("The generated backup lacks its complete data-only receipt")
    expected = rewrite(capture, binaries, home)
    current = inventory(capture, binaries, home, "rewritten-current")
    obsolete = sorted(set(prior["files"]) - set(current["files"]))
    obsolete_data = [name for name in obsolete if name.startswith(("directory/", "locator/", "packs/"))]
    if not obsolete_data:
        raise RuntimeError("The generated workload does not produce obsolete dependency parts")
    retained_files(home, prior)
    reclaim = command(capture, binaries["maintenance"], "reclaim-held", "reclaim", home, grace_ms=0)
    if reclaim.get("reclaim_deferred") is True:
        raise RuntimeError("The supported reclaim command unexpectedly defers")
    retained_files(home, prior)
    readback(capture, binaries["disk"], home, "held-reclaim", expected)
    command(capture, binaries["maintenance"], "release", "release", home, reference=reference["reference"])
    command(capture, binaries["maintenance"], "reclaim-released", "reclaim", home, grace_ms=0)
    if any((home / name).exists() for name in obsolete_data):
        raise RuntimeError("An obsolete unretained data part remains after reclaim")
    readback(capture, binaries["disk"], home, "released-reclaim", expected)
    crash_reopen(capture, binaries, home, expected)
    restored = capture.root / "restored-earlier"
    restore = command(capture, binaries["recovery"], "restore-earlier", "restore", restored, backup=bundle)
    if (restore.get("complete") is not True
            or (restore["generation"], restore["frontier"]) != (prior["generation"], prior["frontier"])):
        raise RuntimeError("The backup restore differs from the retained earlier root")
    readback(capture, binaries["disk"], restored, "restored-earlier", expected_before)
    restored_inventory = inventory(capture, binaries, restored, "restored-earlier")
    if restored_inventory != prior:
        raise RuntimeError("The restored root differs from the exact earlier dependency inventory")
    capture.save_receipts()
    return {"complete": True, "keys_before": COUNT, "keys_after": len(expected),
            "obsolete_data_parts_removed": obsolete_data, "retained_dependencies_unchanged": True,
            "exact_and_range_values_verified": True, "forced_stop_reopen_verified": True,
            "earlier_backup_restore_verified": True, "identity_bytes_read_or_copied": False,
            "scope": "Generated native product operations; no power-loss or cloud claim"}


def rewrite(capture, binaries, home):
    for revision in range(1, 5):
        command(capture, binaries["disk"], f"rewrite-{revision}", "write", home,
                [mutation(number, revision) for number in range(COUNT)])
    command(capture, binaries["disk"], "delete-16", "write", home,
            [mutation(number) for number in range(16)])
    return {key(number): body(number, 4) for number in range(16, COUNT)}


def crash_reopen(capture, binaries, home, expected):
    capture.start()
    capture.flush("physical-data-before-force-stop")
    capture.stop(force=True)
    capture.save_receipts()
    capture.start()
    capture.flush("physical-data-after-force-stop")
    capture.stop()
    readback(capture, binaries["disk"], home, "forced-stop-reopened", expected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--disk-binary", type=Path, required=True)
    parser.add_argument("--maintenance-binary", type=Path, required=True)
    parser.add_argument("--recovery-binary", type=Path, required=True)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-revision", required=True)
    args = parser.parse_args()
    args.label = "receipt-retention-operational"
    capture = original("benchmark").Benchmark(args)
    binaries = {"disk": args.disk_binary.resolve(strict=True),
                "maintenance": args.maintenance_binary.resolve(strict=True),
                "recovery": args.recovery_binary.resolve(strict=True)}
    save(capture.output / "inputs.json", {"revision": args.source_revision,
         "binary": str(capture.binary), "binary_sha256": file_hash(capture.binary),
         "commands": {name: {"binary": str(path), "sha256": file_hash(path)} for name, path in binaries.items()},
         "helpers_sha256": {str(Path(__file__)): file_hash(Path(__file__))},
         "generated_keys": COUNT, "generated_body_bytes": 1024,
         "limits": {"run_seconds": 300, "native_bytes": 1024 ** 3, "native_files": 20000},
         "scope": "Fresh owned generated data; no primary or archived data"})
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
