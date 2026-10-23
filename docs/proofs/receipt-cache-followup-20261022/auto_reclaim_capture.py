#!/usr/bin/env python3
"""Observe startup and periodic no-pack reclaim on an owned generated home."""

import argparse
import hashlib
import json
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
from reuse import original
import benchmark_support as support
from benchmark_support import FIELDS, SCHEMA, file_hash, save
from retention_capture import command


class AutoCapture(support.Benchmark):
    automatic = False

    def start(self):
        previous = support.SETTINGS
        support.SETTINGS = {**previous, "LASTDB_AUTO_MAINTENANCE": str(int(self.automatic)),
                            "LASTDB_AUTO_MAINTENANCE_START_DELAY_SECS": "0"}
        try:
            super().start()
        finally:
            support.SETTINGS = previous


def readback(capture, label):
    exact = capture.call("/api/query", {"schema_name": SCHEMA, "fields": FIELDS,
        "filter": {"HashRangeKeys": [["measure", key] for key in sorted(capture.expected)]}, "limit": 16})
    if capture.rows(exact) != capture.expected or exact.get("has_more") is not False:
        raise RuntimeError("The AUTO exact read differs from the eight known keys")
    actual, order, cursor = {}, [], None
    for _ in range(4):
        request = {"schema_name": SCHEMA, "fields": FIELDS, "limit": 3,
                   "filter": {"HashRangeRange": {"hash": "measure", "start": "key-000000",
                                                "end": "key-000008"}}}
        if cursor is not None:
            request["cursor"] = cursor
        result = capture.call("/api/query", request)
        rows = capture.rows(result)
        if len(rows) > 3 or set(rows).intersection(actual):
            raise RuntimeError("The AUTO range repeats a key or exceeds three records")
        actual.update(rows)
        order.extend(row["key"]["range"] for row in result["results"])
        if result.get("has_more") is False:
            break
        next_cursor = result.get("next_cursor")
        if not next_cursor or next_cursor == cursor:
            raise RuntimeError("The AUTO range cursor cannot advance")
        cursor = next_cursor
    else:
        raise RuntimeError("The AUTO range exceeds four pages")
    if actual != capture.expected or order != sorted(capture.expected):
        raise RuntimeError("The AUTO range values or order differ")
    capture.remember("auto-readback", label=label, keys=8, range_page_size=3,
                     expected_sha256=hashlib.sha256(json.dumps(capture.expected, sort_keys=True).encode()).hexdigest())


def native(capture):
    value = capture.call("/api/db/native-status")["native_disk"]
    parts = value["parts"]
    if (value.get("writer_failed") is not False or parts["auto_maintenance"] is not True
            or parts["dependency_high_water"] != 4096 or parts["dependency_trigger"] != 4096
            or parts["dependency_parts"] >= parts["dependency_trigger"]
            or parts["maintenance_runs"] != 0
            or parts["maintenance_worker_alive"] is not True or parts["reclaim_failures"] != 0
            or parts.get("reclaim_last_error") is not None):
        raise RuntimeError("AUTO status violates the default below-trigger no-pack contract")
    return value


def dependencies(capture, binary, home, label):
    current = file_hash(home / "CURRENT")
    value = command(capture, binary, label, "inventory", home)
    if file_hash(home / "CURRENT") != current:
        raise RuntimeError("CURRENT changed across the quiet dependency inventory")
    files = {row["path"] for row in value["files"]}
    if len(files) > 20000 or sum(row["bytes"] for row in value["files"]) > 64 * 1024 ** 2:
        raise RuntimeError("The AUTO dependency inventory exceeds its finite bound")
    if any(Path(name).is_absolute() or ".." in Path(name).parts for name in files):
        raise RuntimeError("The native dependency inventory contains an invalid relative path")
    return {"files": files, "current_sha256": current, "generation": value["generation"]}


def obsolete(prior, current, home, require_present=True):
    names = sorted(prior["files"] - current["files"])
    data = [name for name in names if name.startswith(("directory/", "locator/", "packs/"))]
    generations = [name for name in names if name.startswith("generations/")]
    if not data or not generations or (require_present and any(not (home / name).is_file() for name in names)):
        raise RuntimeError("The AUTO workload lacks actual obsolete data and generation files")
    return names


def wait_removed(capture, home, targets, current, label):
    started, deadline = time.monotonic(), min(capture.deadline, time.monotonic() + 90)
    polls, readbacks = [], 0
    while time.monotonic() < deadline:
        value = native(capture)
        present = [name for name in targets if (home / name).exists()]
        polls.append({"elapsed_seconds": time.monotonic() - started, "remaining": present,
                      "native": value})
        if file_hash(home / "CURRENT") != current["current_sha256"]:
            raise RuntimeError("The no-pack AUTO sweep changes the current snapshot root")
        if len(polls) % 5 == 1 or not present:
            readback(capture, label + f"-poll-{len(polls)}")
            readbacks += 1
        if not present:
            result = {"elapsed_seconds": time.monotonic() - started, "targets": targets,
                      "polls": polls, "readbacks": readbacks, "current_root_unchanged": True}
            save(capture.output / f"{label}-sweep.json", result)
            return result
        time.sleep(min(1, capture.remaining()))
    save(capture.output / f"{label}-sweep.json", {"targets": targets, "polls": polls, "complete": False})
    raise RuntimeError("The AUTO sweep misses its ninety-second file removal deadline")


def run(capture, maintenance):
    capture.start()
    capture.install()
    capture.flush("auto-off-schema")
    capture.write(list(range(8)), 0, create=True)
    capture.checkpoint("auto-off-seeded")
    home = capture.home / "data" / "disk-pack"
    seeded = dependencies(capture, maintenance, home, "auto-off-seeded")
    for revision in range(1, 5):
        capture.write(list(range(8)), revision)
    capture.checkpoint("auto-off-rewritten")
    rewritten = dependencies(capture, maintenance, home, "auto-off-rewritten")
    startup_targets = obsolete(seeded, rewritten, home)
    capture.stop()
    capture.automatic = True
    capture.start()
    capture.checkpoint("auto-on-startup")
    before = dependencies(capture, maintenance, home, "auto-on-startup-current")
    startup = wait_removed(capture, home, startup_targets, before, "startup")
    native(capture)
    readback(capture, "startup-drained")
    if any(not (home / name).is_file() for name in before["files"]):
        raise RuntimeError("A current dependency is absent before the periodic update burst")
    burst_started = time.monotonic()
    for step in range(32):
        capture.write([step % 8], 100 + step)
    capture.checkpoint("auto-on-32-durable-updates")
    after = dependencies(capture, maintenance, home, "auto-on-updated-current")
    periodic_targets = obsolete(before, after, home, require_present=False)
    burst_seconds = time.monotonic() - burst_started
    capture.remember("periodic-targets", observed_live_after_startup=periodic_targets,
                     already_removed=[name for name in periodic_targets if not (home / name).exists()],
                     update_burst_seconds=burst_seconds)
    periodic = wait_removed(capture, home, periodic_targets, after, "periodic")
    capture.stop()
    capture.start()
    capture.checkpoint("auto-on-reopened")
    native(capture)
    readback(capture, "auto-on-reopened")
    return {"complete": True, "keys": 8, "durable_updates_after_startup": 32,
            "automatic_maintenance": True, "dependency_high_water": 4096,
            "startup_obsolete_files_removed": startup_targets,
            "periodic_obsolete_files_removed": periodic_targets,
            "startup_seconds": startup["elapsed_seconds"], "periodic_seconds": periodic["elapsed_seconds"],
            "update_burst_seconds": burst_seconds,
            "maintenance_runs": 0, "manual_pack_or_reclaim_calls": 0,
            "exact_and_range_during_sweep_and_reopen": True,
            "scope": "Generated local operations; no power-loss, primary, or cloud claim"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("binary", "disk-binary", "maintenance-binary", "scratch", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--source-revision", required=True)
    args = parser.parse_args()
    args.label = "receipt-auto-reclaim-operational"
    capture = AutoCapture(args)
    save(capture.output / "inputs.json", {"revision": args.source_revision,
         "binary": str(capture.binary), "binary_sha256": file_hash(capture.binary),
         "maintenance_binary": str(args.maintenance_binary), "maintenance_sha256": file_hash(args.maintenance_binary),
         "helpers_sha256": {str(Path(__file__)): file_hash(Path(__file__)),
                            str(Path(command.__code__.co_filename)): file_hash(Path(command.__code__.co_filename))},
         "settings_by_stage": {"history": {**support.SETTINGS, "LASTDB_AUTO_MAINTENANCE_START_DELAY_SECS": "0"},
                               "startup_and_periodic": {**support.SETTINGS, "LASTDB_AUTO_MAINTENANCE": "1",
                                                        "LASTDB_AUTO_MAINTENANCE_START_DELAY_SECS": "0"}},
         "stages": ["AUTO off generated history", "AUTO on startup drain", "AUTO on periodic drain"],
         "default_high_water": 4096, "known_keys": 8, "post_startup_durable_updates": 32,
         "limits": {"run_seconds": 300, "sweep_seconds": 90, "dependency_bytes": 64 * 1024 ** 2,
                    "native_files": 20000, "inventory_depth": 8, "inventory_entries": 40000},
         "scope": "Fresh owned generated data; no primary or archived data"})
    summary = {"complete": False}
    try:
        summary.update(run(capture, args.maintenance_binary.resolve(strict=True)))
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
