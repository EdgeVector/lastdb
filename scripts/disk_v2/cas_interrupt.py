#!/usr/bin/env python3
"""CI-only real process interruption during native CAS deletion and normal reopen."""

import argparse
import json
import os
from pathlib import Path
import signal
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
from node import Node, file_hash, save
from cas_interrupt_ops import COUNT, FILE_BYTES, require, seed, records, files_after_recovery, counters
from cleanup import request_and_wait, verify_saved

ATTEMPTS = 4


def interrupt(node):
    """Observe real file publication; never write or synthesize native state."""
    intent = node.home / "data/disk-run/CAS_DELETE"
    current = node.home / "data/disk-run/CURRENT"
    started, deadline = time.monotonic(), time.monotonic() + 210
    while time.monotonic() < deadline:
        require(node.process.poll() is None, "owned writer exited before deliberate interruption")
        try:
            pending = intent.stat()
            published = current.stat()
            if published.st_mtime_ns > pending.st_mtime_ns and intent.exists():
                process = node.process
                process.kill()
                process.wait(timeout=10)
                require(process.returncode == -signal.SIGKILL, "owned child did not receive the expected stop")
                node.process = None
                node.log.close()
                node.log = None
                return {"signal": "SIGKILL", "owned_pid": process.pid,
                        "intent_present_after_stop": intent.exists(),
                        "intent_sha256_after_stop": file_hash(intent) if intent.exists() else None,
                        "wait_seconds": round(time.monotonic() - started, 3)}
        except FileNotFoundError:
            pass
        # The ordinary 120-second dwell stays in force. Poll closely only
        # near its due boundary, rather than consume a CPU during that wait.
        time.sleep(0.1 if time.monotonic() - started < 118 else 0.0002)
    return {"signal": None, "intent_present_after_stop": False,
            "wait_seconds": round(time.monotonic() - started, 3)}


def attempt(args, number):
    output = args.output / f"attempt-{number}"
    output.mkdir()
    node = Node(args.candidate, args.candidate_revision, output, environment={
        "LASTDB_SELF_METRICS_TO_DB": "false", "LASTDB_SELF_METRICS_SAMPLE_SECS": "1",
        "LASTDB_TTL_SWEEP": "0", "LASTDB_BLOB_DWELL_SECS": "120", "LASTDB_BLOB_DWELL_CHUNK": "64",
        "LASTDB_BYTE_BACKUP_V2_ENABLED": "false", "EXEMEM_API_URL": "http://127.0.0.1:9",
        "FOLD_SCHEMA_SERVICE_URL": "http://127.0.0.1:9"})
    complete = False
    result = {"observed_completed_recovery": False}
    try:
        node.start()
        require(node.disk_format() == 2, "operation did not create a v2 home")
        pointers = seed(node)
        result["interruption"] = interrupt(node)
        if node.process is not None:
            node.stop()
        node.start()
        native = node.call("/api/db/native-status")[0]["native_disk"]
        require(native["writer_failed"] is False, "normal reopen requires recovery")
        recovered = native["cas_delete_recovery"]
        result["recovery"] = recovered
        # A merge can replace CURRENT without changing its frontier. The
        # authenticated production recovery report, not mtime, classifies the cut.
        observed = recovered is not None and recovered["outcome"] == "completed"
        if observed:
            require(result["interruption"]["intent_present_after_stop"]
                    and recovered["intent_keys"] > 0 and recovered["intent_released"] is True
                    and recovered["cas_frontier_after"] == recovered["cas_frontier_before"] + 1,
                    "normal open did not complete the exact pending CAS deletion")
        intent = node.home / "data/disk-run/CAS_DELETE"
        if result["interruption"]["intent_present_after_stop"]:
            require(recovered is not None and recovered["intent_released"] is True,
                    "normal open did not release the saved intent")
            # Other due dwell chunks resume after normal open. A new intent
            # has a fresh authenticated frame id; it must not be the saved one.
            try:
                current_intent = file_hash(intent)
            except FileNotFoundError:
                current_intent = None
            require(current_intent != result["interruption"]["intent_sha256_after_stop"],
                    "reopen retained the original deletion intent")
        records(node, pointers, True)
        files_after_recovery(node, pointers)
        result["counters"] = counters(node)
        receipt = request_and_wait(node)
        node.stop()
        require(not intent.exists(), "completed cleanup retained a deletion intent")
        stops = [json.loads((output / "shutdown.json").read_text())]
        node.start()
        require(node.call("/api/db/native-status")[0]["native_disk"]["cas_delete_recovery"] is None,
                "a second normal boot repeated the deletion recovery")
        records(node, pointers, True)
        files_after_recovery(node, pointers)
        verify_saved(node, receipt)
        result.update(observed_completed_recovery=observed, deleted_records=COUNT,
                      deleted_blobs=COUNT, live_records=2, live_blobs=1,
                      live_file_bytes=FILE_BYTES, live_file_sha256=pointers[-1]["sha256"],
                      cleanup=receipt, complete_cleanup=True, normal_reopened_boots=2, no_forged_state=True)
        result["intent_absent_after_cleanup"] = True
        node.stop()
        require(not intent.exists(), "second clean boot retained a deletion intent")
        stops.append(json.loads((output / "shutdown.json").read_text()))
        result["clean_shutdowns"] = stops
        complete = True
        return result
    finally:
        if complete:
            node.close()
        else:
            # Preserve this owned generated home on an uncertain failure.
            # The ordinary stop checks still run and may fail the operation.
            try:
                node.stop()
            finally:
                save(output / "result.json", result)
        if complete:
            save(output / "result.json", result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--candidate-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    require(os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted",
            "this operation requires a GitHub-hosted CI runner")
    require(args.output.is_absolute() and not args.output.exists(), "fresh external output directory required")
    runner = Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)
    require(args.output.parent.resolve(strict=True).is_relative_to(runner), "output must stay in runner temporary storage")
    args.output.mkdir(mode=0o700)
    summary = {"source": args.candidate_revision, "binary_sha256": file_hash(args.candidate),
               "drivers": {name: file_hash(Path(__file__).parent / name)
                           for name in ("cas_interrupt.py", "cas_interrupt_ops.py", "cleanup.py")},
               "attempts": [], "complete": False, "primary_access": False, "cloud_access": False}
    for number in range(ATTEMPTS):
        print(f"CAS interruption: real generated node attempt {number + 1}/{ATTEMPTS}", flush=True)
        summary["attempts"].append(attempt(args, number))
        save(args.output / "operation.json", summary)
        if summary["attempts"][-1]["observed_completed_recovery"]:
            summary["complete"] = True
            break
    save(args.output / "operation.json", summary)
    require(summary["complete"], "no authenticated main-published/CAS-pending interruption cut was captured")
    print("PASS: real interrupted CAS deletion recovered; 256 deleted blobs absent, two live records share one exact file; counters and cleanup complete; second boot does not replay", flush=True)


if __name__ == "__main__":
    main()
