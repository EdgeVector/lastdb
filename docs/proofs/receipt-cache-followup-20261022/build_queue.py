#!/usr/bin/env python3
"""Record the external optimized-build wait without changing its shared queue."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import time

ROOT = Path(__file__).resolve().parent
LOCK = Path("/Users/tomtang/.cache/fold-db-perf-guard/host-cargo.lock")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observe-ticket")
    parser.add_argument("--finalize", action="store_true")
    args = parser.parse_args()
    target = ROOT / "optimized-build-queue-wait.json"
    if bool(args.observe_ticket) == args.finalize:
        raise ValueError("Select one original-ticket observation or final build-time observation")
    if args.observe_ticket:
        if target.exists() or not re.fullmatch(r"[0-9]{20}-[0-9]+", args.observe_ticket):
            raise ValueError("Require an unrecorded exact standard FIFO ticket")
        path = LOCK.with_name(LOCK.name + ".queue") / args.observe_ticket
        metadata = LOCK.read_text().strip()
        try:
            info = path.stat()
        except FileNotFoundError:
            info = None
            preparation = ROOT / "after-cache-preparation.json"
            if (not metadata.startswith("pid=" + args.observe_ticket.split("-")[1] + " ")
                    or not preparation.exists()):
                raise ValueError("The removed ticket lacks its matching acquired owner and build receipt")
        if info is not None and (path.is_symlink() or not path.is_file() or info.st_uid != os.getuid()):
            raise ValueError("The exact owned queued ticket differs")
        value = {"complete": False, "queue_ticket": args.observe_ticket,
                 "queue_entry_unix_ns_from_standard_ticket": int(args.observe_ticket.split("-")[0]),
                 "observation_unix_ns": time.time_ns(), "lock_metadata": metadata,
                 "ticket_device": info.st_dev if info is not None else None,
                 "ticket_inode": info.st_ino if info is not None else None,
                 "ticket_owner_uid": info.st_uid if info is not None else None,
                 "ticket_removed_at_acquisition_before_file_identity_capture": info is None,
                 "ticket_name_source": "The exact FIFO ticket was read in the preceding standard queue inventory",
                 "shared_queue_mutated": False,
                 "standard_queue_source_sha256": hashlib.sha256((ROOT.parents[2] / "scripts/ci/lib/host-cargo-lock.py").read_bytes()).hexdigest()}
    else:
        value = json.loads(target.read_text())
        if value.get("complete") is not False:
            raise ValueError("Do not replace a completed external wait receipt")
        preparation = ROOT / "after-cache-preparation.json"
        started = json.loads(preparation.read_text())["start_unix_ns"]
        value.update(complete=True, package_cache_preparation_start_unix_ns=started,
                     wait_plus_initial_source_guard_seconds=(started - value["queue_entry_unix_ns_from_standard_ticket"]) / 1e9,
                     queue_wait_limit="This interval includes the short source-identity checks between lock acquisition and cache preparation; it is separate from all request timings",
                     preparation_receipt_sha256=hashlib.sha256(preparation.read_bytes()).hexdigest())
    value["driver_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    target.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"queue_wait_receipt_complete": value["complete"]}))


if __name__ == "__main__":
    main()
