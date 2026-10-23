#!/usr/bin/env python3
"""Remove the exact stopped home from the explicitly rejected cache attempt."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import stat
import time

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("owned_cleanup",
    HERE.parent / "snapshot-commits-20261022" / "cleanup.py")
cleanup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cleanup)


def main():
    rejected = HERE / "cache-reuse-invalid"
    disposition = cleanup.load(rejected / "rejection.json")
    run = rejected / "measurements" / "pair-0-after"
    if disposition.get("rejected") is not True:
        raise ValueError("No explicit rejection receipt")
    owner = cleanup.load(run / "ownership.json")
    summary = cleanup.load(run / "summary.json")
    if summary.get("all_owned_processes_stopped") is not True:
        raise ValueError("The rejected node remains active")
    root = Path(owner["scratch"])
    temporary = Path(os.environ["TMPDIR"]).resolve(strict=True)
    if (root.is_symlink() or root.resolve(strict=True) != root or root.parent != temporary
            or owner.get("primary_data") is not False
            or owner.get("home") != str(root / "home")):
        raise ValueError("Not the exact generated temporary home")
    info = root.stat()
    if (info.st_uid != os.getuid() or info.st_uid != owner["owner_uid"]
            or stat.S_IMODE(info.st_mode) != 0o700
            or info.st_dev != owner["device"] or info.st_ino != owner["inode"]):
        raise ValueError("The ownership metadata differs")
    if cleanup.load(root / ".snapshot-capture-owner.json") != owner:
        raise ValueError("The ownership marker differs")
    stopped = cleanup.owned_processes(run)
    cleanup.no_active_sockets(root)
    rows = [json.loads(line) for line in (run / "receipts.jsonl").read_text().splitlines()]
    stops = {row["pid"]: row for row in rows if row["operation"] == "owned-node-stop"}
    for pid in stopped:
        row = stops.get(pid, {})
        if (row.get("forced") is not False or row.get("exit_code") != 0
                or row.get("shutdown_receipt", {}).get("flush_ok") is not True):
            raise ValueError("The rejected node lacks its graceful stop receipt")
    receipt = {"removed": False, "scratch": str(root), "ownership": owner,
        "rejected_results_excluded": True, "stopped_pids": stopped,
        "pre_cleanup": cleanup.inventory(root), "at_ns": time.time_ns()}
    target = run / "rejected-cleanup.json"
    target.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    shutil.rmtree(root)
    if root.exists():
        raise RuntimeError("The rejected owned home remains")
    receipt["removed"] = True
    target.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"rejected_home_removed": str(root)}))


if __name__ == "__main__":
    main()
