#!/usr/bin/env python3
"""Remove one stopped generated home after exact ownership and receipt checks."""

import argparse
import json
import os
from pathlib import Path
import shutil
import socket
import stat
import time


def load(path):
    if path.stat().st_size > 32 * 1024 * 1024:
        raise ValueError("Receipt exceeds the finite byte bound")
    return json.loads(path.read_text())


def owned_processes(run):
    processes = set()
    for path in (run / "receipts.jsonl", run / "backup-restore" / "receipts.jsonl"):
        if not path.exists():
            continue
        if path.stat().st_size > 32 * 1024 * 1024:
            raise ValueError("Process receipts exceed the finite byte bound")
        for line in path.read_text().splitlines():
            item = json.loads(line)
            if item.get("operation") == "owned-node-start":
                processes.add(item["pid"])
    if not processes:
        raise ValueError("No owned node process receipt exists")
    for pid in processes:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            continue
        raise ValueError(f"Recorded node process {pid} still exists; retain the data")
    return sorted(processes)


def no_active_sockets(root):
    for path in (root / "q.sock", root / "f.sock"):
        probe = socket.socket(socket.AF_UNIX)
        probe.settimeout(0.25)
        try:
            probe.connect(str(path))
        except (FileNotFoundError, ConnectionRefusedError):
            pass
        else:
            raise ValueError("The owned home still has an active node socket")
        finally:
            probe.close()


def inventory(root):
    stack, entries, size, deadline = [(root, 0)], 0, 0, time.monotonic() + 20
    while stack:
        parent, depth = stack.pop()
        if depth > 12:
            raise ValueError("Owned cleanup exceeds depth twelve")
        for path in parent.iterdir():
            entries += 1
            if entries > 40000 or time.monotonic() > deadline:
                raise ValueError("Owned cleanup exceeds its entry or time bound")
            if path.is_symlink():
                raise ValueError("Owned cleanup refuses a symlink")
            info = path.lstat()
            if info.st_uid != os.getuid():
                raise ValueError("Owned cleanup found another file owner")
            if stat.S_ISDIR(info.st_mode):
                stack.append((path, depth + 1))
            elif stat.S_ISREG(info.st_mode):
                size += info.st_size
            elif not stat.S_ISSOCK(info.st_mode):
                raise ValueError("Owned cleanup found an unsupported file type")
    return {"entries": entries, "logical_file_bytes": size}


def cleanup(run):
    ownership = load(run / "ownership.json")
    summary = load(run / "summary.json")
    root = Path(ownership["scratch"])
    temporary_root = Path(os.environ["TMPDIR"]).resolve(strict=True)
    if (root.is_symlink() or root.resolve(strict=True) != root or root.parent != temporary_root
            or ownership.get("primary_data") is not False
            or ownership.get("home") != str(root / "home")):
        raise ValueError("Refuse any path except the exact owned temporary home")
    info = root.stat()
    if (info.st_uid != os.getuid() or info.st_uid != ownership["owner_uid"]
            or stat.S_IMODE(info.st_mode) != 0o700
            or info.st_dev != ownership["device"] or info.st_ino != ownership["inode"]):
        raise ValueError("Scratch ownership metadata differs")
    if load(root / ".snapshot-capture-owner.json") != ownership:
        raise ValueError("Scratch ownership marker differs")
    if summary.get("complete") is not True or summary.get("all_owned_processes_stopped") is not True:
        raise ValueError("The preceding product capture is incomplete or active")
    backup_summary = run / "backup-restore" / "summary.json"
    if backup_summary.exists():
        backup = load(backup_summary)
        if backup.get("complete") is not True or backup.get("all_owned_processes_stopped") is not True:
            raise ValueError("The backup/restore capture is incomplete or active")
    stopped = owned_processes(run)
    no_active_sockets(root)
    receipt = {"scratch": str(root), "ownership": ownership, "stopped_pids": stopped,
               "pre_cleanup": inventory(root), "at_unix_ns": time.time_ns()}
    target = run / "cleanup.json"
    target.write_text(json.dumps({**receipt, "removed": False}, indent=2, sort_keys=True) + "\n")
    shutil.rmtree(root)
    if root.exists():
        raise RuntimeError("The exact owned scratch remains after cleanup")
    target.write_text(json.dumps({**receipt, "removed": True}, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"removed": True, "scratch": str(root), "receipt": str(target)}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    args = parser.parse_args()
    run = args.run.resolve(strict=True)
    cleanup(run)


if __name__ == "__main__":
    main()
