#!/usr/bin/env python3
"""Recover the first failed generated home with the supported offline compact command."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import stat
import subprocess
import threading

from product_run import ProductRun, SUPPORT, base

spec = importlib.util.spec_from_file_location("snapshot_cleanup", SUPPORT.with_name("cleanup.py"))
cleanup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cleanup)


def owned_root(run):
    ownership = cleanup.load(run / "ownership.json")
    root = Path(ownership["scratch"])
    temporary = Path(os.environ["TMPDIR"]).resolve(strict=True)
    if (root.is_symlink() or root.resolve(strict=True) != root or root.parent != temporary
            or ownership.get("primary_data") is not False
            or ownership.get("home") != str(root / "home")):
        raise ValueError("Refuse any path except the exact owned temporary home")
    info = root.stat()
    if (info.st_uid != os.getuid() or info.st_uid != ownership["owner_uid"]
            or stat.S_IMODE(info.st_mode) != 0o700
            or info.st_dev != ownership["device"] or info.st_ino != ownership["inode"]
            or cleanup.load(root / ".snapshot-capture-owner.json") != ownership):
        raise ValueError("Scratch ownership metadata differs")
    cleanup.owned_processes(run)
    cleanup.no_active_sockets(root)
    cleanup.inventory(root)
    return root, ownership


class Recovery(ProductRun):
    def __init__(self, args):
        self.root, ownership = owned_root(args.run)
        original = cleanup.load(args.run / "summary.json")
        if original.get("complete") is not False or original.get("all_owned_processes_stopped") is not True:
            raise ValueError("Expected the stopped failed first capture")
        self.binary = args.binary.resolve(strict=True)
        self.maintain = args.maintain.resolve(strict=True)
        self.output = args.run / "recovery"
        self.output.mkdir(mode=0o700, exist_ok=False)
        base.save(self.output / "ownership.json", ownership)
        base.save(self.output / "inputs.json", {
            "source_revision": args.source_revision, "binary_sha256": base.file_hash(self.binary),
            "maintenance_sha256": base.file_hash(self.maintain),
            "driver_sha256": base.file_hash(Path(__file__)), "original_run_complete": False,
        })
        self.identity = ownership["run_id"]
        self.home = self.root / "home"
        self.socket, self.full_socket = self.root / "q.sock", self.root / "f.sock"
        self.process, self.log, self.sequence = None, None, 0
        self.receipt_lock = threading.Lock()
        self.timings, self.compactions = [], []
        self.records = 512
        self.expected = {}
        for number in range(self.records):
            revision = 111 if number < 16 or 32 <= number < 48 else 0
            if 64 <= number < 80:
                revision = 200
            self.expected[base.key(number)] = base.fields(number, revision)
        long_key = "long-0-" + "x" * 1536
        value = base.fields(0, 0)
        value["id"] = long_key
        self.expected[long_key] = value
        self.long_keys = [long_key]

    def recover(self):
        command = [str(self.maintain), "compact", "--home", str(self.home / "data" / "disk-pack"),
                   "--grace-ms", "0"]
        result = subprocess.run(command, text=True, capture_output=True, timeout=180)
        self.remember("offline-recovery-compact", argv=command, exit_code=result.returncode,
                      stdout=result.stdout, stderr=result.stderr)
        if result.returncode:
            raise RuntimeError("The supported offline recovery compact failed")
        self.start()
        self.verify("recovered-accepted-values")
        self.write_long(1, 201)
        self.verify("recovered-new-group-write")
        self.verify_counters()
        self.flush("recovered")
        self.stop()
        self.start()
        self.verify("recovered-reopen")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--maintain", type=Path, required=True)
    parser.add_argument("--source-revision", required=True)
    args = parser.parse_args()
    args.run = args.run.resolve(strict=True)
    capture, complete, error = Recovery(args), False, None
    try:
        capture.recover()
        complete = True
    except Exception as failure:
        error = f"{type(failure).__name__}: {failure}"
        capture.remember("failure", error=error)
    finally:
        try:
            capture.stop()
        except Exception as failure:
            complete, error = False, f"{error or ''}; shutdown: {failure}"
        base.save(capture.output / "summary.json", {
            "complete": complete, "error": error, "original_run_complete": False,
            "scratch": str(capture.root), "all_owned_processes_stopped": capture.process is None,
            "expected_live_records": len(capture.expected), "data_retained_for_review": True,
        })
    print(json.dumps({"complete": complete, "error": error, "output": str(capture.output)}))
    return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())
