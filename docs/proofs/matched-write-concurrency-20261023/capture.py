#!/usr/bin/env python3
"""Measure equal-sized real durable requests with one or four independent clients."""

import argparse
import gzip
import hashlib
import importlib.util
import json
import os
import platform
from pathlib import Path
import shutil
import socket
import sys
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
SPEC = importlib.util.spec_from_file_location(
    "queue_capture", HERE.parent / "eager-durable-queue-20261023/run.py")
support = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(support)
save, schemas = support.save, support.schemas


class Node(support.Node):
    def start(self):
        super().start()
        self.pids.append(self.process.pid)
        save(self.output / "processes.json", self.pids)


def cleanup(home, node, owner):
    info = home.stat()
    if (home.is_symlink() or home.resolve() != home
            or (info.st_dev, info.st_ino, info.st_uid) != (owner["device"], owner["inode"], os.getuid())
            or json.loads((home / "capture-owner.json").read_text()) != owner
            or node.process.poll() is None):
        raise RuntimeError("retain the exact home: ownership or process state differs")
    for pid in node.pids:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            continue
        raise RuntimeError("retain the home: a recorded process still exists")
    for name in ("folddb.sock", "folddb-full.sock"):
        probe = socket.socket(socket.AF_UNIX)
        probe.settimeout(.25)
        try:
            probe.connect(str(home / "data" / name))
        except (FileNotFoundError, ConnectionRefusedError):
            pass
        else:
            raise RuntimeError("retain the home: an active socket exists")
        finally:
            probe.close()
    save(node.output / "cleanup.json", {"owner": owner, "pids": node.pids, "removed": False})
    shutil.rmtree(home)
    save(node.output / "cleanup.json", {"owner": owner, "pids": node.pids, "removed": not home.exists()})


def verify_refs(node, output):
    cursor = None
    for _ in range(256):
        request = {"max_pages": 64, "max_ms": 3000, "pause_ms": 0}
        if cursor:
            request["cursor"] = cursor
        result = node.call("/api/db/verify-refs", request)["verify_refs"]
        if result.get("done"):
            save(output / "atom-counters.json", result)
            if result.get("mismatches_total") != 0 or result.get("tally_complete") is not True:
                raise RuntimeError("the complete product atom-counter diagnostic differs")
            return
        following = result.get("next_cursor")
        if not following or following == cursor:
            raise RuntimeError("the diagnostic cursor did not advance")
        cursor = following
    raise RuntimeError("the product diagnostic exceeds the bounded capture")


def run(args):
    root = args.scratch.resolve(strict=True)
    if root.stat().st_uid != os.getuid():
        raise RuntimeError("scratch owner differs")
    output = args.output.resolve()
    if output.is_relative_to(ROOT.resolve()) or root.is_relative_to(ROOT.resolve()):
        raise RuntimeError("capture output and generated homes must stay outside the source tree")
    output.mkdir()
    home = root / "home"
    home.mkdir()
    info = home.stat()
    owner = {"home": str(home), "device": info.st_dev, "inode": info.st_ino,
             "uid": os.getuid(), "primary_data": False, "created_ns": time.time_ns()}
    save(home / "capture-owner.json", owner)
    save(output / "ownership.json", owner)
    if len(str(home / "data/folddb-full.sock").encode()) > 103:
        raise RuntimeError("the generated socket path exceeds the macOS bound")
    binary = args.binary.resolve(strict=True)
    node = Node(binary, home, root / "daemon.log", 4096, False, workers=8)
    node.profile, node.maintenance, node.pids, node.output = args.profile, False, [], output
    workload = argparse.Namespace(output=output, count=args.count, schemas=64,
                                  clients=args.clients, batch_size=8)
    save(output / "inputs.json", {"source": args.revision, "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "clients": args.clients, "records_per_request": 8, "requests": args.count, "schemas": 64,
        "value_bytes": 64, "workers": 8, "profile": args.profile, "auto_maintenance": False,
        "durability_parameter": "omitted; durable receipt required", "generated_data_only": True,
        "load_before": os.getloadavg(), "platform": platform.system(),
        "github_actions": True, "github_run_id": os.environ.get("GITHUB_RUN_ID"),
        "host_activity_controlled": False, "request_mode": "closed loop; all requests retained"})
    complete = False
    try:
        node.start()
        save(output / "version.json", node.call("/api/version"))
        schemas.seed(node, 64, output)
        expected = schemas.measured_calls(node, workload)
        result = schemas.check_values(node, expected)
        save(output / "readback.json", result)
        if not result["all_values_match"]:
            raise RuntimeError("acknowledged known-key values differ")
        support.recover(node, workload, expected)
        if args.profile:
            verify_refs(node, output)
        save(output / "shutdown.json", {"exit_code": node.stop()})
        flushed = json.loads((home / ".shutdown_flush_ready").read_text())
        save(output / "native-shutdown.json", flushed)
        if not flushed["flush_ok"]:
            raise RuntimeError("the final disk flush failed")
        complete = True
    except Exception as error:
        save(output / "failure.json", {"error": str(error), "type": type(error).__name__})
        raise
    finally:
        node.stop()
        node.log.close()
        support.old.preserve_log(root, "daemon", output)
        save(output / "summary.json", {"complete": complete, "pids": node.pids,
                                       "all_owned_processes_stopped": node.process is None or node.process.poll() is not None})
        if complete:
            cleanup(home, node, owner)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--clients", type=int, choices=(1, 4), required=True)
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--profile", action="store_true")
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
        raise RuntimeError("LastDB captures run only on GitHub-hosted Actions runners")
    if args.count < 64 or args.count % args.clients:
        raise RuntimeError("use complete equal client assignments")
    run(args)


if __name__ == "__main__":
    main()
