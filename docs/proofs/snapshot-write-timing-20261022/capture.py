#!/usr/bin/env python3
"""Measure real durable writes with numeric phase capture on fresh generated homes."""

import argparse
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[3]
PROOF = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location(
    "snapshot_capture", PROOF.parent / "snapshot-durable-rebase-20261022/capture.py")
base = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(base)
save = base.save


class Node(base.Node):
    def start(self):
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(("LASTDB_", "FOLDDB_"))
               and key not in ("OBS_SENTRY_DSN", "SENTRY_DSN")}
        env.update(LASTDB_ISOLATED_COPY="1", LASTDB_SUPPRESS_RECOVERY_PHRASE="1",
                   FOLDDB_DISABLE_KEYCHAIN="1", LASTDB_UDS_WORKERS="8",
                   LASTDB_DEPENDENCY_HIGH_WATER="4096", LASTDB_AUTO_MAINTENANCE_START_DELAY_SECS="0",
                   LASTDB_AUTO_MAINTENANCE=str(int(self.maintenance)),
                   LASTDB_SNAPSHOT_TIMING=str(int(self.profile)),
                   RUST_MIN_STACK="67108864", RUST_LOG="warn")
        self.process = subprocess.Popen([str(self.binary), "--data-dir", str(self.home)],
                                        env=env, stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"owned daemon exited {self.process.returncode}")
            try:
                self.call("/api/db/native-status")
                return
            except (OSError, TimeoutError):
                time.sleep(.1)
        raise RuntimeError("owned daemon did not become ready")


def capture(binary, scratch, name, profile, maintenance, number):
    output = PROOF / name
    output.mkdir()
    home = scratch / ("h" + str(number))
    home.mkdir()
    args = argparse.Namespace(output=output, count=1000, schemas=64, clients=1, batch_size=1)
    node = Node(binary, home, scratch / (name + ".log"), 4096, False, workers=8)
    node.profile, node.maintenance = profile, maintenance
    save(output / "inputs.json", {"binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
         "home": str(home), "schemas": 64, "calls": 1000, "clients": 1, "value_bytes": 64,
         "batch_size": 1, "request_workers": 8, "high_water": 4096,
         "timing_enabled": profile, "auto_maintenance": maintenance,
         "durability_parameter": "omitted; durable required", "generated_data": True})
    try:
        node.start()
        base.schemas.seed(node, 64, output)
        expected = base.schemas.measured_calls(node, args)
        values = base.schemas.check_values(node, expected)
        save(output / "memory-read.json", values)
        if not values["all_values_match"]:
            raise RuntimeError("known-key values differ")
        base.schemas.recovery(node, args, expected)
        save(output / "shutdown.json", {"exit_code": node.stop()})
        save(output / "native-shutdown.json", json.loads((home / ".shutdown_flush_ready").read_text()))
    except Exception as error:
        save(output / "failure.json", {"type": type(error).__name__, "message": str(error)})
        raise
    finally:
        node.stop()
        node.log.close()
        raw = (scratch / (name + ".log")).read_bytes()
        packed = gzip.compress(raw, mtime=0)
        (output / "daemon.txt.gz").write_bytes(packed)
        save(output / "log-receipt.json", {"original_bytes": len(raw), "gzip_bytes": len(packed),
             "original_sha256": hashlib.sha256(raw).hexdigest(),
             "gzip_sha256": hashlib.sha256(packed).hexdigest()})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    args = parser.parse_args()
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise RuntimeError("hold the host compiler exclusion for the whole capture")
    scratch = args.scratch.resolve()
    if scratch.stat().st_uid != os.getuid() or any(scratch.iterdir()):
        raise RuntimeError("use a fresh owned scratch directory")
    binary = scratch / "candidate"
    shutil.copyfile(ROOT / "target/dev-release/lastdbd", binary)
    binary.chmod(0o755)
    plan = [("control", False, True), ("profile", True, True), ("no-maintenance", True, False)]
    save(PROOF / "plan.json", {"source_head": subprocess.check_output(
         ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
         "source_has_timing_only_changes": True, "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
         "scratch": str(scratch), "uid": os.getuid(), "load_before": os.getloadavg(),
         "order": plan, "cooperative_compilers_excluded": True, "other_host_work_controlled": False})
    outcomes = []
    for number, (name, profile, maintenance) in enumerate(plan):
        print(json.dumps({"starting": name}), flush=True)
        began = time.monotonic()
        capture(binary, scratch, name, profile, maintenance, number)
        outcomes.append({"name": name, "seconds": time.monotonic() - began, "complete": True,
                         "load_after": os.getloadavg()})
        save(PROOF / "outcomes.json", outcomes)


if __name__ == "__main__":
    main()
