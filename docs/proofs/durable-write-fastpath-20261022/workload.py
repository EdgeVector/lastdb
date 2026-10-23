#!/usr/bin/env python3
"""Capture real concurrent batch requests while explicit compaction runs."""

import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys

sys.dont_write_bytecode = True
PROOF = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("fastpath_benchmark", PROOF / "benchmark.py")
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)
capture = benchmark.capture
base = capture.base


class Node(capture.Node):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.profile, self.maintenance = True, True


def main():
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise RuntimeError("hold the compiler exclusion for the complete measurement")
    # Use the existing daemon workload and restart checks, with numeric phase
    # capture and durable notification replay for the improved binary.
    base.Node = Node
    base.schemas.recovery = benchmark.recovery_with_notifications
    output = Path(sys.argv[sys.argv.index("--output") + 1])
    home = Path(sys.argv[sys.argv.index("--home") + 1])
    log = home.parent / (home.name + ".log")
    try:
        base.main()
    finally:
        if output.is_dir() and log.is_file():
            raw = log.read_bytes()
            packed = gzip.compress(raw, mtime=0)
            (output / "daemon.txt.gz").write_bytes(packed)
            capture.save(output / "log-receipt.json", {
                "original_bytes": len(raw), "gzip_bytes": len(packed),
                "original_sha256": hashlib.sha256(raw).hexdigest(),
                "gzip_sha256": hashlib.sha256(packed).hexdigest()})


if __name__ == "__main__":
    main()
