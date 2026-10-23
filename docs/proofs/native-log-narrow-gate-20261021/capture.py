#!/usr/bin/env python3
"""Measure the real daemon with the former publication gate as a control."""

import argparse
import json
import os
from pathlib import Path
import signal
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "native-log-tail-20261021"))
import capture as previous

FLAG = "LASTDB_SERIAL_PREFIX_INSTALL_DIAGNOSTIC"


def main():
    parser = argparse.ArgumentParser(description=__doc__, add_help=False)
    parser.add_argument("--serial-install", action="store_true")
    parser.add_argument("--flush-before-stop", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args, remaining = parser.parse_known_args()
    os.environ.pop("LASTDB_SERIAL_LOG_PUBLICATION_DIAGNOSTIC", None)
    os.environ.pop(FLAG, None)
    if args.serial_install:
        os.environ[FLAG] = "1"

    class Node(previous.Node):
        def stop(self, sig=signal.SIGTERM):
            if sig == signal.SIGKILL and args.flush_before_stop:
                receipt = self.call("/api/db/flush", {})
                previous.save(args.output / "explicit-flush.json", receipt)
            return super().stop(sig)

        def stopped_boundaries(self):
            receipt = super().stopped_boundaries()
            tail = (self.home / "data/disk-pack/JOURNAL_TAIL").read_bytes()
            receipt["tail_first_batch"] = int.from_bytes(tail[32:40], "little")
            receipt["tail_encoded_bytes"] = int.from_bytes(tail[48:56], "little")
            receipt["tail_retains_published_prefix"] = (
                receipt["tail_first_batch"] <= receipt["published_batch"])
            return receipt

    previous.Node = Node
    sys.argv = [sys.argv[0], "--output", str(args.output), *remaining]
    try:
        previous.main()
    finally:
        path = args.output / "inputs.json"
        if path.exists():
            inputs = json.loads(path.read_text())
            inputs.update(serial_install_diagnostic=args.serial_install,
                          flush_before_forced_stop=args.flush_before_stop)
            previous.save(path, inputs)
    native = json.loads((args.output / "writes.json").read_text())["after"]["native_disk"]
    if (not native["concurrent_journal_publication"]
            or native["concurrent_journal_installation"] == args.serial_install):
        raise RuntimeError("the actual daemon mode differs from its requested control")


if __name__ == "__main__":
    main()
