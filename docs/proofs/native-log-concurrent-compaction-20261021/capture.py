#!/usr/bin/env python3
"""Real daemon compaction/write measurements on fresh generated homes only."""

import argparse
import json
import os
from pathlib import Path
import signal
import sys
import threading
import time

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "native-log-tail-20261021"))
import capture as previous

FLAG = "LASTDB_SERIAL_COMPACTION_DIAGNOSTIC"


def main():
    parser = argparse.ArgumentParser(description=__doc__, add_help=False)
    parser.add_argument("--serial-compaction", action="store_true")
    parser.add_argument("--flush-before-stop", action="store_true")
    parser.add_argument("--stop-during-copy", action="store_true")
    parser.add_argument("--disk-cap-bytes", type=int)
    parser.add_argument("--output", type=Path, required=True)
    args, remaining = parser.parse_known_args()
    for flag in (FLAG, "LASTDB_SERIAL_PREFIX_INSTALL_DIAGNOSTIC", "LASTDB_SERIAL_LOG_PUBLICATION_DIAGNOSTIC", "LASTDB_DISK_CAP_BYTES"):
        os.environ.pop(flag, None)
    if args.serial_compaction:
        os.environ[FLAG] = "1"
    if args.disk_cap_bytes:
        os.environ["LASTDB_DISK_CAP_BYTES"] = str(args.disk_cap_bytes)

    class Node(previous.Node):
        def stop(self, sig=signal.SIGTERM):
            thread = None
            if sig == signal.SIGKILL and args.flush_before_stop:
                previous.save(args.output / "explicit-flush.json", self.call("/api/db/flush", {}))
            if sig == signal.SIGKILL and args.stop_during_copy:
                outcome = {}
                packs = self.home / "data/disk-pack/packs"
                earlier = {path.name for path in packs.iterdir()}

                def compact():
                    try:
                        outcome["response"] = self.call("/api/db/compact", {"dry_run": False, "grace_ms": 0})
                    except Exception as error:
                        outcome["connection_end"] = type(error).__name__

                thread = threading.Thread(target=compact, daemon=True)
                thread.start()
                deadline = time.monotonic() + 45
                while time.monotonic() < deadline:
                    status = self.call("/api/db/native-status")
                    visible = any(path.parent.name not in earlier and path.stat().st_size > 128
                                  for path in packs.glob("*/part-*"))
                    if status["native_disk"]["compaction_copying"] and visible:
                        previous.save(args.output / "copy-before-forced-stop.json", {
                            "observed_unix_us": time.time_ns() // 1000,
                            "new_pack_data_visible": visible, "native_status": status})
                        break
                    time.sleep(.01)
                else:
                    raise RuntimeError("the owned daemon never reported an active compaction copy")
            code = super().stop(sig)
            if thread:
                thread.join(timeout=10)
                previous.save(args.output / "interrupted-compact-call.json", outcome)
            return code

        def stopped_boundaries(self):
            receipt = super().stopped_boundaries()
            tail = (self.home / "data/disk-pack/JOURNAL_TAIL").read_bytes()
            receipt["tail_first_batch"] = int.from_bytes(tail[32:40], "little")
            return receipt

    previous.Node = Node
    sys.argv = [sys.argv[0], "--output", str(args.output), *remaining]
    try:
        previous.main()
    finally:
        path = args.output / "inputs.json"
        if path.exists():
            inputs = json.loads(path.read_text())
            inputs.update(serial_compaction=args.serial_compaction,
                          stop_during_copy=args.stop_during_copy,
                          flush_before_stop=args.flush_before_stop,
                          disk_cap_bytes=args.disk_cap_bytes)
            previous.save(path, inputs)
    native = json.loads((args.output / "writes.json").read_text())["after"]["native_disk"]
    if (not native["concurrent_journal_publication"] or not native["concurrent_journal_installation"]
            or native["concurrent_compaction"] == args.serial_compaction):
        raise RuntimeError("the actual daemon mode differs from the requested compaction mode")


if __name__ == "__main__":
    main()
