#!/usr/bin/env python3
"""Use the existing real-daemon measurement with an explicit publication control."""

import argparse
import json
import os
from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "native-log-tail-20261021"))
import capture as previous

FLAG = "LASTDB_SERIAL_LOG_PUBLICATION_DIAGNOSTIC"


def main():
    parser = argparse.ArgumentParser(description=__doc__, add_help=False)
    parser.add_argument("--serial-publication", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args, remaining = parser.parse_known_args()
    os.environ.pop(FLAG, None)
    if args.serial_publication:
        os.environ[FLAG] = "1"
    sys.argv = [sys.argv[0], "--output", str(args.output), *remaining]
    try:
        previous.main()
    finally:
        path = args.output / "inputs.json"
        if path.exists():
            inputs = json.loads(path.read_text())
            inputs["serial_publication_diagnostic"] = args.serial_publication
            path.write_text(json.dumps(inputs, indent=2) + "\n")
    measured = json.loads((args.output / "writes.json").read_text())
    enabled = measured["after"]["native_disk"]["concurrent_journal_publication"]
    if enabled == args.serial_publication:
        raise RuntimeError("the actual daemon publication mode differs from its requested control")


if __name__ == "__main__":
    main()
