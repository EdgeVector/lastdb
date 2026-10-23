#!/usr/bin/env python3
"""Package this completed round with the frozen first-round evidence tools."""

import hashlib
import json
from reuse import ROOT, original


def main():
    for name in ("compress_receipts", "compact_receipts"):
        tool = original(name)
        tool.ROOT = ROOT
        tool.main()
    index = ROOT / "evidence-index.json"
    value = json.loads(index.read_text())
    value["packaging_helpers_sha256"] = {
        str(path): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (ROOT / "package_receipts.py",
                     ROOT.parent / "journal-removal-benchmark-20261022" / "compress_receipts.py",
                     ROOT.parent / "journal-removal-benchmark-20261022" / "compact_receipts.py")
    }
    index.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
