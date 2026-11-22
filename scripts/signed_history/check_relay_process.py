#!/usr/bin/env python3
"""Check a running reference_relay: its peak memory and its stderr request lines.

Usage:
  check_relay_process.py --stderr FILE [--pid PID --max-peak-bytes N]
                         [--expect-line REGEX ...]

--pid and --max-peak-bytes read VmHWM (peak resident memory) from
/proc/PID/status, so this runs on the Linux CI runner only, and must run while
the relay process is still alive. Each --expect-line must match at least one
line of the stderr file (the relay prints one line per request, for example
"relay route=log/read status=200 entries=3").
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


def peak_bytes(pid: int) -> int:
    for line in Path(f"/proc/{pid}/status").read_text(encoding="utf-8").splitlines():
        if line.startswith("VmHWM:"):
            return int(line.split()[1]) * 1024
    raise SystemExit(f"/proc/{pid}/status has no VmHWM line")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stderr", required=True, type=Path)
    parser.add_argument("--pid", type=int)
    parser.add_argument("--max-peak-bytes", type=int)
    parser.add_argument("--expect-line", action="append", default=[])
    args = parser.parse_args()
    if (args.pid is None) != (args.max_peak_bytes is None):
        raise SystemExit("--pid and --max-peak-bytes go together")

    found: list[str] = []
    if args.pid is not None:
        peak = peak_bytes(args.pid)
        print(f"relay peak resident memory {peak} bytes (limit {args.max_peak_bytes})")
        if peak > args.max_peak_bytes:
            found.append(f"peak memory {peak} is over {args.max_peak_bytes}")
    lines = args.stderr.read_text(encoding="utf-8", errors="replace").splitlines()
    for pattern in args.expect_line:
        if not any(re.search(pattern, line) for line in lines):
            found.append(f"no relay stderr line matches {pattern!r}")
    print(f"{len(lines)} relay stderr lines")
    for item in found:
        print(f"FAIL {item}")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
