#!/usr/bin/env python3
"""Check the last stdout line of signed_history_relay_e2e.

Usage: check_relay_e2e.py STDOUT_FILE [LABEL]

Fails unless the bin printed a JSON object as its last line with ok=true,
grant_revoke_ok=true, and upload_seconds / restore_seconds both under the
limit. Prints a one-line verdict.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

LIMIT_SECONDS = 60.0


def last_json(path: Path) -> dict:
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not lines:
        raise SystemExit(f"{path}: no stdout from signed_history_relay_e2e")
    try:
        report = json.loads(lines[-1])
    except json.JSONDecodeError as err:
        raise SystemExit(f"{path}: last stdout line is not JSON: {err}") from err
    if not isinstance(report, dict):
        raise SystemExit(f"{path}: last stdout line is not a JSON object")
    return report


def problems(report: dict) -> list[str]:
    found: list[str] = []
    if report.get("ok") is not True:
        found.append(f"ok is {report.get('ok')!r}: {report.get('error')}")
    if report.get("grant_revoke_ok") is not True:
        found.append("grant_revoke_ok is not true")
    for key in ("upload_seconds", "restore_seconds"):
        value = report.get(key)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            found.append(f"{key} is missing or not a number")
        elif value >= LIMIT_SECONDS:
            found.append(f"{key}={value:.3f} is not under {LIMIT_SECONDS:.0f}s")
    return found


def main() -> int:
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    label = sys.argv[2] if len(sys.argv) > 2 else "relay"
    report = last_json(Path(sys.argv[1]))
    found = problems(report)
    verdict = "FAIL" if found else "PASS"
    line = (
        f"{verdict} {label} upload={report.get('upload_seconds')} "
        f"restore={report.get('restore_seconds')} bytes={report.get('bytes_uploaded')} "
        f"streams={report.get('streams_discovered')} grant_revoke_ok={report.get('grant_revoke_ok')}"
    )
    print(line)
    for item in found:
        print(f"  - {item}")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
