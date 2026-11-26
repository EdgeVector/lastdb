#!/usr/bin/env python3
"""Refuse a breaking change to the relay contract.

docs/designs/relay-protocol.openapi.json is generated from the relay_protocol crate (the
`relay_contract` job checks that first). This compares the base commit's file with the
generated one using a pinned oasdiff and fails on any change it rates WARN or ERR: within
protocol 1 a change may only add. The label `relay-contract-break-approved` lets a reviewed
break through, for example removing /relay/v1/ once /relay/v2/ serves every client.

A base file that was not generated (written by hand, before the crate) is not compared: there
is no earlier contract of the same kind to break.
"""

import argparse
import subprocess
import sys
import tempfile
from pathlib import Path

CONTRACT = "docs/designs/relay-protocol.openapi.json"
GENERATED_MARK = "Generated from the relay_protocol crate"


def base_file(base: str) -> str | None:
    shown = subprocess.run(
        ["git", "show", f"{base}:{CONTRACT}"], capture_output=True, text=True, check=False
    )
    return shown.stdout if shown.returncode == 0 else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True, help="base commit to compare against")
    parser.add_argument("--head-file", required=True, type=Path, help="generated contract")
    parser.add_argument("--oasdiff", required=True, help="path to the pinned oasdiff binary")
    parser.add_argument("--break-approved", action="store_true",
                        help="the pull request carries relay-contract-break-approved")
    args = parser.parse_args()

    if not args.base or set(args.base) == {"0"}:
        print("relay contract: no base commit; nothing to compare")
        return 0
    text = base_file(args.base)
    if text is None:
        print(f"relay contract: {args.base} has no {CONTRACT}; nothing to compare")
        return 0
    if GENERATED_MARK not in text:
        print(f"relay contract: the base file at {args.base} was written by hand; "
              "this change makes it generated, so there is no generated contract to compare")
        return 0

    with tempfile.TemporaryDirectory() as scratch:
        base_path = Path(scratch) / "base.json"
        base_path.write_text(text, encoding="utf-8")
        result = subprocess.run(
            [args.oasdiff, "breaking", str(base_path), str(args.head_file),
             "--fail-on", "WARN", "--format", "text"],
            capture_output=True, text=True, check=False,
        )
    report = (result.stdout + result.stderr).strip()
    if report:
        print(report)
    if result.returncode == 0:
        print("relay contract: additions only")
        return 0
    if args.break_approved:
        print("relay contract: breaking change approved by label relay-contract-break-approved")
        return 0
    print("::error::The relay contract changed in a way that breaks protocol 1 clients. Only "
          "additions are allowed under /relay/v1/. A breaking change is protocol 2 "
          "(/relay/v2/); removing v1 needs the label relay-contract-break-approved.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
