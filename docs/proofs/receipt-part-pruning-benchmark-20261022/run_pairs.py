#!/usr/bin/env python3
"""Run the frozen three-pair coordinator against this round and exact source commits."""

import argparse
import os
from pathlib import Path
import sys

sys.dont_write_bytecode = True
from reuse import ROOT, original, validate_revisions


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host-lock", required=True, type=Path)
    parser.add_argument("--before-revision", required=True)
    parser.add_argument("--after-revision", required=True)
    args = parser.parse_args()
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise ValueError("Use the standard external host Cargo lock for every measured run")
    validate_revisions(ROOT, args.before_revision, args.after_revision)
    coordinator = original("run_pairs")
    coordinator.PROOF = ROOT
    previous = sys.argv
    sys.argv = [str(ROOT / "run_pairs.py"), "--host-lock", str(args.host_lock)]
    try:
        return coordinator.main()
    finally:
        sys.argv = previous


if __name__ == "__main__":
    raise SystemExit(main())
