#!/usr/bin/env python3
"""Show what a proposed module set references outside itself.

A proposed set is a list of path prefixes (relative to --src). The tool lists
each outside target file and item that the set names, so the extraction cost
of the set is visible before a move.

Usage:
    closure.py --src fold_db/crates/core/src --set atom/,schema/types/,canonical/
"""

import argparse
import os
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from split_measure import Crate  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    ap.add_argument("--set", required=True)
    args = ap.parse_args()
    prefixes = [p for p in args.set.split(",") if p]
    crate = Crate(args.src)
    inside = {r for r in crate.files if any(r == p or r.startswith(p) for p in prefixes)}
    lines = sum(crate.lines[r] for r in inside)
    out = defaultdict(Counter)
    for rel in sorted(inside):
        for target, name in crate.references(rel):
            if target not in inside:
                out[target][name.split("::")[-1]] += 1
    print(f"set files={len(inside)} lines={lines} outside_targets={len(out)} refs={sum(sum(c.values()) for c in out.values())}")
    for target, c in sorted(out.items(), key=lambda kv: -sum(kv[1].values())):
        names = ", ".join(f"{n}:{k}" for n, k in c.most_common(6))
        print(f"  {sum(c.values()):<4} {target:<62} {names}")


if __name__ == "__main__":
    main()
