#!/usr/bin/env python3
"""Compose two candidate splits: an outer split, and an inner split for one outer unit.

Each file keeps its outer unit, except the files of the outer unit named by
--split. Those files take the unit of the inner candidate, with a prefix.
The result lists each file by its exact path, so it is a plain candidate.

Usage:
    compose_candidates.py --src fold_db/crates/core/src \
        --outer candidates/40-store-substrate.json --split database \
        --inner candidates/20-canonical-levels.json --prefix db_ --out composed.json
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from split_measure import Crate, assign  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    ap.add_argument("--outer", required=True)
    ap.add_argument("--inner", required=True)
    ap.add_argument("--split", required=True, help="the outer unit that the inner candidate divides")
    ap.add_argument("--prefix", default="db_")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    outer = json.load(open(args.outer))
    inner = json.load(open(args.inner))
    crate = Crate(args.src)

    units = {}
    for rel in sorted(crate.files):
        name = assign(rel, outer["units"])
        if name == args.split:
            name = args.prefix + assign(rel, inner["units"])
        units.setdefault(name, []).append(rel)

    allow = {u: deps for u, deps in outer.get("allow", {}).items() if u != args.split}
    split_deps = outer.get("allow", {}).get(args.split, [])
    for u, deps in inner.get("allow", {}).items():
        allow[args.prefix + u] = [args.prefix + d for d in deps] + list(split_deps)
    composed = {
        "name": f"{outer.get('name')}+{inner.get('name')}",
        "units": [{"name": n, "paths": p} for n, p in sorted(units.items())],
        "allow": allow,
    }
    with open(args.out, "w") as f:
        json.dump(composed, f, indent=1)
    print({n: len(p) for n, p in sorted(units.items())})


if __name__ == "__main__":
    main()
