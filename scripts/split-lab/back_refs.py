#!/usr/bin/env python3
"""List the file references behind each back edge of a proposed layer order.

Usage:
    back_refs.py --src fold_db/crates/core/src --order crypto,atom,resident,...
"""

import argparse
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from split_measure import Crate, module_of  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    ap.add_argument("--order", required=True, help="comma list, bottom first")
    args = ap.parse_args()
    order = args.order.split(",")
    pos = {m: i for i, m in enumerate(order)}
    crate = Crate(args.src)
    refs = Counter()
    for rel in crate.files:
        a = module_of(rel)[:1]
        a = a[0] if a else "<root>"
        if a not in pos:
            continue
        for target, name in crate.references(rel):
            b = module_of(target)[:1]
            b = b[0] if b else "<root>"
            if b in pos and pos[b] > pos[a]:
                refs[(a, b, rel, target, name.split("::")[-1])] += 1
    for (a, b, rel, target, name), c in sorted(refs.items()):
        print(f"{a:>14} -> {b:<14} {rel} -> {target} :: {name}")


if __name__ == "__main__":
    main()
