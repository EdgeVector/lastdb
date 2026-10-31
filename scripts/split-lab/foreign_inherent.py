#!/usr/bin/env python3
"""List inherent impl blocks outside a proposed set for types defined inside it.

After a move to another crate, such a block breaks the orphan rule (E0116).
Each one is behavior that lives on a model type from outside the model.

Usage:
    foreign_inherent.py --src fold_db/crates/core/src --set atom/,schema/types/
"""

import argparse
import os
import re
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from model_behavior import top_items  # noqa: E402
from split_measure import Crate  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    ap.add_argument("--set", required=True)
    args = ap.parse_args()
    prefixes = [p for p in args.set.split(",") if p]
    crate = Crate(args.src)
    inside = {r for r in crate.files if any(r == p or r.startswith(p) for p in prefixes)}
    defined = set()
    for rel in inside:
        for kind, name, _h, _b in top_items(crate.files[rel]):
            if kind in ("struct", "enum", "union", "trait") and name:
                defined.add(name)
    hits = Counter()
    lines = Counter()
    for rel, text in crate.files.items():
        if rel in inside:
            continue
        for kind, name, trait, body in top_items(text):
            if kind == "impl" and name in defined:
                local_trait = trait and re.sub(r"<.*", "", trait).split("::")[-1].strip()
                if trait and local_trait not in defined:
                    continue  # impl LocalTrait for ModelType: legal in core
                hits[(name, rel, trait or "inherent")] += 1
                lines[name] += body.count("\n") + 1
    print(f"blocks={sum(hits.values())} types={len(lines)} lines={sum(lines.values())}")
    for (name, rel, trait), c in sorted(hits.items()):
        print(f"  {name:<28} {trait[:30]:<30} {rel}")


if __name__ == "__main__":
    main()
