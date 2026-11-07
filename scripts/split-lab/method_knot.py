#!/usr/bin/env python3
"""Map the method-call knot inside one service type (a god object).

Path references miss method calls: `store.commit_tip_changes(..)` names no
file. This tool finds every `impl <Type>` block, maps each method name that
only one type defines to its file, and then finds the `.name(` and
`Type::name(` / `Self::name(` calls in every file. It groups files by module
prefix (`--depth`) and reports the knots of that call graph.

Usage:
    method_knot.py --src fold_db/crates/core/src --type AtomStore --depth 3
"""

import argparse
import os
import re
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from knot_map import group, layer_order  # noqa: E402
from model_behavior import top_items  # noqa: E402
from split_measure import Crate, sccs  # noqa: E402

FN = re.compile(r"\bfn\s+([A-Za-z_][A-Za-z0-9_]*)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    ap.add_argument("--type", dest="types", required=True, help="comma list")
    ap.add_argument("--depth", type=int, default=3)
    ap.add_argument("--top", type=int, default=25)
    args = ap.parse_args()
    wanted = set(args.types.split(","))
    crate = Crate(args.src)
    owners = defaultdict(set)  # method name -> set of types
    defined = {}  # (type, name) -> rel
    for rel, text in crate.files.items():
        for kind, name, _trait, body in top_items(text):
            if kind != "impl" or not name:
                continue
            for m in FN.finditer(body):
                owners[m.group(1)].add(name)
                if name in wanted:
                    defined.setdefault((name, m.group(1)), rel)
    methods = {n: rel for (t, n), rel in defined.items() if len(owners[n]) == 1}
    if not methods:
        print("no methods")
        return
    call = re.compile(r"(?:\.|::)(" + "|".join(sorted(map(re.escape, methods))) + r")\s*(?:::<[^>]*>)?\(")
    edges = defaultdict(Counter)
    lines = Counter()
    users = Counter()
    for rel, text in crate.files.items():
        a = group(rel, args.depth)
        lines[a] += crate.lines[rel]
        for m in call.finditer(text):
            target = methods[m.group(1)]
            b = group(target, args.depth)
            if a != b:
                edges[a][b] += 1
                users[b] += 0
            users[(a, b)] += 1
    nodes = sorted(set(lines))
    comps = sorted(sccs(nodes, {k: list(v) for k, v in edges.items()}), key=len, reverse=True)
    owned = sorted({group(r, args.depth) for r in methods.values()})
    print(f"types={','.join(sorted(wanted))} unique_methods={len(methods)} defining_groups={len(owned)} knots={len(comps)}")
    for i, comp in enumerate(comps[:3]):
        cl = sum(lines[m] for m in comp)
        print(f"\n# call knot {i}: {len(comp)} groups, {cl} lines")
        print("  " + " ".join(comp))
    if comps:
        order, back = layer_order(comps[0], edges)
        inside = sum(c for a in comps[0] for b, c in edges[a].items() if b in comps[0])
        print(f"\n# layer order for call knot 0; back edges={len(back)} calls={sum(c for c, _, _ in back)} of {inside}")
        print("  " + " < ".join(order))
        for c, a, b in sorted(back, reverse=True)[: args.top]:
            print(f"  back {c:<4} {a} -> {b}")
    fan = Counter()
    for a, row in edges.items():
        for b in row:
            fan[b] += 1
    print(f"\n# groups whose methods most other groups call (fan-in, top {args.top})")
    for b, c in fan.most_common(args.top):
        print(f"  {c:<4} {b}")


if __name__ == "__main__":
    main()
