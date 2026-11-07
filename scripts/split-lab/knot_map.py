#!/usr/bin/env python3
"""Map the module dependency knot of one Rust crate.

The tool builds the file reference graph with split_measure.Crate, groups it
by module prefix (depth 1 or 2), and reports:
- the strongly connected components (the knots);
- for the largest knot, the weakest edges (few references): the cheapest cuts;
- the outside crates each module names (`--extern fold_db` counts
  `fold_db::<top>` paths, to show which core modules a host crate reaches).

The parse is regex based and approximate, like split_measure.py.

Usage:
    knot_map.py --src lastdb_node/src --depth 1 [--extern fold_db] [--json out.json]
"""

import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from split_measure import Crate, module_of, sccs  # noqa: E402


def group(rel, depth):
    mod = module_of(rel)
    return "::".join(mod[:depth]) if mod else "<root>"


def layer_order(comp, edges):
    """Order one knot bottom first so that the upward (back) references are few.

    Greedy Eades-Lin-Smyth on reference weights, then a local swap pass. An edge
    a -> b means a uses b, so b belongs below a; a back edge points upward.
    """
    nodes = set(comp)
    w = {(a, b): c for a in nodes for b, c in edges[a].items() if b in nodes}

    def out_w(v, live):
        return sum(c for (a, b), c in w.items() if a == v and b in live)

    def in_w(v, live):
        return sum(c for (a, b), c in w.items() if b == v and a in live)

    live = set(nodes)
    top, bottom = [], []
    while live:
        changed = True
        while changed:
            changed = False
            for v in sorted(live):
                if out_w(v, live) == 0:  # uses nothing live: a bottom layer
                    bottom.append(v)
                    live.discard(v)
                    changed = True
                elif in_w(v, live) == 0:  # nothing live uses it: a top layer
                    top.append(v)
                    live.discard(v)
                    changed = True
        if live:
            v = max(sorted(live), key=lambda x: in_w(x, live) - out_w(x, live))
            bottom.append(v)
            live.discard(v)
    order = bottom + top[::-1]

    def cost(seq):
        pos = {v: i for i, v in enumerate(seq)}
        return sum(c for (a, b), c in w.items() if pos[b] > pos[a])

    best = cost(order)
    improved = True
    while improved:
        improved = False
        for i in range(len(order)):
            for j in range(len(order)):
                if i == j:
                    continue
                seq = order[:]
                seq.insert(j, seq.pop(i))
                c = cost(seq)
                if c < best:
                    order, best, improved = seq, c, True
    pos = {v: i for i, v in enumerate(order)}
    back = [(c, a, b) for (a, b), c in w.items() if pos[b] > pos[a]]
    return order, back


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    ap.add_argument("--depth", type=int, default=1)
    ap.add_argument("--extern", dest="extern", default=None)
    ap.add_argument("--top", type=int, default=30)
    ap.add_argument("--json")
    args = ap.parse_args()

    crate = Crate(args.src)
    edges = defaultdict(Counter)
    lines = Counter()
    for rel in crate.files:
        a = group(rel, args.depth)
        lines[a] += crate.lines[rel]
        for target, _name in crate.references(rel):
            b = group(target, args.depth)
            if a != b:
                edges[a][b] += 1
    nodes = sorted(lines)
    comps = sorted(sccs(nodes, {k: list(v) for k, v in edges.items()}), key=len, reverse=True)
    total = sum(lines.values())
    print(f"modules={len(nodes)} lines={total} edges={sum(len(v) for v in edges.values())} knots={len(comps)}")
    for i, comp in enumerate(comps[:5]):
        cl = sum(lines[m] for m in comp)
        print(f"\n# knot {i}: {len(comp)} modules, {cl} lines ({100 * cl // max(total, 1)}%)")
        print("  " + " ".join(comp))
    weak = []
    if comps:
        big = set(comps[0])
        for a in big:
            for b, c in edges[a].items():
                if b in big:
                    weak.append((c, a, b))
        weak.sort()
        print(f"\n# weakest edges inside knot 0 (top {args.top}): refs  from -> to")
        for c, a, b in weak[: args.top]:
            print(f"  {c:<4} {a} -> {b}")
        print(f"\n# heaviest edges inside knot 0 (top {args.top})")
        for c, a, b in sorted(weak, reverse=True)[: args.top]:
            print(f"  {c:<4} {a} -> {b}")

    order, back = [], []
    if comps:
        order, back = layer_order(comps[0], edges)
        cut = sum(c for c, _, _ in back)
        inside = sum(c for c, _, _ in weak)
        print(f"\n# layer order for knot 0 (bottom first); back edges={len(back)} refs={cut} of {inside}")
        print("  " + " < ".join(order))
        for c, a, b in sorted(back, reverse=True)[: args.top]:
            print(f"  back {c:<4} {a} -> {b}")

    ext = defaultdict(Counter)
    if args.extern:
        rx = re.compile(r"\b" + re.escape(args.extern) + r"::(\{[^}]*\}|[A-Za-z_][A-Za-z0-9_]*)")
        for rel, text in crate.files.items():
            a = group(rel, args.depth)
            for m in rx.finditer(text):
                tok = m.group(1)
                if tok.startswith("{"):
                    for part in re.findall(r"(?:^|[{,])\s*([A-Za-z_][A-Za-z0-9_]*)", tok):
                        ext[a][part] += 1
                else:
                    ext[a][tok] += 1
        reach = Counter()
        for a, c in ext.items():
            for t in c:
                reach[t] += 1
        print(f"\n# {args.extern} top modules: count of host modules that name each")
        for t, c in reach.most_common(args.top):
            print(f"  {c:<4} {t}")
        print(f"\n# host modules by count of distinct {args.extern} modules named")
        for a, c in sorted(ext.items(), key=lambda kv: -len(kv[1]))[: args.top]:
            print(f"  {len(c):<4} {a:<28} lines={lines[a]}")

    if args.json:
        with open(args.json, "w") as f:
            json.dump(
                {
                    "lines": lines,
                    "edges": {a: dict(v) for a, v in edges.items()},
                    "knots": comps,
                    "weak": weak,
                    "extern": {a: dict(v) for a, v in ext.items()},
                },
                f,
                indent=1,
            )


if __name__ == "__main__":
    main()
