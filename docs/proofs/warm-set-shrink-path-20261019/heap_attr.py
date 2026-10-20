#!/usr/bin/env python3
"""Attribute an inverted malloc_history call tree: for each allocation path,
charge its bytes to the first frame (from the leaf) that matches one of the
interesting module patterns. Prints the top frames by bytes."""
import re
import sys
from collections import defaultdict

LINE = re.compile(r'^(?P<pre>[ +!:|]*)(?P<count>\d+) \((?P<size>[\d.]+)(?P<unit>[KMG]?)\) (?P<sym>.+?)  \(in ')
UNIT = {'': 1, 'K': 1024, 'M': 1024 ** 2, 'G': 1024 ** 3}
pattern = re.compile(sys.argv[2] if len(sys.argv) > 2 else r'fold_db::')

totals = defaultdict(lambda: [0, 0.0])
stack = []  # (depth, attributed)
with open(sys.argv[1]) as f:
    for raw in f:
        m = LINE.match(raw.rstrip('\n'))
        if not m:
            continue
        depth = len(m.group('pre'))
        while stack and stack[-1][0] >= depth:
            stack.pop()
        attributed = any(a for _, a in stack)
        sym = m.group('sym')
        hit = (not attributed) and pattern.search(sym)
        if hit:
            short = re.sub(r'::h[0-9a-f]{16}$', '', sym)
            short = short.replace('$LT$', '<').replace('$GT$', '>').replace('$u20$', ' ').replace('..', '::')
            totals[short][0] += int(m.group('count'))
            totals[short][1] += float(m.group('size')) * UNIT[m.group('unit')]
        stack.append((depth, bool(hit) or attributed))

for name, (count, size) in sorted(totals.items(), key=lambda kv: -kv[1][1])[:40]:
    print(f'{size/1e6:8.2f} MB {count:8d}  {name[:200]}')
