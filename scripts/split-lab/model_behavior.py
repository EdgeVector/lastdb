#!/usr/bin/env python3
"""Measure how one Rust crate mixes models and behavior.

A model is data plus pure rules: no async, no locks or atomics, no clock,
ids, env, file, network, spawn or global state, and no handle to a stateful
service object. Behavior is everything else.

The tool classifies:
- each struct/enum: `service` when its fields hold a lock, an atomic, a
  channel, a task, or another service type (fixed point); `model` otherwise;
- each impl block: `behavior` when its body has a behavior marker or names a
  service type; `pure` otherwise;
- each file: `pure` when it has no behavior marker and names no service type.

It then reports the edges that stop a model layer from standing alone:
- mixed model types (a model type with behavior impl blocks);
- model items that live in behavior files (movable without a design change);
- pure files that reference a behavior file, split into references to model
  items there (a move fixes them) and to behavior items (a real knot).

The parse is regex based and approximate, like split_measure.py.

Usage:
    model_behavior.py --src fold_db/crates/core/src [--top 25] [--json out.json]
"""

import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from split_measure import EFFECTS, Crate, impl_self_type, module_of  # noqa: E402

IDENT = r"[A-Za-z_][A-Za-z0-9_]*"
MARKERS = {
    "async": re.compile(r"\basync\b|\.await\b"),
    "lock": re.compile(
        r"\b(?:Mutex|RwLock|Condvar|Semaphore|Notify|Barrier|OnceLock|LazyLock|OnceCell)\b"
        r"|\bAtomic(?:U8|U16|U32|U64|Usize|I32|I64|Isize|Bool|Ptr)\b|\bArcSwap\b"
    ),
    "channel": re.compile(r"\b(?:mpsc|oneshot|broadcast|watch)::|\bJoinHandle\b|\bJoinSet\b"),
    "runtime": re.compile(r"\btokio::|\bHandle::current\(|\bblock_on\("),
}
for _name in ("clock", "ids", "env", "globals", "spawn", "net", "fs", "timing"):
    MARKERS[_name] = EFFECTS[_name]

ITEM_HEAD = re.compile(
    r"^[ \t]*(?:pub(?:\([^)]*\))?\s+)?(?:(?:unsafe|async|const|default)\s+)*"
    r"(struct|enum|union|trait|fn|impl)\b",
    re.M,
)


def markers_in(text):
    return sorted(k for k, rx in MARKERS.items() if rx.search(text))


def item_end(text, start):
    """Return the end index of the item that starts at `start` (brace matched, or `;`)."""
    depth = 0
    i = start
    n = len(text)
    while i < n:
        ch = text[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return i + 1
        elif ch == ";" and depth == 0:
            return i + 1
        i += 1
    return n


def top_items(text):
    """Yield (kind, name, header, body) for top-level items, skipping nested ones."""
    pos = 0
    for m in ITEM_HEAD.finditer(text):
        if m.start() < pos:
            continue
        kind = m.group(1)
        end = item_end(text, m.start())
        chunk = text[m.start() : end]
        brace = chunk.find("{")
        header = chunk if brace == -1 else chunk[:brace]
        body = "" if brace == -1 else chunk[brace:]
        name = None
        if kind == "impl":
            hm = re.match(r"\s*(?:pub\S*\s+)?(?:unsafe\s+)?impl\b(.*)", header, re.S)
            h = hm.group(1) if hm else header
            trait = None
            fm = re.search(r"\bfor\b", h)
            if fm:
                trait = re.sub(r"\s+", " ", h[: fm.start()]).strip()
                target = h[fm.end() :]
            else:
                target = h
            name = impl_self_type(target) if not fm else impl_self_type(" " + target.strip())
            yield kind, name, (trait or ""), body
        else:
            nm = re.search(kind + r"\s+(" + IDENT + r")", header)
            name = nm.group(1) if nm else None
            yield kind, name, header, body
        pos = end


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("--json")
    args = ap.parse_args()

    crate = Crate(args.src)
    types = {}  # name -> {"rel", "kind", "fields_text"}
    impls = []  # dict(rel, type, trait, body, lines)
    fns = []  # free functions: dict(rel, name, body)
    file_items = defaultdict(list)
    for rel, text in crate.files.items():
        for kind, name, header, body in top_items(text):
            if not name:
                continue
            if kind in ("struct", "enum", "union"):
                types.setdefault(name, {"rel": rel, "kind": kind, "text": header + body})
                file_items[rel].append(("type", name))
            elif kind == "trait":
                file_items[rel].append(("trait", name))
            elif kind == "impl":
                impls.append(
                    {"rel": rel, "type": name, "trait": header, "body": body, "lines": body.count("\n") + 1}
                )
            elif kind == "fn":
                fns.append({"rel": rel, "name": name, "text": header + body})
                file_items[rel].append(("fn", name))

    # Service types: a field holds a behavior marker or another service type.
    service = set()
    for name, t in types.items():
        if markers_in(t["text"]):
            service.add(name)
    changed = True
    while changed:
        changed = False
        for name, t in types.items():
            if name in service:
                continue
            words = set(re.findall(IDENT, t["text"]))
            if words & service:
                service.add(name)
                changed = True
    service_re = re.compile(r"\b(?:" + "|".join(sorted(map(re.escape, service))) + r")\b") if service else None

    def is_behavior(text):
        found = markers_in(text)
        if service_re and service_re.search(text):
            found.append("service")
        return found

    # Impl blocks.
    per_type = defaultdict(lambda: {"pure": 0, "behavior": 0, "behavior_lines": 0, "files": set(), "markers": Counter()})
    for b in impls:
        found = is_behavior(b["body"])
        b["behavior"] = found
        s = per_type[b["type"]]
        s["files"].add(b["rel"])
        if found:
            s["behavior"] += 1
            s["behavior_lines"] += b["lines"]
            for f in found:
                s["markers"][f] += 1
        else:
            s["pure"] += 1

    # Files.
    file_class = {}
    for rel, text in crate.files.items():
        file_class[rel] = "behavior" if is_behavior(text) else "pure"

    # Item classes inside files: model types / pure fns vs behavior.
    item_model = {}
    for name, t in types.items():
        item_model[(t["rel"], name)] = name not in service
    for f in fns:
        item_model[(f["rel"], f["name"])] = not is_behavior(f["text"])

    # Edges from pure files into behavior files.
    edge_model = Counter()
    edge_behavior = Counter()
    behavior_names = Counter()
    movable_names = Counter()
    for rel, cls in file_class.items():
        if cls != "pure":
            continue
        seen = set()
        for target, name in crate.references(rel):
            if target == rel or file_class.get(target) != "behavior":
                continue
            item = name.split("::")[-1]
            key = (rel, target, item)
            if key in seen:
                continue
            seen.add(key)
            model = item_model.get((target, item))
            if model is None:
                # A module path or an unresolved name: count it as the module's class.
                model = False
            if model:
                edge_model[target] += 1
                movable_names[(target, item)] += 1
            else:
                edge_behavior[target] += 1
                behavior_names[(target, item)] += 1

    model_types = [n for n in types if n not in service]
    mixed = sorted(
        ((n, per_type[n]) for n in model_types if per_type[n]["behavior"]),
        key=lambda kv: -kv[1]["behavior_lines"],
    )
    services = sorted(
        ((n, per_type[n]) for n in service if per_type[n]["behavior"] + per_type[n]["pure"]),
        key=lambda kv: -(kv[1]["behavior"] + kv[1]["pure"]),
    )
    lines = crate.lines
    pure_files = [r for r, c in file_class.items() if c == "pure"]
    beh_files = [r for r, c in file_class.items() if c == "behavior"]
    misplaced = sorted(
        (
            (rel, name)
            for (rel, name), model in item_model.items()
            if model and file_class[rel] == "behavior"
        )
    )
    beh_impl_lines = sum(b["lines"] for b in impls if b["behavior"])
    pure_impl_lines = sum(b["lines"] for b in impls if not b["behavior"])

    summary = {
        "files": len(crate.files),
        "pure_files": len(pure_files),
        "pure_lines": sum(lines[r] for r in pure_files),
        "behavior_files": len(beh_files),
        "behavior_lines": sum(lines[r] for r in beh_files),
        "types": len(types),
        "model_types": len(model_types),
        "service_types": len(service),
        "mixed_model_types": len(mixed),
        "impl_blocks": len(impls),
        "behavior_impl_blocks": sum(1 for b in impls if b["behavior"]),
        "behavior_impl_lines": beh_impl_lines,
        "pure_impl_lines": pure_impl_lines,
        "model_items_in_behavior_files": len(misplaced),
        "pure_to_behavior_refs_movable": sum(edge_model.values()),
        "pure_to_behavior_refs_knot": sum(edge_behavior.values()),
    }
    print("summary " + " ".join(f"{k}={v}" for k, v in summary.items()))
    top = args.top
    print(f"\n# service types by impl blocks (top {top})")
    for n, s in services[:top]:
        print(f"  {n:<34} impls={s['behavior'] + s['pure']:<4} files={len(s['files']):<4} behavior={s['behavior']}")
    print(f"\n# mixed model types: behavior impl lines (top {top})")
    for n, s in mixed[:top]:
        mk = ",".join(f"{k}:{v}" for k, v in s["markers"].most_common(4))
        print(f"  {n:<34} behavior_impls={s['behavior']:<3} lines={s['behavior_lines']:<5} pure_impls={s['pure']:<3} [{mk}] {types[n]['rel']}")
    print(f"\n# behavior files that pure files import model items from (top {top})")
    for t, c in edge_model.most_common(top):
        print(f"  {c:<4} {t}")
    print(f"\n# behavior items that pure files reference: the real knots (top {top})")
    for (t, item), c in behavior_names.most_common(top):
        print(f"  {c:<4} {item:<36} {t}")
    if args.json:
        with open(args.json, "w") as f:
            json.dump(
                {
                    "summary": summary,
                    "file_class": file_class,
                    "service_types": sorted(service),
                    "mixed": [
                        {"type": n, "rel": types[n]["rel"], "behavior_impls": s["behavior"], "behavior_lines": s["behavior_lines"],
                         "pure_impls": s["pure"], "markers": dict(s["markers"]), "files": sorted(s["files"])}
                        for n, s in mixed
                    ],
                    "services": [
                        {"type": n, "impls": s["behavior"] + s["pure"], "files": sorted(s["files"])} for n, s in services
                    ],
                    "misplaced_model_items": [list(x) for x in misplaced],
                    "knot_refs": [[t, i, c] for (t, i), c in behavior_names.most_common()],
                    "movable_refs": [[t, i, c] for (t, i), c in movable_names.most_common()],
                },
                f,
                indent=1,
            )


if __name__ == "__main__":
    main()
