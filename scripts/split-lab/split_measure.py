#!/usr/bin/env python3
"""Measure the module dependencies of one Rust crate against a candidate split.

A candidate is a JSON file:

    {
      "name": "purity",
      "units": [
        {"name": "model", "pure": true, "about": "values and rules",
         "paths": ["record/", "schema/types/"]},
        {"name": "engine", "paths": ["db_operations/", "lib.rs"]}
      ],
      "allow": {"model": [], "engine": ["model"]}
    }

Each source file goes to the unit with the longest matching path prefix.
`allow[A]` lists the units that A may depend on. Any other cross-unit
reference is a violation. A `pure` unit must also have no effect sites.

The resolver is static and approximate: it parses `use` trees and inline
`crate::` / `super::` / `self::` paths, follows `pub use` re-exports, and
falls back to the unique definition of a name under the module. It does not
run cargo, so it cannot see macro expansion or trait-method calls.

Usage:
    split_measure.py --src fold_db/crates/core/src --config cand.json \
        [--dependent lastdb_node=lastdb_node/src ...] [--json out.json]
"""

import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict

IDENT = r"[A-Za-z_][A-Za-z0-9_]*"
ITEM_RE = re.compile(
    r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:(?:unsafe|async|const|default)\s+|extern\s+\"[^\"]*\"\s+)*"
    r"(struct|enum|fn|trait|type|const|static|union|mod)\s+(" + IDENT + r")",
    re.M,
)
MACRO_RE = re.compile(r"^\s*macro_rules!\s*(" + IDENT + r")", re.M)
USE_RE = re.compile(r"(?:^|[;{}\s])((?:pub(?:\([^)]*\))?\s+)?)use\s+([^;]+);", re.S)
INLINE_RE = re.compile(r"\b((?:crate|super|self)(?:::(?:super|" + IDENT + r"))+)")
EXTERN_RE = re.compile(r"\b(" + IDENT + r")::")

EFFECTS = {
    "clock": re.compile(r"Utc::now\(|SystemTime::now\(|Local::now\(|clock::unix_\w+\("),
    "ids": re.compile(r"new_v4\(|thread_rng\(|rand::rng\(|OsRng"),
    "env": re.compile(r"\benv::var(?:_os)?\(|env_flag::"),
    "globals": re.compile(
        r"lazy_static!|thread_local!|"
        r"static\s+(?:mut\s+)?[A-Z_0-9]+\s*:[^=;]*\b(?:OnceLock|LazyLock|OnceCell|Lazy|Atomic\w+|Mutex|RwLock)\b"
    ),
    "spawn": re.compile(r"tokio::spawn\(|spawn_blocking\(|thread::spawn\("),
    "net": re.compile(r"\breqwest::"),
    "fs": re.compile(r"\bstd::fs::|\btokio::fs::|\bfs::(?:read|write|create_dir|remove|rename|metadata|File)|File::(?:open|create)\(|OpenOptions"),
    "timing": re.compile(r"Instant::now\("),
}
PURITY_EFFECTS = ("clock", "ids", "env", "globals", "spawn", "net", "fs")


def strip_comments(text):
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    text = re.sub(r"//[^\n]*", "", text)
    # `pub(in crate::x)` is a visibility marker, not a dependency.
    return re.sub(r"pub\s*\(\s*in\s+[^)]*\)", "pub", text)


IMPL_RE = re.compile(r"^[ \t]*(?:unsafe\s+)?impl\b([^{;]*)\{", re.M)
BARE_RE = re.compile(r"(?<![:\w])([a-z_][a-z0-9_]*)((?:::[A-Za-z_][A-Za-z0-9_]*)+)")


def impl_self_type(header):
    """Return the self type name of an inherent impl header, or None for a trait impl."""
    if re.search(r"\bfor\b", header):
        return None
    h = header.strip()
    if h.startswith("<"):
        depth = 0
        for i, ch in enumerate(h):
            depth += ch == "<"
            depth -= ch == ">"
            if depth == 0:
                h = h[i + 1 :].strip()
                break
    m = re.match(r"((?:[A-Za-z_][A-Za-z0-9_]*::)*)([A-Za-z_][A-Za-z0-9_]*)", h)
    return m.group(2) if m else None


def split_top(body):
    parts, depth, cur = [], 0, []
    for ch in body:
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    if cur:
        parts.append("".join(cur))
    return [p for p in parts if p]


def expand_use(tree):
    """Return [(path_segments, alias_or_None)] for one use tree."""
    tree = re.sub(r"\s+as\s+(" + IDENT + r")", r" as \1", tree)
    out = []

    def rec(prefix, t):
        t = t.strip()
        alias = None
        m = re.match(r"^(.*?)\s+as\s+(" + IDENT + r")$", t, re.S)
        if m and "{" not in m.group(1):
            t, alias = m.group(1), m.group(2)
        t = re.sub(r"\s+", "", t)
        i = t.find("{")
        if i == -1:
            segs = [s for s in (prefix + t).split("::") if s]
            if segs and segs[-1] == "self":
                segs = segs[:-1]
            out.append((segs, alias))
            return
        depth, j = 0, i
        for j in range(i, len(t)):
            if t[j] == "{":
                depth += 1
            elif t[j] == "}":
                depth -= 1
                if depth == 0:
                    break
        for part in split_top(t[i + 1 : j]):
            rec(prefix + t[:i], part)

    rec("", tree)
    return out


def module_of(rel):
    parts = rel[:-3].split("/")
    if parts[-1] in ("mod", "lib", "main"):
        parts = parts[:-1]
    return tuple(parts)


class Crate:
    def __init__(self, src):
        self.src = src
        self.files = {}  # rel -> stripped text
        self.lines = {}
        self.modules = {}  # modpath -> rel
        self.items = defaultdict(set)
        self.reexports = defaultdict(dict)  # modpath -> name -> abs path
        self.globs = defaultdict(list)
        # walk-ok: bounded to one crate src directory passed on the command line
        for root, dirs, names in os.walk(src):
            dirs[:] = [d for d in dirs if d not in ("target", ".git")]
            for n in names:
                if n.endswith(".rs"):
                    full = os.path.join(root, n)
                    rel = os.path.relpath(full, src)
                    raw = open(full, encoding="utf-8", errors="replace").read()
                    self.lines[rel] = raw.count("\n") + 1
                    self.files[rel] = strip_comments(raw)
                    self.modules[module_of(rel)] = rel
        self.type_defs = defaultdict(list)
        for rel, text in self.files.items():
            mod = module_of(rel)
            for kind, name in ITEM_RE.findall(text):
                self.items[mod].add(name)
                if kind in ("struct", "enum", "union", "trait"):
                    self.type_defs[name].append(rel)
            for name in MACRO_RE.findall(text):
                self.items[mod].add(name)
        for rel, text in self.files.items():
            mod = module_of(rel)
            for vis, tree in USE_RE.findall(text):
                if not vis.strip():
                    continue
                for segs, alias in expand_use(tree):
                    absp = self.absolute(mod, segs)
                    if absp is None:
                        continue
                    if segs and segs[-1] == "*":
                        self.globs[mod].append(tuple(absp[:-1]))
                    elif absp:
                        self.reexports[mod][alias or absp[-1]] = tuple(absp)

    def absolute(self, mod, segs):
        if not segs:
            return None
        head = segs[0]
        if head == "crate":
            return list(segs[1:])
        if head in ("self", "super"):
            base = list(mod)
            rest = list(segs)
            if rest[0] == "self":
                rest = rest[1:]
            while rest and rest[0] == "super":
                base = base[:-1]
                rest = rest[1:]
            return base + rest
        # 2018 edition: a bare path can start at a child module of this module.
        child = tuple(mod) + (head,)
        if child in self.modules:
            return list(child) + list(segs[1:])
        return None

    def module_aliases(self, rel):
        """Names that this file imports and that name a module: name -> module path."""
        mod = module_of(rel)
        out = {}
        for _, tree in USE_RE.findall(self.files[rel]):
            for segs, alias in expand_use(tree):
                absp = self.absolute(mod, segs)
                if absp and tuple(absp) in self.modules:
                    out[alias or absp[-1]] = tuple(absp)
        return out

    def foreign_impls(self, rel):
        """Yield (type_name, defining_rel) for each inherent impl in rel of a type defined elsewhere."""
        mod = module_of(rel)
        for header in IMPL_RE.findall(self.files[rel]):
            name = impl_self_type(header)
            if not name:
                continue
            defs = self.type_defs.get(name, [])
            if rel in defs:
                continue
            if len(defs) == 1:
                yield name, defs[0]
                continue
            # Several types share the name: use this file's imports to choose.
            for _, tree in USE_RE.findall(self.files[rel]):
                for segs, alias in expand_use(tree):
                    if (alias or (segs[-1] if segs else None)) == name:
                        absp = self.absolute(mod, segs)
                        hit = self.resolve(absp) if absp else None
                        if hit and hit != rel:
                            yield name, hit

    def resolve(self, path, depth=0):
        path = tuple(p for p in path if p != "*")
        cur = ()
        for seg in path:
            if cur + (seg,) in self.modules:
                cur = cur + (seg,)
            else:
                break
        rest = path[len(cur) :]
        if cur not in self.modules:
            return None
        if not rest:
            return self.modules[cur]
        name = rest[0]
        if name in self.items[cur]:
            return self.modules[cur]
        if depth < 8 and name in self.reexports[cur]:
            hit = self.resolve(self.reexports[cur][name] + rest[1:], depth + 1)
            if hit:
                return hit
        if depth < 8:
            for g in self.globs[cur]:
                if name in self.items.get(g, ()) or name in self.reexports.get(g, {}):
                    hit = self.resolve(g + rest, depth + 1)
                    if hit:
                        return hit
        under = [m for m in self.modules if m[: len(cur)] == cur and name in self.items[m]]
        if len(under) == 1:
            return self.modules[under[0]]
        return self.modules[cur]

    def references(self, rel):
        """Yield (target_rel, name) for each crate-internal reference in rel."""
        mod = module_of(rel)
        text = self.files[rel]
        for _, tree in USE_RE.findall(text):
            for segs, _alias in expand_use(tree):
                absp = self.absolute(mod, segs)
                if absp:
                    hit = self.resolve(absp)
                    if hit:
                        yield hit, "::".join(absp[-2:])
        body = USE_RE.sub(" ", text)
        for m in INLINE_RE.finditer(body):
            segs = m.group(1).split("::")
            absp = self.absolute(mod, segs)
            if absp:
                hit = self.resolve(absp)
                if hit:
                    yield hit, "::".join(absp[-2:])
        # Bare `module::Item` paths through a child module or an imported module name.
        aliases = self.module_aliases(rel)
        for m in BARE_RE.finditer(INLINE_RE.sub(" ", body)):
            head = m.group(1)
            rest = [s for s in m.group(2).split("::") if s]
            if head in aliases:
                absp = list(aliases[head]) + rest
            elif tuple(mod) + (head,) in self.modules:
                absp = list(mod) + [head] + rest
            else:
                continue
            hit = self.resolve(tuple(absp))
            if hit:
                yield hit, "::".join(absp[-2:])


def assign(rel, units):
    best, best_len = "UNASSIGNED", -1
    for u in units:
        for p in u["paths"]:
            if (rel == p or rel.startswith(p)) and len(p) > best_len:
                best, best_len = u["name"], len(p)
    return best


def sccs(nodes, edges):
    index, low, stack, on, out, counter = {}, {}, [], set(), [], [0]

    def strong(v):
        index[v] = low[v] = counter[0]
        counter[0] += 1
        stack.append(v)
        on.add(v)
        for w in edges.get(v, ()):
            if w not in index:
                strong(w)
                low[v] = min(low[v], low[w])
            elif w in on:
                low[v] = min(low[v], index[w])
        if low[v] == index[v]:
            comp = []
            while True:
                w = stack.pop()
                on.discard(w)
                comp.append(w)
                if w == v:
                    break
            if len(comp) > 1:
                out.append(sorted(comp))

    sys.setrecursionlimit(10000)
    for v in nodes:
        if v not in index:
            strong(v)
    return out


def external_crates(cargo_toml):
    names = set()
    try:
        text = open(cargo_toml).read()
    except OSError:
        return names
    section = None
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("["):
            section = s
            continue
        if section in ("[dependencies]",) and "=" in s and not s.startswith("#"):
            names.add(s.split("=")[0].strip().replace("-", "_"))
    return names


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    ap.add_argument("--config", required=True)
    ap.add_argument("--dependent", action="append", default=[], help="NAME=SRC_DIR of a crate that uses this crate")
    ap.add_argument("--crate-name", default="fold_db")
    ap.add_argument("--json", help="write the full result here")
    ap.add_argument("--top", type=int, default=12)
    ap.add_argument("--concepts", help="JSON map of concept name -> regex on the file path")
    ap.add_argument("--home-lines", type=int, default=300, help="lines that make a unit a home of a concept")
    args = ap.parse_args()

    cfg = json.load(open(args.config))
    units = cfg["units"]
    allow = {k: set(v) for k, v in cfg.get("allow", {}).items()}
    pure = {u["name"] for u in units if u.get("pure")}
    crate = Crate(args.src)
    externs = external_crates(os.path.join(os.path.dirname(os.path.abspath(args.src)), "Cargo.toml"))

    unit_of = {rel: assign(rel, units) for rel in crate.files}
    unit_files = defaultdict(list)
    for rel, u in unit_of.items():
        unit_files[u].append(rel)

    unit_lines = Counter()
    unit_effects = defaultdict(Counter)
    effect_sites = defaultdict(list)
    unit_externs = defaultdict(Counter)
    for rel, text in crate.files.items():
        u = unit_of[rel]
        unit_lines[u] += crate.lines[rel]
        for k, rx in EFFECTS.items():
            n = len(rx.findall(text))
            if n:
                unit_effects[u][k] += n
                if u in pure and k in PURITY_EFFECTS:
                    effect_sites[u].append((rel, k, n))
        for m in EXTERN_RE.findall(text):
            if m in externs:
                unit_externs[u][m] += 1

    pair_refs = Counter()
    pair_names = defaultdict(Counter)
    pair_sources = defaultdict(Counter)
    pair_edges = defaultdict(set)
    unit_graph = defaultdict(set)
    for rel in crate.files:
        a = unit_of[rel]
        for tgt, name in crate.references(rel):
            if tgt == rel:
                continue
            b = unit_of[tgt]
            if a == b:
                continue
            pair_refs[(a, b)] += 1
            pair_names[(a, b)][name] += 1
            pair_sources[(a, b)][rel] += 1
            pair_edges[(a, b)].add((rel, tgt))
            unit_graph[a].add(b)

    # Rust permits an inherent impl only in the crate that defines the type.
    foreign_pairs = Counter()
    foreign_types = Counter()
    for rel in crate.files:
        a = unit_of[rel]
        for name, defining in crate.foreign_impls(rel):
            b = unit_of[defining]
            if a != b:
                foreign_pairs[(a, b)] += 1
                foreign_types[f"{name} ({b})"] += 1
    foreign_total = sum(foreign_pairs.values())

    concept_homes = {}
    if args.concepts:
        for concept, pattern in json.load(open(args.concepts)).items():
            rx = re.compile(pattern)
            per_unit = Counter()
            for rel in crate.files:
                if rx.search(rel):
                    per_unit[unit_of[rel]] += crate.lines[rel]
            concept_homes[concept] = {u: n for u, n in per_unit.items() if n >= args.home_lines}
    multi_home = sum(1 for homes in concept_homes.values() if len(homes) > 1)

    violations = {p: n for p, n in pair_refs.items() if p[1] not in allow.get(p[0], set())}
    cycles = sccs(sorted(unit_files), {k: sorted(v) for k, v in unit_graph.items()})

    dependents = {}
    for spec in args.dependent:
        name, path = spec.split("=", 1)
        used = Counter()
        rx_use = re.compile(r"\buse\s+(" + args.crate_name + r"::[^;]+);", re.S)
        rx_inline = re.compile(r"\b" + args.crate_name + r"((?:::" + IDENT + r")+)")
        # walk-ok: bounded to one dependent crate src directory passed on the command line
        for root, dirs, names in os.walk(path):
            dirs[:] = [d for d in dirs if d not in ("target", ".git")]
            for n in names:
                if not n.endswith(".rs"):
                    continue
                text = strip_comments(open(os.path.join(root, n), encoding="utf-8", errors="replace").read())
                paths = []
                for tree in rx_use.findall(text):
                    paths += [segs[1:] for segs, _ in expand_use(tree)]
                for m in rx_inline.finditer(rx_use.sub(" ", text)):
                    paths.append([s for s in m.group(1).split("::") if s])
                for segs in paths:
                    hit = crate.resolve(tuple(segs)) if segs else crate.modules.get(())
                    if hit:
                        used[unit_of[hit]] += 1
        dependents[name] = dict(used)

    total_refs = sum(pair_refs.values())
    viol_refs = sum(violations.values())
    viol_edges = sum(len(pair_edges[p]) for p in violations)
    viol_sources = len({s for p in violations for s in pair_sources[p]})
    pure_effects = sum(n for u in pure for (_, _, n) in effect_sites[u])

    print(f"# candidate: {cfg.get('name')}")
    print(f"files {len(crate.files)} · cross-unit refs {total_refs} · violation refs {viol_refs} "
          f"· violation file edges {viol_edges} · violating source files {viol_sources} "
          f"· foreign inherent impls {foreign_total} "
          f"· effect sites in pure units {pure_effects} · unit cycles {len(cycles)}"
          + (f" · concepts with >1 home {multi_home}/{len(concept_homes)}" if concept_homes else ""))
    print()
    print("## foreign inherent impls (impl unit -> type unit: blocks)")
    for (a, b), n in sorted(foreign_pairs.items(), key=lambda kv: -kv[1]):
        print(f"- {a} -> {b}: {n}")
    if foreign_types:
        print("    types: " + ", ".join(f"{k}×{v}" for k, v in foreign_types.most_common(args.top)))
    if not foreign_pairs:
        print("- none")
    print()
    if concept_homes:
        print(f"## concept homes (units with >= {args.home_lines} lines of the concept)")
        for concept, homes in concept_homes.items():
            print(f"- {concept}: " + ", ".join(f"{u} {n}" for u, n in sorted(homes.items(), key=lambda kv: -kv[1])))
        print()
    print("## units")
    for u in [x["name"] for x in units] + (["UNASSIGNED"] if "UNASSIGNED" in unit_files else []):
        eff = ", ".join(f"{k}={v}" for k, v in sorted(unit_effects[u].items()))
        ext = ", ".join(k for k, _ in unit_externs[u].most_common(12))
        flag = " (pure)" if u in pure else ""
        print(f"- {u}{flag}: {len(unit_files[u])} files, {unit_lines[u]} lines | effects: {eff or '-'} | externs: {ext or '-'}")
    if "UNASSIGNED" in unit_files:
        print("  UNASSIGNED files: " + ", ".join(sorted(unit_files["UNASSIGNED"])[: args.top]))
    print()
    print("## violations (from -> to: refs, file edges)")
    for (a, b), n in sorted(violations.items(), key=lambda kv: -kv[1]):
        print(f"- {a} -> {b}: {n} refs, {len(pair_edges[(a, b)])} file edges")
        print("    names:   " + ", ".join(f"{k}×{v}" for k, v in pair_names[(a, b)].most_common(args.top)))
        print("    sources: " + ", ".join(f"{k}×{v}" for k, v in pair_sources[(a, b)].most_common(6)))
    if not violations:
        print("- none")
    print()
    if pure:
        print("## effect sites in pure units")
        for u in sorted(pure):
            for rel, k, n in sorted(effect_sites[u], key=lambda t: -t[2])[: args.top * 2]:
                print(f"- {u}: {rel} {k}×{n}")
        print()
    print("## unit cycles")
    for c in cycles:
        print("- " + " <-> ".join(c))
    if not cycles:
        print("- none")
    if dependents:
        print()
        print("## dependents: units they reference")
        for name, used in dependents.items():
            print(f"- {name}: " + ", ".join(f"{k}×{v}" for k, v in sorted(used.items(), key=lambda kv: -kv[1])))

    if args.json:
        out = {
            "name": cfg.get("name"),
            "summary": {
                "files": len(crate.files), "cross_refs": total_refs, "violation_refs": viol_refs,
                "violation_file_edges": viol_edges, "violating_source_files": viol_sources,
                "pure_effect_sites": pure_effects, "unit_cycles": cycles,
                "foreign_inherent_impls": foreign_total, "concepts_multi_home": multi_home,
            },
            "foreign_impls": {f"{a}->{b}": n for (a, b), n in foreign_pairs.items()},
            "concept_homes": concept_homes,
            "units": {u: {"files": len(unit_files[u]), "lines": unit_lines[u],
                          "effects": dict(unit_effects[u]), "externs": dict(unit_externs[u])}
                      for u in unit_files},
            "pairs": {f"{a}->{b}": {"refs": n, "violation": (a, b) in violations,
                                    "edges": sorted(pair_edges[(a, b)])}
                      for (a, b), n in pair_refs.items()},
            "dependents": dependents,
            "assignment": unit_of,
        }
        with open(args.json, "w") as f:
            json.dump(out, f, indent=1)


if __name__ == "__main__":
    main()
