#!/usr/bin/env python3
"""Format writer capability ownership and the recognized direct-writer check.

Logical providers cannot hold raw store types, native row constructors or namespace
factories. Trusted format/physical modules and the enumerated bootstrap entry
functions retain those rights; private child constructors bind one home context.

This lint finds every Rust function with a direct write verb (put, delete,
batch_mutate, ...) and a raw handle or range scan token. Any direct writer fails.

This is a source check. It does not prove the live result. It finds a direct
write in the source; it cannot see a key that a caller builds elsewhere and
hands to a generic helper.

Exit 0 on a clean tree, 1 otherwise. Usage: lint-store-writers.py [--root DIR]
"""
import argparse
import os
import re
import sys

SOURCE_DIRS = ["fold_db/crates/model/src", "fold_db/crates/core/src", "lastdb_node/src"]

WRITE = re.compile(
    r"\.(?:put_item|batch_put_items|batch_delete_keys|delete_item|batch_mutate|batch_put|"
    r"batch_delete|put|delete)\s*\(|KvMutation::(?:put|delete)\s*\("
)
RAW = re.compile(
    r"\.raw\(\)|\b(?:main_store|metadata_store|feed_store|schemas_store|schema_states_store|field_store|file_store)\b|\bopen_namespace\s*\(|"
    r"\.scan_(?:prefix|prefix_keys|prefix_paged|range|range_paged|range_yielding|range_physical_paged)\s*\("
)
FN = re.compile(r"\bfn\s+([A-Za-z_][A-Za-z0-9_]*)")

# Physical engines/format writers are trusted. All other logical sources,
# including the storage-adjacent field-key provider, have no raw authority.
CORE_SOURCE = "fold_db/crates/core/src/"
TRUSTED_PREFIXES = tuple(CORE_SOURCE + part for part in (
    "storage/format/", "storage/disk_pack/", "storage/disk_backup/",
    "storage/disk_cleanup/",
))
TRUSTED_FILES = {CORE_SOURCE + "storage/" + part for part in (
    "mod.rs", "format.rs",
)}
BOOTSTRAP_FILES = {
    CORE_SOURCE + "db_operations/mod.rs": {
        "from_namespaced_store_with_content_and_field_keys",
    },
    CORE_SOURCE + "fold_db_core/mod.rs": {
        "create_fold_db_with_options",
    },
}
RAW_CAPABILITY = re.compile(
    r"\b(?:PackKvStore|KvMutation|PackNamespacedStore|RunHome|HomeMutation)\b|"
    r"\.(?:namespaced_store|open_namespace)\s*\(|\.raw\(\)|"
    r"\bfrom_namespaced_store_with_content_and_field_keys\s*\("
)


def raw_capabilities(path, source, spans):
    """A provided reader cannot write; this policy also closes raw imports/factories.

    This is an architectural source rule, not a hostile-code/filesystem sandbox.
    Bootstrap inputs may be raw; their results are typed operations/capabilities.
    """
    if path in TRUSTED_FILES or path.startswith(TRUSTED_PREFIXES):
        return []
    allowed_fns = BOOTSTRAP_FILES.get(path)
    imports = list(re.finditer(r"\buse\b[^;]*;", source)) if allowed_fns else []
    failures = []
    for token in RAW_CAPABILITY.finditer(source):
        owners = [name for name, start, end in spans if start <= token.start() <= end]
        if not owners and any(item.start() <= token.start() < item.end() for item in imports):
            continue
        if allowed_fns and owners and all(name in allowed_fns for name in owners):
            continue
        line = source.count("\n", 0, token.start()) + 1
        failures.append(f"{path}:{line}: raw storage capability outside trusted bootstrap/format: {token.group(0)}")
    return failures



def blank(text):
    """Blank comments and literals while preserving line structure and offsets."""
    n = len(text)
    nostr = list(text)
    i = 0

    def wipe(arr, a, b):
        for k in range(a, b):
            if arr[k] != "\n":
                arr[k] = " "

    while i < n:
        c = text[i]
        two = text[i : i + 2]
        if two == "//":
            j = text.find("\n", i)
            j = n if j < 0 else j
            wipe(nostr, i, j)
            i = j
        elif two == "/*":
            depth, j = 1, i + 2
            while j < n and depth:
                if text[j : j + 2] == "/*":
                    depth += 1
                    j += 2
                elif text[j : j + 2] == "*/":
                    depth -= 1
                    j += 2
                else:
                    j += 1
            wipe(nostr, i, j)
            i = j
        elif c == "r" and re.match(r'r#*"', text[i : i + 12]) and (i == 0 or not (text[i - 1].isalnum() or text[i - 1] == "_")):
            m = re.match(r'r(#*)"', text[i:])
            close = '"' + m.group(1)
            j = text.find(close, i + len(m.group(0)))
            j = n if j < 0 else j + len(close)
            wipe(nostr, i, j)
            i = j
        elif c == '"':
            j = i + 1
            while j < n and text[j] != '"':
                j += 2 if text[j] == "\\" else 1
            j = min(j + 1, n)
            wipe(nostr, i, j)
            i = j
        elif c == "'":
            m = re.match(r"'(?:\\.[^']{0,8}|[^\\'])'", text[i : i + 12])
            if m:
                wipe(nostr, i, i + len(m.group(0)))
                i += len(m.group(0))
            else:
                i += 1  # a lifetime
        else:
            i += 1
    return "".join(nostr)


def functions(nostr):
    """Return [(name, start, end)] for every fn that has a body, innermost last."""
    out = []
    for m in FN.finditer(nostr):
        i = m.end()
        paren = 0
        while i < len(nostr):
            ch = nostr[i]
            if ch == "(":
                paren += 1
            elif ch == ")":
                paren -= 1
            elif ch == ";" and paren <= 0:
                break
            elif ch == "{" and paren <= 0:
                depth, j = 0, i
                while j < len(nostr):
                    if nostr[j] == "{":
                        depth += 1
                    elif nostr[j] == "}":
                        depth -= 1
                        if depth == 0:
                            break
                    j += 1
                out.append((m.group(1), m.start(), j))
                break
            i += 1
    return out


def innermost_bodies(spans):
    """For each span, the text offsets that no nested span owns."""
    result = []
    for idx, (name, a, b) in enumerate(spans):
        holes = [(c, d) for k, (_, c, d) in enumerate(spans) if k != idx and a < c and d < b]
        result.append((name, a, b, holes))
    return result


def own_text(src, a, b, holes):
    pieces, cur = [], a
    for c, d in sorted(holes):
        if c >= cur:
            pieces.append(src[cur:c])
            cur = d
    pieces.append(src[cur:b])
    return "".join(pieces)


def scan(root, capability_failures=None):
    """Return the (path, function) pairs for recognized direct writers."""
    found = set()
    for d in SOURCE_DIRS:
        base = os.path.join(root, d)
        # walk-ok: fixed source directories of this repository, not a workspace root
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [x for x in dirnames if x not in ("target", ".git", "benches")]
            for fname in filenames:
                if not fname.endswith(".rs"):
                    continue
                path = os.path.join(dirpath, fname)
                rel = os.path.relpath(path, root)
                with open(path, encoding="utf-8", errors="replace") as handle:
                    text = handle.read()
                nostr = blank(text)
                spans = functions(nostr)
                if capability_failures is not None:
                    capability_failures.extend(raw_capabilities(rel, nostr, spans))
                for name, a, b, holes in innermost_bodies(spans):
                    body_nostr = own_text(nostr, a, b, holes)
                    if WRITE.search(body_nostr) and RAW.search(body_nostr):
                        found.add((rel, name))
    return found


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
    args = parser.parse_args()
    root = os.path.abspath(args.root)
    capability_failures = []
    found = scan(root, capability_failures)
    for failure in capability_failures:
        print(f"lint-store-writers: {failure}", file=sys.stderr)
    for path, fn in sorted(found):
        print(f"lint-store-writers: {path}::{fn} writes through a raw handle", file=sys.stderr)
    if capability_failures or found:
        return 1
    print("lint-store-writers: OK (0 writer functions)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
