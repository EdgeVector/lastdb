#!/usr/bin/env python3
"""Writer boundary of the atom counter.

Only the planner (db_operations/atom_store/counter/ and, for blob counters and
the blob dwell queue, db_operations/atom_store/blob_counter/) may write tip
records (`mk:`), conflict and history records, atom bodies, atom counters, blob
counters, blob queue entries and the molecule counters in the molecule header
(`mh:`) inside a write scope. The `bindings` counter has one writer,
`molecule_bindings.rs`. This lint finds every Rust function that writes through a store
and either (a) names one of those key families or (b) holds a raw store handle,
and it compares the set with scripts/lints/atom-counter-writers.allow.

The check is per function, not per file. A function is a writer when its body
has a write verb (put, delete, batch_mutate, restore_batch_put, ...) and one of:

- a key family token (family T tip, C conflict or history, B body, N atom
  counter, M molecule header and binding counters, R blob counter or blob queue
  entry);
- a raw handle token: `.raw()`, the `MainRows` port `.rows()`, `main_store`,
  `open_namespace(`, a generic row
  restore (`restore_batch_put(`), or a scan (`scan_prefix(`, `scan_range(`, ...)
  next to a delete, which is a range delete of rows of every family.

Each writer needs an allow-list line `path|function|families|reason`. The
families on the line must cover the families the function names. `*` as the
function covers every function of the file and still checks the families, so a
file that is listed with `-` (no family) fails when someone adds a tip put to it.
A line that no writer matches fails, so the list stays honest.

This is a source check. It does not prove the live result. It finds a direct
write in the source; it cannot see a key that a caller builds elsewhere and
hands to a generic helper.

Exit 0 on a clean tree, 1 otherwise. Usage: lint-atom-counter-writers.py [--root DIR]
"""
import argparse
import os
import re
import sys

SOURCE_DIRS = ["fold_db/crates/core/src", "lastdb_node/src", "lastdb_host", "lastdb_uds"]
ALLOW = "scripts/lints/atom-counter-writers.allow"

WRITE = re.compile(
    r"\.(?:put_item|batch_put_items|batch_delete_keys|delete_item|batch_mutate|batch_put|"
    r"batch_delete|restore_batch_put|put|delete)\s*\(|KvMutation::(?:put|delete)\s*\(|"
    r"\brestore_batch_put\s*\("
)
RAW = re.compile(
    r"\.raw\(\)|\.rows\(\)|\bmain_store\b|\bopen_namespace\s*\(|\brestore_batch_put\s*\(|"
    r"\.scan_(?:prefix|prefix_keys|prefix_paged|range|range_paged|range_yielding|range_physical_paged)\s*\("
)
FAMILIES = {
    "T": re.compile(
        r'"mk:|\bMK_PREFIX\b|\bhash_range_record_key\b|\bapi_hash_range_record_key|'
        r"\bPerKeyRecord\b|\bmolecule_record_key|\bmolecule_generation_record_key\b"
    ),
    "C": re.compile(
        r'\bhistory_event_key\b|"conflict|"history|\bmcc:|\bhcu:|anchored\("conflict"|'
        r'\bconflict_record_key|\bconflict_key\('
    ),
    "B": re.compile(r'\bbody_key\(|\bbody_storage_key\(|\batom_key_codec\b|"atom:'),
    "N": re.compile(r"\bcounter_key\(|\bAtomCounter\b"),
    "M": re.compile(
        r'"mh:|\bMH_PREFIX\b|\bheader_key\(|\bMoleculeHeader\b|'
        r'\bmolecule_bindings_key\b|\bMoleculeBindings\b|"mref:v1:n:'
    ),
    "R": re.compile(r'\bblob_counter_key|\bBlobQueue\b|\bBlobCounter\b|"bref:v3'),
}
FN = re.compile(r"\bfn\s+([A-Za-z_][A-Za-z0-9_]*)")


def blank(text):
    """Return (nostr, withstr): comments blanked in both, string and char
    literals also blanked in `nostr`. Line structure and offsets are kept."""
    n = len(text)
    nostr = list(text)
    withstr = list(text)
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
            wipe(withstr, i, j)
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
            wipe(withstr, i, j)
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
    return "".join(nostr), "".join(withstr)


def functions(nostr):
    """Return [(name, start, end)] for every fn that has a body, innermost last."""
    out = []
    for m in FN.finditer(nostr):
        i = m.end()
        paren = 0
        angle_ok = True
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
        _ = angle_ok
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


def scan(root):
    """Return {(path, fn): set(families)} for every writer function."""
    found = {}
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
                nostr, withstr = blank(text)
                for name, a, b, holes in innermost_bodies(functions(nostr)):
                    body_nostr = own_text(nostr, a, b, holes)
                    body_str = own_text(withstr, a, b, holes)
                    if not WRITE.search(body_nostr):
                        continue
                    fams = {f for f, rx in FAMILIES.items() if rx.search(body_str)}
                    raw = RAW.search(body_nostr) is not None
                    if fams or raw:
                        key = (rel, name)
                        found.setdefault(key, set()).update(fams)
                        if raw:
                            found[key].add("raw")
    return found


def load_allow(root):
    entries = []
    with open(os.path.join(root, ALLOW), encoding="utf-8") as handle:
        for number, line in enumerate(handle, 1):
            line = line.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            parts = line.split("|", 3)
            if len(parts) != 4 or not all(p.strip() for p in parts):
                print(f"{ALLOW}:{number}: need path|function|families|reason", file=sys.stderr)
                sys.exit(1)
            path, fn, fams, reason = (p.strip() for p in parts)
            allowed = set() if fams == "-" else set(fams.split())
            bad = allowed - {"T", "C", "B", "N", "M", "R"}
            if bad:
                print(f"{ALLOW}:{number}: unknown family {sorted(bad)}", file=sys.stderr)
                sys.exit(1)
            entries.append({"path": path, "fn": fn, "fams": allowed, "reason": reason, "line": number})
    return entries


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
    args = parser.parse_args()
    root = os.path.abspath(args.root)
    found = scan(root)
    entries = load_allow(root)
    failed = False
    used = set()
    for (path, fn), fams in sorted(found.items()):
        key_fams = fams - {"raw"}
        match = [e for e in entries if e["path"] == path and e["fn"] in (fn, "*")]
        if not match:
            failed = True
            kinds = " ".join(sorted(key_fams)) or "raw handle"
            print(
                f"lint-atom-counter-writers: {path}::{fn} writes through a store and names "
                f"[{kinds}] but is not in the allow list.\n"
                "  Route the write through the planner (counter::commit_tip_changes), or add\n"
                f"  `{path}|{fn}|<families>|<reason>` to {ALLOW}.",
                file=sys.stderr,
            )
            continue
        covered = set()
        for e in match:
            used.add(e["line"])
            covered |= e["fams"]
        missing = key_fams - covered
        if missing:
            failed = True
            print(
                f"lint-atom-counter-writers: {path}::{fn} names famil{'ies' if len(missing) > 1 else 'y'} "
                f"{' '.join(sorted(missing))} but the allow list covers [{' '.join(sorted(covered)) or '-'}].\n"
                "  A new tip, conflict, body, counter, molecule counter or blob counter write belongs in the planner.",
                file=sys.stderr,
            )
    for e in entries:
        if e["line"] not in used:
            failed = True
            print(
                f"lint-atom-counter-writers: {ALLOW}:{e['line']} names {e['path']}::{e['fn']}, "
                "which is not a writer any more. Remove the line.",
                file=sys.stderr,
            )
    if failed:
        return 1
    print(f"lint-atom-counter-writers: OK ({len(found)} writer functions, {len(entries)} allow lines)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
