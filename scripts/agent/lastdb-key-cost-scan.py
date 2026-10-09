#!/usr/bin/env python3
"""Attribute a LastDB home's stored bytes, and cost the KEY fixes, in one pass.

`lastdb db inventory` needs the daemon, decrypts everything, takes about fifty
minutes and reports only the main tree. This reads the segment files directly,
covers every plane, separates live bytes from dead ones, and finishes in about
a minute on a 7 GiB home.

It answers two questions that the byte totals alone do not:

  FIX A  Re-encode each long hex run as base64url. A 256-bit hash written as 64
         ASCII hex characters costs 8x the raw 32 bytes and 1.5x base64url, so
         the saving is exact per run: len - ceil(nbytes / 3) * 4.
  FIX B  Intern the runs. The same hash and the same caller record key appear in
         several key classes. Interning replaces each occurrence with a short
         reference and stores each distinct run once.

The two fixes overlap -- both target the same bytes -- so the script reports the
combined ceiling, never their sum.

READ-ONLY. It opens files for reading only, never contacts the daemon and never
decrypts. Bodies contribute their length and nothing else. Record ids are never
printed: the class table shows a redacted shape template instead.

Requires `packaging=Plain` (see `lastdb status`, line `Layout:`). Under other
packaging the stored ids are not verbatim and the key attribution is void.

Method, and why the scaling is per plane:
  1. Composition from a stratified hash-group sample (every STRIDE-th group).
     Groups are hash-partitioned, so any stride samples the keyspace evenly.
  2. Magnitude from an exact `stat` walk of every file.
  3. Scale each plane's sampled composition by its own measured byte ratio.
Scaling globally instead overstates sparse planes badly -- planes where only a
few groups hold segments are common. Validate a run against the daemon with
`lastdb db compact --all --json` (dry run) before publishing any figure.

Record format (`vendor/laststore/src/segfmt.rs`):
  put: 0x01 | u16le id_len | id | u32le body_len | body
  del: 0x02 | u16le id_len | id
"""

from __future__ import annotations

import argparse
import collections
import json
import math
import os
import sys

OP_PUT = 0x01
OP_DEL = 0x02
FRAME_BYTES = 7  # op + u16 id_len + u32 body_len
HEX_BYTES = frozenset(b"0123456789abcdefABCDEF")
MiB = 1024.0 * 1024.0

CLASS_KEYS = (
    "live", "live_rec", "key_b", "body_b",
    "runs", "run_b", "run_saved",
)

# Interning uses ONE table for the whole store, so distinctness is counted per
# hash group across every key class -- not per class, which would count the same
# hash once per class and understate the saving.
GROUP_KEYS = ("distinct_runs", "distinct_run_b", "distinct_run_saved")


def b64_len(n_bytes: int) -> int:
    """Length of `n_bytes` binary in unpadded base64url."""
    return math.ceil(n_bytes * 4 / 3)


def hex_runs(raw: bytes, min_run: int):
    """Every hex run in `raw` at least `min_run` characters long."""
    out = []
    n = len(raw)
    i = 0
    while i < n:
        if raw[i] in HEX_BYTES:
            j = i
            while j < n and raw[j] in HEX_BYTES:
                j += 1
            if j - i >= min_run:
                out.append(raw[i:j])
            i = j
        else:
            i += 1
    return out


def key_class(ident: str) -> str:
    """The stable prefix of a record id, or a marker when it has none."""
    j = ident.find(":")
    if not 0 < j <= 24:
        return "(unprefixed)"
    head = ident[:j]
    if head == "atom" and ident.startswith("atom:mk:"):
        return "atom:mk"
    return head


def shape(ident: str, min_run: int) -> str:
    """A redacted template for a record id. Never reveals caller data.

    Only the leading class token survives verbatim -- it is a fixed prefix the
    engine chooses, not caller data. Hex runs collapse to `<hex:N>` and every
    other segment collapses to its length alone. A caller record key can be a
    short word, so no segment is echoed just because it looks harmless.
    """
    parts = ident.split(":")
    out = [parts[0]] if parts and key_class(ident) != "(unprefixed)" else []
    for part in parts[len(out):]:
        raw = part.encode("utf-8", "replace")
        if len(raw) >= min_run and all(c in HEX_BYTES for c in raw):
            out.append("<hex:%d>" % len(raw))
        elif part == "":
            out.append("")
        else:
            out.append("<s:%d>" % len(part))
    return ":".join(out)


def new_acc():
    return collections.defaultdict(lambda: dict.fromkeys(CLASS_KEYS, 0))


def new_group_acc():
    return dict.fromkeys(GROUP_KEYS, 0)


def group_dirs(plane_dir: str):
    """Every hash-group directory under a plane: `<plane>/<epoch>/g/<group>`."""
    try:
        epochs = sorted(os.listdir(plane_dir))
    except OSError:
        return
    for epoch in epochs:
        parent = os.path.join(plane_dir, epoch, "g")
        if not os.path.isdir(parent):
            continue
        try:
            groups = sorted(os.listdir(parent))
        except OSError:
            continue
        for group in groups:
            path = os.path.join(parent, group)
            if os.path.isdir(path):
                yield path


def file_bytes(path: str):
    """Exact `.seg` and `.idx` bytes under a directory."""
    seg = idx = 0
    for dirpath, _dirnames, filenames in os.walk(path):
        for name in filenames:
            try:
                size = os.path.getsize(os.path.join(dirpath, name))
            except OSError:
                continue
            if name.endswith(".seg"):
                seg += size
            elif name.endswith(".idx"):
                idx += size
    return seg, idx


def replay_group(gdir: str):
    """Last-put-wins replay of one group's segments -> its live set.

    Segments are append-only between compactions, so replaying them in sequence
    order reproduces exactly the store's own live set. Dead bytes then become a
    measurement rather than an estimate.
    """
    live = {}
    try:
        names = sorted(f for f in os.listdir(gdir) if f.endswith(".seg"))
    except OSError:
        return live
    for name in names:
        try:
            with open(os.path.join(gdir, name), "rb") as handle:
                data = handle.read()
        except OSError:
            continue
        off, n = 0, len(data)
        while off + 3 <= n:
            op = data[off]
            id_len = int.from_bytes(data[off + 1:off + 3], "little")
            id_end = off + 3 + id_len
            if id_end > n:
                break
            if op == OP_PUT:
                if id_end + 4 > n:
                    break
                body_len = int.from_bytes(data[id_end:id_end + 4], "little")
                rec_end = id_end + 4 + body_len
                if rec_end > n:
                    break
                live[data[off + 3:id_end]] = body_len
                off = rec_end
            elif op == OP_DEL:
                live.pop(data[off + 3:id_end], None)
                off = id_end
            else:
                break  # torn tail or foreign packaging; stop this segment
    return live


def scan_group(gdir: str, acc, gacc, min_run: int, shapes):
    seen = set()
    for raw_id, body_len in replay_group(gdir).items():
        ident = raw_id.decode("utf-8", "replace")
        cls = key_class(ident)
        entry = acc[cls]
        entry["live"] += 1
        entry["key_b"] += len(raw_id)
        entry["body_b"] += body_len
        entry["live_rec"] += FRAME_BYTES + len(raw_id) + body_len
        for run in hex_runs(raw_id, min_run):
            usable = len(run) - len(run) % 2  # a half byte cannot be re-encoded
            entry["runs"] += 1
            entry["run_b"] += usable
            entry["run_saved"] += usable - b64_len(usable // 2)
            seen.add(run)
        if shapes is not None and shapes[cls][0] < 1:
            shapes[cls] = (1, shape(ident, min_run))
    # Distinctness is measured within a group. Partitioning is PartitionPrefix,
    # so rows for one record co-locate; a run repeated across key classes for
    # the same record lands in the same group. Copies that span groups are not
    # counted, which makes FIX B a lower bound.
    for run in seen:
        usable = len(run) - len(run) % 2
        gacc["distinct_runs"] += 1
        gacc["distinct_run_b"] += usable
        gacc["distinct_run_saved"] += usable - b64_len(usable // 2)


def scan(root: str, stride: int, min_run: int):
    planes = {}
    totals = new_acc()
    grand_totals = new_group_acc()
    shapes = collections.defaultdict(lambda: (0, ""))
    grand_seg = grand_idx = 0

    for plane in sorted(os.listdir(root)):
        pdir = os.path.join(root, plane)
        if not os.path.isdir(pdir):
            continue
        seg, idx = file_bytes(pdir)
        grand_seg += seg
        grand_idx += idx
        if seg == 0:
            continue
        acc = new_acc()
        gacc = new_group_acc()
        sampled = 0
        for i, gdir in enumerate(group_dirs(pdir)):
            if i % stride:
                continue
            gseg, _ = file_bytes(gdir)
            if gseg == 0:
                continue
            sampled += gseg
            scan_group(gdir, acc, gacc, min_run, shapes)
        if sampled == 0:
            continue
        scale = seg / sampled
        group_scaled = {k: gacc[k] * scale for k in GROUP_KEYS}
        for k in GROUP_KEYS:
            grand_totals[k] += group_scaled[k]
        classes = {}
        for cls, entry in acc.items():
            scaled = {k: entry[k] * scale for k in CLASS_KEYS}
            classes[cls] = scaled
            for k in CLASS_KEYS:
                totals[cls][k] += scaled[k]
        planes[plane] = {
            "seg_bytes": seg, "idx_bytes": idx,
            "sampled_seg_bytes": sampled, "scale": scale, "classes": classes,
            "intern": group_scaled,
        }

    return {
        "root": root, "stride": stride, "min_run": min_run,
        "seg_bytes": grand_seg, "idx_bytes": grand_idx,
        "planes": planes,
        "totals": {k: dict(v) for k, v in totals.items()},
        "intern": grand_totals,
        "shapes": {k: v[1] for k, v in shapes.items()},
    }


def summarize(report):
    """Derive the store-wide roll-up and the two key-fix ceilings."""
    totals = report["totals"].values()
    live = sum(t["live"] for t in totals)
    live_rec = sum(t["live_rec"] for t in totals)
    key_b = sum(t["key_b"] for t in totals)
    body_b = sum(t["body_b"] for t in totals)
    run_b = sum(t["run_b"] for t in totals)
    run_saved = sum(t["run_saved"] for t in totals)
    runs = sum(t["runs"] for t in totals)
    intern = report["intern"]
    distinct_runs = intern["distinct_runs"]
    distinct_run_b = intern["distinct_run_b"]
    distinct_saved = intern["distinct_run_saved"]
    seg = report["seg_bytes"]

    # FIX A: every run keeps its own bytes, re-encoded. Summed exactly per run
    # during the scan, so odd-length runs do not inflate the figure.
    fix_a = run_saved

    # FIX A+B: each occurrence becomes a reference; the table holds each
    # distinct run once, itself base64url-encoded.
    ref_bytes = report.get("ref_bytes", 5)
    fix_ab = run_b - runs * ref_bytes - (distinct_run_b - distinct_saved)

    return {
        "live_rows": live,
        "live_record_bytes": live_rec,
        "live_key_bytes": key_b,
        "live_body_bytes": body_b,
        "live_frame_bytes": live * FRAME_BYTES,
        "dead_bytes": seg - live_rec,
        "run_bytes": run_b,
        "run_occurrences": runs,
        "distinct_runs": distinct_runs,
        "distinct_run_bytes": distinct_run_b,
        "fix_a_base64url_bytes": fix_a,
        "fix_ab_intern_bytes": fix_ab,
        "ref_bytes": ref_bytes,
    }


def print_text(report, summary):
    seg = report["seg_bytes"]
    idx = report["idx_bytes"]
    store = seg + idx
    s = summary
    key_b = s["live_key_bytes"] or 1

    print("LastDB key-cost scan  root=%s stride=%d" % (
        report["root"], report["stride"]))
    print()
    print("store            %10.1f MiB   (seg %.1f + idx %.1f)" % (
        store / MiB, seg / MiB, idx / MiB))
    print("live rows        %10.0f" % s["live_rows"])
    print("live records     %10.1f MiB   key %.1f + body %.1f + framing %.1f" % (
        s["live_record_bytes"] / MiB, key_b / MiB,
        s["live_body_bytes"] / MiB, s["live_frame_bytes"] / MiB))
    print("dead versions    %10.1f MiB   (%.1f%% of seg)" % (
        s["dead_bytes"] / MiB, 100.0 * s["dead_bytes"] / (seg or 1)))
    print()

    print("%-14s %12s %7s %10s %10s %6s  %s" % (
        "class", "live rows", "row%", "rec MiB", "key MiB", "key%", "shape"))
    rows = sorted(report["totals"].items(), key=lambda kv: -kv[1]["live_rec"])
    for cls, t in rows:
        if t["live_rec"] < 1 * MiB:
            continue
        print("%-14s %12.0f %6.1f%% %10.1f %10.1f %5.1f%%  %s" % (
            cls, t["live"], 100.0 * t["live"] / (s["live_rows"] or 1),
            t["live_rec"] / MiB, t["key_b"] / MiB,
            100.0 * t["key_b"] / (t["live_rec"] or 1),
            report["shapes"].get(cls, "")))
    print()

    print("KEY attribution")
    print("  live key bytes        %10.1f MiB  = %.1f%% of seg" % (
        key_b / MiB, 100.0 * key_b / (seg or 1)))
    print("  inside hex runs       %10.1f MiB  = %.1f%% of key bytes" % (
        s["run_bytes"] / MiB, 100.0 * s["run_bytes"] / key_b))
    print("  run occurrences       %10.0f  over %.0f distinct (%.2fx copies)" % (
        s["run_occurrences"], s["distinct_runs"],
        s["run_occurrences"] / (s["distinct_runs"] or 1)))
    print()
    print("CEILINGS (segment bytes; the .idx sidecar does not store keys)")
    print("  FIX A  hex -> base64url        %10.1f MiB  = %.1f%% of the store" % (
        s["fix_a_base64url_bytes"] / MiB,
        100.0 * s["fix_a_base64url_bytes"] / (store or 1)))
    print("  FIX A+B  + intern (%d B ref)   %10.1f MiB  = %.1f%% of the store" % (
        s["ref_bytes"], s["fix_ab_intern_bytes"] / MiB,
        100.0 * s["fix_ab_intern_bytes"] / (store or 1)))
    extra = s["fix_ab_intern_bytes"] - s["fix_a_base64url_bytes"]
    print("  interning adds        %10.1f MiB over FIX A alone (%.0f%% of A+B)" % (
        extra / MiB, 100.0 * extra / (s["fix_ab_intern_bytes"] or 1)))


def resolve_root(home: str) -> str:
    """Accept a LastDB home, its `data` dir, or the plane root itself."""
    for candidate in (os.path.join(home, "data", "data"),
                      os.path.join(home, "data"),
                      home):
        if os.path.isdir(candidate) and any(
            os.path.isdir(os.path.join(candidate, p, e, "g"))
            for p in os.listdir(candidate)
            if os.path.isdir(os.path.join(candidate, p))
            for e in os.listdir(os.path.join(candidate, p))
            if os.path.isdir(os.path.join(candidate, p, e))
        ):
            return candidate
    raise SystemExit(
        "no LastDB plane root under %r (expected <plane>/<epoch>/g/<group>)" % home)


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Read-only key-cost attribution for a LastDB home.")
    ap.add_argument("--home", default=os.path.expanduser("~/.lastdb"),
                    help="LastDB home, its data dir, or the plane root")
    ap.add_argument("--stride", type=int, default=8,
                    help="sample every Nth hash group (1 = every group)")
    ap.add_argument("--min-run", type=int, default=16,
                    help="shortest hex run worth re-encoding, in characters")
    ap.add_argument("--ref-bytes", type=int, default=5,
                    help="width of a hypothetical intern-table reference")
    ap.add_argument("--json", action="store_true", help="emit JSON")
    args = ap.parse_args(argv)

    if args.stride < 1:
        raise SystemExit("--stride must be at least 1")

    report = scan(resolve_root(args.home), args.stride, args.min_run)
    report["ref_bytes"] = args.ref_bytes
    summary = summarize(report)
    if args.json:
        report["summary"] = summary
        json.dump(report, sys.stdout, indent=1, sort_keys=True)
        sys.stdout.write("\n")
    else:
        print_text(report, summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
