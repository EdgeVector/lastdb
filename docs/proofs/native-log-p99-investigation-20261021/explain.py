#!/usr/bin/env python3
"""Summarize real write receipts and a bounded native process sample."""

import argparse
from collections import Counter
import json
from pathlib import Path
import re
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "native-log-improved-20261021"))
from analyze import distribution


def write_summary(path):
    rows = json.loads((path / "requests.json").read_text())
    events = json.loads((path / "events.json").read_text())
    slow = [row for row in rows if row["client_ms"] > 40]
    blocked = [row for row in rows if row["directory_overlap_ms"] > 1]
    unblocked = [row for row in rows if row["directory_overlap_ms"] <= 1]
    pubs = [event for event in events if event["stage"] == "directory_publish"
            and rows[0]["start_unix_us"] <= event["start_unix_us"] <= rows[-1]["end_unix_us"]]
    ordered = sorted(rows, key=lambda row: row["client_ms"])
    p99 = ordered[(len(rows) * 99 + 99) // 100 - 1]
    return {"requests": len(rows), "over_40_ms": len(slow),
            "over_40_ms_without_publication_wait": sum(row not in blocked for row in slow),
            "publication_blocked_requests": len(blocked),
            "publication_blocked_percent": 100 * len(blocked) / len(rows),
            "blocked_client_ms": distribution(row["client_ms"] for row in blocked),
            "other_client_ms": distribution(row["client_ms"] for row in unblocked),
            "p99_request": p99,
            "publications": len(pubs),
            "publication_ms": distribution(event["elapsed_us"] / 1000 for event in pubs),
            "publication_records": distribution(event["records"] for event in pubs),
            "publication_first_half_ms": distribution(event["elapsed_us"] / 1000 for event in pubs[:len(pubs)//2]),
            "publication_second_half_ms": distribution(event["elapsed_us"] / 1000 for event in pubs[len(pubs)//2:])}


def read_frames(path):
    if path.stat().st_size > 8 * 1024 * 1024:
        raise RuntimeError("the explicit process sample exceeds 8 MiB")
    roots, stack = [], []
    for line in path.read_text().splitlines():
        if line.startswith("Total number in stack"):
            break
        match = re.match(r"^([ +:!|]*)(\d+) (.*?)  \(in ([^)]+)\)", line)
        if not match:
            continue
        prefix, count, symbol, _library = match.groups()
        frame = {"depth": len(prefix), "samples": int(count),
                 "symbol": re.sub(r"::h[0-9a-f]+$", "", symbol), "children": []}
        while stack and stack[-1]["depth"] >= frame["depth"]:
            stack.pop()
        if stack:
            stack[-1]["children"].append(frame)
        else:
            roots.append(frame)
        stack.append(frame)
    if not roots:
        raise RuntimeError("the process sample contains no recognized frames")
    return roots


def publication_roots(frames, ancestors=()):
    result = []
    for frame in frames:
        symbol = frame["symbol"]
        if "HomeJournal$GT$::publish_durable" in symbol and any("::commit_queue" in item for item in ancestors):
            result.append(frame)
        else:
            result.extend(publication_roots(frame["children"], ancestors + (symbol,)))
    return result


def classify(symbols):
    if any("Publisher::flush_changes" in symbol for symbol in symbols):
        return "lookup_file_update"
    if any("::generation::" in symbol for symbol in symbols):
        return "generation_receipt"
    if any("JournalRecovery::" in symbol for symbol in symbols):
        return "journal_read_verification"
    if any("::publish_current" in symbol for symbol in symbols):
        return "current_file_publication"
    if any("::resume::checkpoint" in symbol for symbol in symbols) and not any("::publish_locked" in symbol for symbol in symbols):
        return "stream_rotation_other"
    if any("::fcntl_fullfsync" in symbol for symbol in symbols):
        return "final_device_flush"
    return "other_publication"


def summarize_frame(frame, totals, leaves, ancestors=()):
    symbols = ancestors + (frame["symbol"],)
    own = frame["samples"] - sum(child["samples"] for child in frame["children"])
    if own < 0:
        raise RuntimeError("recognized child sample counts exceed their parent")
    if own:
        totals[classify(symbols)] += own
        leaves[frame["symbol"]] += own
    for child in frame["children"]:
        summarize_frame(child, totals, leaves, symbols)


def profile_summary(path):
    roots = publication_roots(read_frames(path))
    if not roots:
        raise RuntimeError("no queue-owned durable publication appears in the sample")
    totals, leaves = Counter(), Counter()
    for root in roots:
        summarize_frame(root, totals, leaves)
    samples = sum(root["samples"] for root in roots)
    if sum(totals.values()) != samples:
        raise RuntimeError("publication sample categories do not reconcile")
    return {"method": "5-second process sample at 1 ms; categories are sample counts, not exact wall times",
            "publication_stack_samples": samples,
            "categories": [{"name": name, "samples": count, "percent": count * 100 / samples}
                           for name, count in totals.most_common()],
            "top_leaf_symbols": [{"symbol": symbol, "samples": count}
                                 for symbol, count in leaves.most_common(20)]}


def backend_summary(worktree):
    package = Path("/Users/tomtang/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/sha2-0.10.9")
    source = package / "src/sha256.rs"
    condition = '#[cfg(all(feature = "asm", target_arch = "aarch64"))]'
    if condition not in source.read_text():
        raise RuntimeError("the recorded SHA-256 backend condition differs from the local dependency")
    receipts = []
    for suffix in ("8432f0940407b78e", "b7df5cc99c853fb2", "d8a015849ca2a50c", "63cd637a81b77f3c"):
        relative = Path(f"target/dev-release/.fingerprint/sha2-{suffix}/lib-sha2.json")
        receipt = json.loads((worktree / relative).read_text())
        receipts.append({"path": str(relative), "features": json.loads(receipt["features"])})
    return {"sha2_version": "0.10.9", "backend_source": str(source),
            "aarch64_backend_condition": condition, "compiled_feature_receipts": receipts,
            "observed_profile_leaf": "sha2::sha256::soft::compress",
            "host": {"architecture": "arm64", "cpu": "Apple M4 Max",
                     "sysctl_hw_optional_arm_FEAT_SHA256": 1},
            "conclusion": "The measured native SHA-256 path uses software; the dependency gates its ARM hardware path on the absent asm feature.",
            "limit": "No accelerated build is measured in this investigation."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    report = {"previous_unprofiled": write_summary(root.parent / "native-log-improved-20261021/attribution"),
              "fresh_profiled": write_summary(root / "attribution"),
              "process_sample": profile_summary(args.profile),
              "sha256_backend": backend_summary(root.parents[2])}
    (root / "findings.json").write_text(json.dumps(report, indent=2) + "\n")
    frames = publication_roots(read_frames(args.profile))
    (root / "publication-stacks.json").write_text(json.dumps(frames, separators=(",", ":")) + "\n")
    print(json.dumps(report["process_sample"], indent=2))


if __name__ == "__main__":
    main()
