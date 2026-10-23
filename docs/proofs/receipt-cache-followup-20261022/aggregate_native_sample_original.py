#!/usr/bin/env python3
"""Summarize an actual native sample call graph without inclusive double counts."""

import argparse
import collections
import hashlib
import json
from pathlib import Path
import re


def parse_sample(path):
    if path.stat().st_size > 64 * 1024 * 1024:
        raise ValueError("Native sample exceeds the 64 MiB analysis bound")
    roots, stack, active, nodes = [], [], False, 0
    for line_number, line in enumerate(path.read_text().splitlines(), 1):
        if line == "Call graph:":
            active = True
            continue
        if active and line.startswith("Total number in stack"):
            break
        if not active or not line.strip():
            continue
        match = re.match(r"^([ +!:|]*)([0-9]+) (.+)$", line)
        if not match:
            raise ValueError(f"Unexpected call-graph line {line_number}: {line[:120]}")
        depth = len(match[1])
        node = {"weight": int(match[2]), "frame": match[3],
                "line": line_number, "children": []}
        while stack and stack[-1][0] >= depth:
            stack.pop()
        if stack:
            stack[-1][1]["children"].append(node)
        else:
            if not node["frame"].startswith("Thread_"):
                raise ValueError(f"Non-thread root at line {line_number}")
            roots.append(node)
        stack.append((depth, node))
        nodes += 1
        if nodes > 500_000 or len(stack) > 512:
            raise ValueError("Call graph exceeds its finite node or depth bound")
    if not roots:
        raise ValueError("No native call graph")
    return roots, nodes


def frames(node):
    yield node
    for child in node["children"]:
        yield from frames(child)


def first_matches(node, predicate):
    if predicate(node["frame"]):
        return [node]
    return [found for child in node["children"] for found in first_matches(child, predicate)]


def is_count(frame):
    return "laststore::disk_pack::live_parts::" in frame


def is_commit(frame):
    return (("PackRuntime" in frame and "commit_prepared_group" in frame)
            or ("HomeSnapshot" in frame and "commit_mutation" in frame)
            or ("SnapshotWriter" in frame and "commit_reserved" in frame)
            or "laststore::disk_pack::publish::publish_prepared" in frame)


def symbol(frame):
    return re.sub(r"::h[0-9a-f]+$", "", frame.split("  (in ")[0])


def terminal_category(path):
    names = [symbol(frame) for frame in path]
    if any(re.search(r"^(?:fsync|__fsync|fdatasync|__fcntl|fcntl)$", name)
           for name in names):
        return "fsync_or_fcntl_syscall"
    if any(re.search(r"^(?:__psynch_\w+|__ulock_wait|__semwait_signal|semaphore_\w+|"
                     r"kevent|poll|nanosleep)$", name)
           for name in names):
        return "wait_or_event_syscall"
    if any(re.search(r"^(?:read|__read_nocancel|pread|__pread|write|pwrite|"
                     r"open|__open|openat|close|__close|stat|fstat|lstat|fstatat|"
                     r"getattrlist|lseek|__lseek|stat64|fstat64|mkdir|__rename)$", name)
           for name in names):
        return "file_syscall"
    if any("crc32fast::" in name or "::verify_checksum" in name
           or "blake3::" in name or "sha2::sha256::" in name for name in names):
        return "checksum_or_digest_function"
    if any("malloc" in name or "alloc::" in name or "HashMap" in name
           or "hashbrown::" in name or "BTreeMap" in name for name in names):
        return "allocation_or_map_function"
    return "other_or_unresolved"


def terminals(node, ancestors=()):
    path = (*ancestors, node["frame"])
    residual = node["weight"] - sum(child["weight"] for child in node["children"])
    if residual < 0:
        raise ValueError(f"Child weights exceed parent at line {node['line']}")
    if residual:
        yield residual, path, node["line"]
    for child in node["children"]:
        yield from terminals(child, path)


def summarize_scope(nodes):
    total = sum(node["weight"] for node in nodes)
    categories, leaf_symbols, examples = collections.Counter(), collections.Counter(), {}
    for node in nodes:
        for weight, path, line in terminals(node):
            category = terminal_category(path)
            categories[category] += weight
            leaf_symbols[symbol(path[-1])] += weight
            if category not in examples or examples[category]["weight"] < weight:
                examples[category] = {"weight": weight, "line": line, "frames": list(path)}
    if sum(categories.values()) != total or sum(leaf_symbols.values()) != total:
        raise ValueError("Terminal attribution does not conserve scope weights")
    return {"weight": total, "first_context_branches": len(nodes),
            "terminal_categories": dict(categories),
            "top_terminal_symbols": leaf_symbols.most_common(25),
            "category_examples": examples,
            "first_context_frames": [{"line": node["line"], "weight": node["weight"],
                                      "frame": node["frame"]} for node in nodes]}


def audit_capture(raw, digest):
    requests = raw["request_intervals"]
    start, end = raw["cpu_counter_interval_monotonic_ns"]
    capture = raw["capture_overlap"]
    sample_start = capture["reported_capture_start_monotonic_ns"]
    sample_end = capture["reported_capture_end_monotonic_ns"]
    assert raw["sample_sha256"] == digest
    assert raw["complete"] and raw["sample_exit_code"] == 0
    assert raw["exact_range_reopen_verified"]
    assert len(requests) == raw["fixed_batch_requests"] == raw["write_inputs"]["requests"]
    assert len(requests) * raw["batch_width"] == raw["offered_logical_mutations"]
    assert start <= sample_start < sample_end <= end
    previous = start
    for request in requests:
        begin, finish = request["started_monotonic_ns"], request["ended_monotonic_ns"]
        assert previous <= begin <= finish <= end
        assert abs((finish - begin) / 1e9 - request["elapsed_seconds"]) < 1e-9
        previous = finish
    overlap = sum(request["started_monotonic_ns"] < sample_end
                  and request["ended_monotonic_ns"] > sample_start for request in requests)
    assert overlap == capture["requests_overlapping_capture"]
    assert abs((sample_end - sample_start) / 1e9 - capture["reported_capture_seconds"]) < 1e-9
    base = raw["mach_timebase"]
    for name, ticks in raw["raw_cpu_counter_deltas"].items():
        converted = ticks * base["numer"] / base["denom"] / 1e9
        assert abs(converted - raw["converted_cpu_seconds"][name]) < 1e-9
    return {"sample_hash": True, "fixed_request_intervals": len(requests),
            "capture_overlapping_requests": overlap, "cpu_conversion": True,
            "exact_range_reopen": True, "all_interval_and_tree_weight_checks": True}


def summarize(sample, summary):
    roots, node_count = parse_sample(sample)
    count_nodes = [node for root in roots for node in first_matches(root, is_count)]
    commit_nodes = [node for root in roots for node in first_matches(root, is_commit)]
    active_roots = [root for root in roots if any(is_commit(node["frame"]) for node in frames(root))]
    scopes = {"whole_process": summarize_scope(roots),
              "threads_with_commit_frames": summarize_scope(active_roots),
              "commit_context": summarize_scope(commit_nodes),
              "live_part_count_context": summarize_scope(count_nodes)}
    paths = {"payload_resolver": "live_parts::payload::resolve",
             "new_metadata_page_pass": "live_parts::scan::observe_new_parts",
             "payload_map_metadata": "::object_metadata::",
             "payload_part_header": "::verify_part::"}
    count_subpaths = {name: summarize_scope([found for node in count_nodes
                     for found in first_matches(node, lambda frame: token in frame)])
                     for name, token in paths.items()}
    threads = [{"thread": root["frame"], "weight": root["weight"],
                "commit_context_weight": sum(node["weight"] for node in first_matches(root, is_commit)),
                "count_context_weight": sum(node["weight"] for node in first_matches(root, is_count))}
               for root in roots]
    raw = json.loads(summary.read_text())
    digest = hashlib.sha256(sample.read_bytes()).hexdigest()
    return {"sample": str(sample.resolve()),
            "sample_sha256": digest, "receipt_audit": audit_capture(raw, digest),
            "node_count": node_count, "thread_count": len(roots),
            "method": "First matching context per branch; descendant matches are excluded. "
                      "Terminal residuals conserve each scope weight. Weights are wall-stack "
                      "residency units, not CPU percentages or absolute call counts.",
            "limitations": ["Release symbols omit inlined or unresolved frames.",
                            "All-thread totals include idle threads; commit context is narrower.",
                            "fcntl call flags are unavailable, so that label does not prove an fsync.",
                            "The profiler interval differs from the whole fixed workload CPU interval."],
            "capture_overlap": raw["capture_overlap"],
            "cpu_counter_interval_monotonic_ns": raw["cpu_counter_interval_monotonic_ns"],
            "converted_cpu_seconds": raw["converted_cpu_seconds"],
            "write_inputs": raw["write_inputs"], "scopes": scopes, "threads": threads,
            "count_subpaths_nonadditive": count_subpaths}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("Analysis output must be a new file")
    result = summarize(args.profile / "sample.txt", args.profile / "summary.json")
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"threads": result["thread_count"], "nodes": result["node_count"],
                      "weights": {name: scope["weight"] for name, scope in result["scopes"].items()}}))


if __name__ == "__main__":
    main()
