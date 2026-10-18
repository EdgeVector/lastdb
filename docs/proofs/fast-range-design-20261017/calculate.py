#!/usr/bin/env python3
"""Arithmetic for the range design. This program does not call LastDB."""
import argparse
import json
from math import ceil
from pathlib import Path

PAGE_BYTES = 16_384
HEADER_BYTES = 128
PAYLOAD_BYTES = PAGE_BYTES - HEADER_BYTES
CURRENT_LEAF_ENTRIES = 53
CURRENT_INLINE_KEY_BYTES = 128
PACKED_KEY_ENTRY_OVERHEAD = 172
PROPOSED_INLINE_LIMIT = 1024
EXTERNAL_KEY_ENTRY_BYTES = 316
KEY_BUDGET = 10_000
KEY_PREFIX_BYTES = 70


def stored_key_bytes(range_bytes):
    return KEY_PREFIX_BYTES + 8 * range_bytes


def key_walk(examined_keys, range_bytes, fill=1.0):
    length = stored_key_bytes(range_bytes)
    old_capacity = max(1, int(PAYLOAD_BYTES * fill) // 304)
    inline = length <= PROPOSED_INLINE_LIMIT
    new_width = length + PACKED_KEY_ENTRY_OVERHEAD if inline else EXTERNAL_KEY_ENTRY_BYTES
    new_capacity = max(1, int(PAYLOAD_BYTES * fill) // new_width)
    tail_pages_per_key = ceil(max(0, length - CURRENT_INLINE_KEY_BYTES) / PAYLOAD_BYTES)
    old_leaf_pages = ceil(examined_keys / old_capacity)
    old_tail_pages = examined_keys * tail_pages_per_key
    new_leaf_pages = ceil(examined_keys / new_capacity)
    new_tail_pages = 0 if inline else old_tail_pages
    new_pages = new_leaf_pages + new_tail_pages
    return {
        "examined_keys": examined_keys,
        "original_range_bytes": range_bytes,
        "stored_key_bytes": length,
        "within_proposed_inline_limit": inline,
        "fill_fraction": fill,
        "current_entries_per_leaf": old_capacity,
        "proposed_entries_per_leaf": new_capacity,
        "current_leaf_pages": old_leaf_pages,
        "current_tail_pages": old_tail_pages,
        "current_key_bytes_read": (old_leaf_pages + old_tail_pages) * PAGE_BYTES,
        "proposed_leaf_pages": new_leaf_pages,
        "proposed_tail_pages": new_tail_pages,
        "proposed_key_pages": new_pages,
        "proposed_key_bytes_read": new_pages * PAGE_BYTES,
        "key_bytes_reduction_factor": round((old_leaf_pages + old_tail_pages) / new_pages, 4),
    }


def tip_sizes():
    minimum = {"entry": {"atom_uuid": "a" * 64, "written_at": 1_700_000_000_000_000_000}}
    authored = {"entry": dict(minimum["entry"], logical_counter=1_234_567,
                              mutation_uuid="m" * 36, device_id="d" * 44,
                              origin_device="o" * 36)}
    result = []
    for label, value in [("minimum", minimum), ("author_fields", authored)]:
        length = len(json.dumps(value, separators=(",", ":")).encode())
        result.append({
            "label": label,
            "json_bytes": length,
            "uncompressed_enb_v1_bytes": length + 34,
            "uncompressed_enc_v1_bytes": 4 + 4 * ceil((length + 29) / 3),
        })
    return result


def preload(keys, fields=1, layouts=1, path_bytes=1024):
    paths = keys * fields * layouts
    return {
        "keys": keys,
        "fields": fields,
        "key_layouts": layouts,
        "paths": paths,
        "assumed_bytes_per_path": path_bytes,
        "estimated_membership_memory_bytes": paths * path_bytes,
        "within_key_budget": paths <= KEY_BUDGET,
        "remaining_paths": max(0, KEY_BUDGET - paths),
        "approximate_queries_to_equal_full_membership_examination": ceil(keys / 256),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    base = key_walk(256, 13)
    latency = []
    for read_us in [20, 100, 500]:
        for bandwidth_mib_s in [500, 2000]:
            latency.append({
                "assumed_fixed_us_per_read": read_us,
                "assumed_transfer_mib_s": bandwidth_mib_s,
                "current_key_stage_model_ms": round(
                    (base["current_leaf_pages"] + base["current_tail_pages"]) * read_us / 1000
                    + base["current_key_bytes_read"] / (bandwidth_mib_s * 1024 * 1024) * 1000, 4),
                "proposed_key_stage_model_ms": round(
                    base["proposed_key_pages"] * read_us / 1000
                    + base["proposed_key_bytes_read"] / (bandwidth_mib_s * 1024 * 1024) * 1000, 4),
            })
    result = {
        "kind": "arithmetic scenarios, not a product benchmark",
        "assumptions": {
            "page_bytes": PAGE_BYTES, "page_header_bytes": HEADER_BYTES,
            "unscoped_key_prefix_bytes": KEY_PREFIX_BYTES,
            "ope_output_bytes_per_input_byte": 8,
            "packed_key_entry_overhead_bytes": PACKED_KEY_ENTRY_OVERHEAD,
            "proposed_inline_key_limit_bytes": PROPOSED_INLINE_LIMIT,
            "external_key_entry_bytes": EXTERNAL_KEY_ENTRY_BYTES,
            "large_key_scenarios": "Keys above the inline limit use the 316-byte external entry and existing bounded overflow in this first version.",
            "current_leaf_capacity": CURRENT_LEAF_ENTRIES,
            "current_inline_key_bytes": CURRENT_INLINE_KEY_BYTES,
            "warm_logical_path_budget": KEY_BUDGET,
            "estimated_memory_bytes_per_path": 1024,
            "modeled_key_pages_exclude": [
                "directory and branch seeks", "tip payloads", "atom payloads",
                "extra pages from page boundaries and file groups",
                "filters, deletes and exact counts", "operating-system cache",
            ],
            "memory_excludes": ["atom objects", "file blobs", "large custom metadata", "call-local buffers"],
        },
        "key_lengths": [
            {"range_bytes": r, "stored_key_bytes": stored_key_bytes(r),
             "current_tail_bytes": max(0, stored_key_bytes(r) - 128)}
            for r in [4, 8, 13, 20, 24, 36, 64, 128]
        ],
        "range_key_walks": [
            key_walk(k, r, fill) for fill in [1.0, 0.75]
            for r in [13, 36, 128] for k in [16, 64, 256, 1024]
        ],
        "tip_examples": tip_sizes(),
        "preload_scenarios": [
            preload(n, f, l) for n, f, l in [
                (1000, 1, 1), (2000, 1, 1), (5000, 1, 1), (10000, 1, 1),
                (100000, 1, 1), (1000000, 1, 1),
                (2000, 4, 1), (2000, 4, 2), (1000000, 5, 1),
            ]
        ],
        "hypothetical_serial_read_cost": latency,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"output": str(args.output), "base_key_walk": base,
                      "tip_examples": result["tip_examples"],
                      "preload_scenarios": result["preload_scenarios"]}, indent=2))


if __name__ == "__main__":
    main()
