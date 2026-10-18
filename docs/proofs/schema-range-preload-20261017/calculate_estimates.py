#!/usr/bin/env python3
"""Arithmetic scenarios, not a database measurement or a schema census."""

import json
from pathlib import Path


def memory_case(name, keys, original_range_bytes):
    binary_bytes = 4 * original_range_bytes
    return {
        "name": name,
        "keys_assumed": keys,
        "original_range_bytes_assumed": original_range_bytes,
        "binary_range_bytes_per_key": binary_bytes,
        "packed_planning_bytes": keys * (binary_bytes + 16),
        "mutable_planning_bytes": keys * (binary_bytes + 64),
    }


def main():
    root = Path(__file__).resolve().parent
    cases = [
        memory_case("Brain RecordListEntry", 50_000, 40),
        memory_case("BoardCards", 10_000, 54),
        memory_case("MilestoneCards", 10_000, 54),
        memory_case("RoutineRunSummary: 100 routines x 100 runs", 10_000, 24),
        memory_case("LastGit Pack", 100_000, 64),
    ]
    disk = json.loads((root / "disk-component.json").read_text())
    disk_cases = []
    for case in disk["results"]:
        times = sorted(read["elapsed_nanos"] / 1_000_000 for read in case["reads"])
        first = case["reads"][0]
        disk_cases.append({
            "keys": case["keys"],
            "range_bytes": case["original_range_bytes"],
            "median_ms": times[len(times) // 2],
            "key_tail_pages": first["costs"]["overflow_pages"],
            "file_bytes": first["costs"]["file_bytes"],
            "read_operations": first["disk"]["read_operations"],
        })
    result = {
        "kind": "arithmetic scenarios; not old database counts or complete preload timing",
        "mib_bytes": 1_048_576,
        "packed_formula": "N * (4 * R + 16); excludes per-domain metadata",
        "mutable_formula": "N * (4 * R + 64); excludes per-domain metadata and reserve",
        "reserve_fraction": [0.25, 0.50],
        "app_shaped_cases": cases,
        "app_total": {
            "keys": sum(case["keys_assumed"] for case in cases),
            "packed_planning_bytes": sum(case["packed_planning_bytes"] for case in cases),
            "mutable_planning_bytes": sum(case["mutable_planning_bytes"] for case in cases),
        },
        "disk_component_medians": disk_cases,
        "current_layout_linear_extrapolation": {
            "basis": "3000-key component cases only; excludes additional schema preload work",
            "scenarios": [
                {
                    "keys": count,
                    "seconds": [
                        case["median_ms"] / 1000 * count / case["keys"]
                        for case in disk_cases if case["keys"] == 3000
                    ],
                }
                for count in [10_000, 100_000, 1_000_000]
            ],
        },
    }
    (root / "estimates.json").write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
