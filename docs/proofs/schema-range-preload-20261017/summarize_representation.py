"""Summarize the saved analysis-only key representation measurements."""
import json
from pathlib import Path
import re
import statistics
import sys


def time_value(text, label):
    match = re.search(r"^\s*(\d+)\s+" + re.escape(label) + r"\s*$", text, re.M)
    if match is None:
        raise ValueError(f"Missing time receipt field: {label}")
    return int(match.group(1))


def main(directory):
    samples = []
    groups = {}
    for path in sorted(directory.glob("[0-9]*-*.json")):
        sample = json.loads(path.read_text())
        if sample.get("operation") != "load":
            continue
        timing = path.with_suffix(".time").read_text()
        sample["receipt"] = path.name
        sample["peak_process_bytes"] = time_value(timing, "maximum resident set size")
        sample["block_inputs"] = time_value(timing, "block input operations")
        sample["page_faults"] = time_value(timing, "page faults")
        identity = (sample["keys"], sample["characters"], sample["representation"], sample["layout"])
        groups.setdefault(identity, []).append(sample)
        samples.append(sample)
    cases = []
    for identity, rows in sorted(groups.items()):
        count, characters, representation, layout = identity
        case = {
            "keys": count, "characters": characters,
            "representation": representation, "layout": layout,
            "samples": len(rows), "key_bytes": rows[0]["key_bytes"],
            "file_bytes": rows[0]["file_bytes"],
        }
        for field in ("open_ms", "read_ms", "build_ms", "total_ms", "peak_process_bytes"):
            values = [row[field] for row in rows]
            case[field] = {
                "median": statistics.median(values),
                "minimum": min(values), "maximum": max(values),
            }
        cases.append(case)
    result = {
        "kind": "synthetic_representation_measurement",
        "lastdb_product_proof": False,
        "cache_state": "Generated files; no OS cache flush; not device-cold",
        "samples": samples, "cases": cases,
    }
    (directory / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    print("keys R form layout file_MiB load_median_ms load_min_ms load_max_ms read_median_ms build_median_ms peak_median_MiB peak_max_MiB")
    for case in cases:
        print(case["keys"], case["characters"], case["representation"], case["layout"],
              f'{case["file_bytes"] / 1048576:.3f}',
              *(f'{case["total_ms"][stat]:.3f}' for stat in ("median", "minimum", "maximum")),
              f'{case["read_ms"]["median"]:.3f}', f'{case["build_ms"]["median"]:.3f}',
              f'{case["peak_process_bytes"]["median"] / 1048576:.3f}',
              f'{case["peak_process_bytes"]["maximum"] / 1048576:.3f}')


if __name__ == "__main__":
    main(Path(sys.argv[1]))
