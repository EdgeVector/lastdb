#!/usr/bin/env python3
"""Summarize compiler layouts; this source diagnostic is not runtime memory proof."""
import argparse
import json
from pathlib import Path
import re

HEADER = re.compile(r"^print-type-size type: `(.+)`: ([0-9]+) bytes, alignment: ([0-9]+) bytes$")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    args = parser.parse_args()
    layouts, current = {}, None
    with args.log.open() as stream:
        for line in stream:
            match = HEADER.match(line.rstrip())
            if match:
                name = match[1]
                current = {"bytes": int(match[2]), "alignment": int(match[3]), "fields": []}
                layouts[name] = current
            elif current is not None and line.startswith("print-type-size"):
                current["fields"].append(line.rstrip())
            else:
                current = None
    # An older revision names the row commit `commit_tip_changes`; format 7 names it `commit_rows`.
    if not layouts or not any(part in name for name in layouts
                              for part in ("commit_tip_changes", "commit_rows")):
        raise RuntimeError("The compiler output lacks the row commit layout")
    args.output.write_text(json.dumps({"revision": args.revision, "layouts": layouts},
                                     sort_keys=True, indent=2) + "\n")
    futures = [(name, row) for name, row in layouts.items()
               if "async fn body" in name and any(part in name for part in
                   ("AtomStore", "MutationManager", "execute_", "FoldDB", "BackgroundAtomHistory"))]
    print(f"Source: {args.revision}. Compiler layout only; runtime proof remains separate.")
    print("| Future | Bytes |")
    print("|---|---:|")
    for name, row in sorted(futures, key=lambda item: item[1]["bytes"], reverse=True)[:40]:
        print(f"| {name[:220].replace('|', '/')} | {row['bytes']} |")


if __name__ == "__main__":
    main()
