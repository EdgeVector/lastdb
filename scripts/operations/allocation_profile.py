#!/usr/bin/env python3
"""Resolve CI allocation call paths. Instrumented timing and RSS are not benchmarks."""
import argparse
import json
from pathlib import Path
import subprocess


def summarize(profile, binary):
    value = json.loads(profile.read_text())
    if value["lost_classes"] or value["lost_sites"] or not value["sites"]:
        raise RuntimeError(f"Incomplete allocation profile: {profile}")
    addresses = sorted({frame for site in value["sites"] for frame in site["frames"] if frame})
    symbols = {}
    for start in range(0, len(addresses), 128):
        batch = addresses[start:start + 128]
        lines = subprocess.check_output(
            ["addr2line", "-f", "-C", "-e", str(binary), *[hex(a) for a in batch]],
            text=True, timeout=60).splitlines()
        if len(lines) != 2 * len(batch):
            raise RuntimeError("The symbol output has unexpected framing")
        symbols.update({address: {"function": lines[i * 2], "source": lines[i * 2 + 1]}
                        for i, address in enumerate(batch)})
    for site in value["sites"]:
        site["symbols"] = [symbols[frame] for frame in site["frames"] if frame]
    profile.with_name("allocation-symbols.json").write_text(json.dumps(value, indent=2) + "\n")
    print(f"### {profile.parent.name}")
    print("Allocation traffic only. No latency or retained-memory conclusion.")
    print("| Usable bytes | Calls | Requested MiB | First caller beyond allocator |")
    print("|---:|---:|---:|---|")
    for site in sorted(value["sites"], key=lambda site: site["requested"], reverse=True)[:25]:
        caller = next((s["function"] for s in site["symbols"]
                       if any(p in s["function"] for p in ("fold_db", "lastdb_host", "lastdb_node"))
                       and "::allocator" not in s["function"]), "see artifact")
        print(f"| {site['usable']} | {site['count']} | {site['requested']/1048576:.2f} | {caller[:200]} |")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--programs", type=Path, required=True)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--candidate", required=True)
    args = parser.parse_args()
    for pair in range(2):
        for label in ("baseline", "candidate"):
            summarize(args.output / f"pair-{pair}-{label}" / "allocation-profile.json",
                      args.programs / getattr(args, label) / "lastdbd")


if __name__ == "__main__":
    main()
