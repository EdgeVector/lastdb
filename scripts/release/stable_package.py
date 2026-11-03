#!/usr/bin/env python3
"""Package a numeric stable version using the nightly's bounded pair writer."""

import argparse
from pathlib import Path

from nightly_package import MAX_BINARY_BYTES, archive_pair, digest, validate_architecture, validate_pair, write_json
from stable_plan import load


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--target", choices=("aarch64-apple-darwin", "x86_64-unknown-linux-gnu"), required=True)
    parser.add_argument("--bin-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    plan = load(args.plan)
    args.output.mkdir(parents=True)
    binaries = validate_pair(args.bin_dir, plan["version"])
    validate_architecture(args.bin_dir, args.target)
    bundle = args.output / f"lastdb-{args.target}.tar.gz"
    archive_pair(bundle, args.bin_dir, binaries, plan["epoch"])
    if bundle.stat().st_size > MAX_BINARY_BYTES:
        raise ValueError("Release archive exceeds the package bound")
    write_json(args.output / f"lastdb-{args.target}.manifest.json", {
        "schema": "lastdb.mini.bundle.v1", "target": args.target,
        "source_git_oid": plan["source"], "version": plan["version"], "binaries": binaries,
        "artifact": {"file": bundle.name, "size": bundle.stat().st_size, "sha256": digest(bundle)}})


if __name__ == "__main__":
    main()
