#!/usr/bin/env python3
"""Link the approved one-time CI entry point; leave product functions unchanged."""
import argparse
import hashlib
from pathlib import Path
import shutil
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--product", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    args.evidence.mkdir(parents=True, exist_ok=True)
    source = Path(__file__).resolve().parent
    product = args.product.resolve(strict=True)
    core = product / "fold_db/crates/core"
    destination = core / "src/pr369_drill"
    destination.mkdir()
    for name in ("mod.rs", "common.rs", "replay.rs", "controls.rs", "files.rs"):
        shutil.copyfile(source / name, destination / name)
    with (core / "src/lib.rs").open("a") as stream:
        stream.write("\npub mod pr369_drill;\n")
    (core / "examples").mkdir(exist_ok=True)
    shutil.copyfile(source / "example.rs", core / "examples/pr369-lifecycle.rs")
    patch = subprocess.run(
        ["git", "-C", str(product), "diff", "--", "fold_db/crates/core/src/lib.rs"],
        check=True, capture_output=True,
    ).stdout
    (args.evidence / "visibility.patch").write_bytes(patch)
    (args.evidence / "visibility.sha256").write_text(hashlib.sha256(patch).hexdigest() + "\n")
    oid = subprocess.run(
        ["git", "-C", str(product), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    (args.evidence / "product-source.txt").write_text(oid + "\n")


if __name__ == "__main__":
    main()
