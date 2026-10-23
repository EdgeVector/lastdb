#!/usr/bin/env python3
"""Build, lint, and capture the real daemon under one host Cargo exclusion."""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    args = parser.parse_args()
    proof = Path(__file__).resolve().parent
    root = proof.parents[2]
    if not args.scratch.is_dir() or not (args.scratch / "candidate").is_file():
        raise RuntimeError("use this capture's existing owned scratch directory")
    checks = []
    commands = [
        ["cargo", "build", "-p", "lastdb_node", "--bin", "lastdbd", "--profile", "dev-release", "--locked"],
        ["cargo", "clippy", "-p", "laststore", "-p", "fold_db", "-p", "lastdb_node",
         "--lib", "--bins", "--locked", "--", "-D", "warnings"],
    ]
    for command in commands:
        began = time.monotonic()
        result = subprocess.run(command, cwd=root)
        checks.append({"command": command, "exit_code": result.returncode,
                       "seconds": time.monotonic() - began})
        (proof / "final-checks.json").write_text(json.dumps(checks, indent=2) + "\n")
        if result.returncode:
            raise RuntimeError("a non-test build or lint check failed")
    binary = args.scratch / "candidate-final"
    shutil.copyfile(root / "target/dev-release/lastdbd", binary)
    binary.chmod(0o755)
    (proof / "final-build.json").write_text(json.dumps({
        "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
        "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "profile": "dev-release",
        "generated_homes_only": True}, indent=2) + "\n")
    for name, home, count, clients, batch, compact in [
        ("small-final", "h3", 1000, 1, 1, False),
        ("compaction-final", "h4", 256, 4, 8, True),
    ]:
        command = [sys.executable, str(proof / "capture.py"), "--binary", str(binary),
                   "--home", str(args.scratch / home), "--output", str(proof / name),
                   "--count", str(count), "--clients", str(clients), "--batch-size", str(batch)]
        if compact:
            command.append("--compact")
        subprocess.run(command, cwd=root, check=True)


if __name__ == "__main__":
    main()
