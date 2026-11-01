"""Keep Cargo dependencies warm without reusing local-package code in CodeQL."""

import json
import os
from pathlib import Path
import subprocess


def main():
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise ValueError("CodeQL dependency preparation runs only in CI")
    target = Path(os.environ["CARGO_TARGET_DIR"])
    runner_temp = Path(os.environ["RUNNER_TEMP"]).resolve()
    if target.is_symlink() or target.resolve() != runner_temp / "codeql-rust-target":
        raise ValueError("CodeQL Cargo output must use its isolated runner directory")
    if not target.exists():
        print("No previous CodeQL Cargo target; extraction prepares fresh dependencies")
        return
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--locked", "--format-version", "1"],
        text=True, timeout=120))
    packages = [package for package in metadata["packages"] if package["source"] is None]
    if not any(package["name"] == "lastdb_node" for package in packages):
        raise ValueError("CodeQL dependency preparation did not identify LastDB")
    # Vendored path packages must also rebuild, even outside the workspace members.
    command = ["cargo", "clean", "--profile", "dev", "--locked"]
    for package in packages:
        command.extend(("--package", package["id"]))
    subprocess.run(command, check=True, timeout=120)
    print(f"Removed outputs for {len(packages)} local packages; external dependencies remain")


if __name__ == "__main__":
    main()
