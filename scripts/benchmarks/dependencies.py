"""Reuse CI dependency artifacts only after all local packages lose their outputs."""

import json
import os
from pathlib import Path
import subprocess
import time


def prepare_target(source, target, environment):
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise ValueError("Benchmark dependency preparation runs only in CI")
    root = Path(os.environ["RUNNER_TEMP"]).resolve()
    if target.is_symlink() or target.resolve().parent != root:
        raise ValueError("The dependency target must stay in this runner's temporary directory")
    if not target.exists():
        return {"target_preexisted": False, "cleaned_path_packages": [], "clean_seconds": 0}
    started = time.monotonic()
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--locked", "--format-version", "1"],
        cwd=source, env=environment, text=True, timeout=120))
    packages = [package for package in metadata["packages"] if package["source"] is None]
    if not any(package["name"] == "lastdb_node" for package in packages):
        raise ValueError("The dependency target has no identified LastDB package to rebuild")
    # Include vendored path dependencies, even when they are not workspace members.
    command = ["cargo", "clean", "--profile", "dev-release", "--locked"]
    for package in packages:
        command.extend(("--package", package["id"]))
    subprocess.run(command, cwd=source, env=environment, check=True, timeout=120)
    for name in ("lastdb", "lastdbd"):
        binary = target / "dev-release" / name
        if binary.exists() or binary.is_symlink():
            raise ValueError("A previous workspace executable survived dependency preparation")
    return {"target_preexisted": True,
            "cleaned_path_packages": sorted(package["name"] for package in packages),
            "clean_seconds": round(time.monotonic() - started, 3)}
