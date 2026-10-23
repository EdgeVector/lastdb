#!/usr/bin/env python3
"""Remove exact owned build binaries after all generated product data is removed."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import time

ROOT = Path(__file__).resolve().parent


def load(path):
    if path.is_symlink() or path.stat().st_size > 32 * 1024 * 1024:
        raise ValueError("An ownership or cleanup receipt is linked or oversized")
    return json.loads(path.read_text())


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def cleaned_runs():
    manifest = load(ROOT / "pair-runs.json")
    if manifest.get("complete") is not True or len(manifest["runs"]) != 6:
        raise ValueError("All six measured runs must finish before build cleanup")
    folders = [ROOT / row["run"] for row in manifest["runs"]]
    folders += [ROOT / name for name in ("retention-operational", "edge-operational")]
    result = []
    for folder in folders:
        if folder.is_symlink() or folder.resolve(strict=True).parent != ROOT:
            raise ValueError("A product capture leaves the exact proof root")
        summary, cleanup = load(folder / "summary.json"), load(folder / "cleanup.json")
        if (summary.get("complete") is not True or summary.get("all_owned_processes_stopped") is not True
                or cleanup.get("removed") is not True or Path(cleanup["scratch"]).exists()):
            raise ValueError("A generated home lacks its successful exact-path cleanup")
        result.append({"run": folder.name, "cleanup_sha256": digest(folder / "cleanup.json")})
    return result


def main():
    argparse.ArgumentParser(description=__doc__).parse_args()
    receipts = cleaned_runs()
    owner = load(ROOT / "build-scratch-owner.json")
    root = Path(owner["root"])
    if (root.is_symlink() or root.resolve(strict=True) != root
            or root.parent != Path(os.environ["TMPDIR"]).resolve(strict=True)
            or owner.get("primary_data") is not False):
        raise ValueError("Refuse any path except the exact owned temporary build root")
    info = root.stat()
    if (info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700
            or (info.st_dev, info.st_ino, info.st_uid) != (owner["device"], owner["inode"], owner["owner_uid"])
            or load(root / "owner.json") != owner):
        raise ValueError("The build-root identity or owner differs")
    entries = []
    for name in ("before-build.json", "before-cli-build.json", "after-build.json", "after-cli-build.json"):
        value = load(ROOT / name)
        entries.append((Path(value["binary"]), value["binary_sha256"]))
    entries += [(Path(value["binary"]), value["binary_sha256"])
                for value in load(ROOT / "after-disk-build.json")["binaries"].values()]
    before_disk = ROOT / "before-disk-build.json"
    if before_disk.exists():
        entries += [(Path(value["binary"]), value["binary_sha256"])
                    for value in load(before_disk)["binaries"].values()]
    directory = root / "bin"
    if ({path.name for path in root.iterdir()} != {"owner.json", "bin"} or directory.is_symlink()
            or set(directory.iterdir()) != {path for path, _ in entries}):
        raise ValueError("The build scratch contains an unexpected path")
    processes = subprocess.check_output(["ps", "-axo", "pid=,command="], text=True)
    if any(str(path) in processes for path, _ in entries):
        raise ValueError("An owned binary path appears in a current process command")
    for path, expected in entries:
        if (path.parent != directory or path.is_symlink() or not path.is_file()
                or path.stat().st_uid != os.getuid() or digest(path) != expected):
            raise ValueError("An exact recorded build binary differs")
    result = {"owner": owner, "generated_data_cleanup": receipts,
              "binaries": [{"path": str(path), "sha256": expected} for path, expected in entries],
              "at_unix_ns": time.time_ns(), "removed": False}
    target = ROOT / "build-scratch-cleanup.json"
    target.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    for path, _ in entries:
        path.unlink()
    directory.rmdir()
    (root / "owner.json").unlink()
    root.rmdir()
    result["removed"] = not root.exists()
    target.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"removed": result["removed"], "scratch": str(root)}))


if __name__ == "__main__":
    main()
