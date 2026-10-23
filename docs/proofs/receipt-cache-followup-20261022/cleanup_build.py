#!/usr/bin/env python3
"""Remove the exact fifteen owned binary copies after every product capture."""

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
        raise ValueError("A cleanup receipt is linked or exceeds its finite size bound")
    return json.loads(path.read_text())


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def cleaned_runs():
    pairs = load(ROOT / "pair-runs.json")
    if pairs.get("complete") is not True or len(pairs["runs"]) != 6:
        raise ValueError("All six controlled captures must finish before binary cleanup")
    names = [row["run"] for row in pairs["runs"]]
    names += ["retention-operational", "edge-operational", "overflow-operational",
              "auto-reclaim-operational", "orphan-repeat-operational"]
    names += [variant + "-profile-1" for variant in ("before", "draft", "after")]
    names += ["auto-" + variant + "-1" for variant in ("before", "draft", "after")]
    result = []
    for name in names:
        folder = ROOT / name
        if folder.is_symlink() or folder.resolve(strict=True).parent != ROOT:
            raise ValueError("A capture folder leaves the exact proof root")
        summary, cleanup = load(folder / "summary.json"), load(folder / "cleanup.json")
        if (summary.get("complete") is not True or summary.get("all_owned_processes_stopped") is not True
                or cleanup.get("removed") is not True or Path(cleanup["scratch"]).exists()):
            raise ValueError("A generated home lacks successful stopped-process exact-path cleanup")
        result.append({"run": name, "summary_sha256": digest(folder / "summary.json"),
                       "cleanup_sha256": digest(folder / "cleanup.json")})
    older = load(ROOT / "edge-operational" / "older-program-refusal.json")
    if (older.get("complete") is not True or older.get("read_refused") is not True
            or older.get("write_refused") is not True or older.get("exact_physical_file_hashes_unchanged") is not True):
        raise ValueError("The older-program refusal capture is incomplete")
    for variant in ("before", "draft", "after"):
        for name in (variant + "-profile-run.json", "auto-" + variant + "-1-run.json"):
            value = load(ROOT / name)
            if value.get("complete") is not True or value.get("generated_home_removed") is not True:
                raise ValueError("An extended capture coordinator is incomplete")
    return result


def binaries():
    result = []
    for variant in ("before", "draft", "after"):
        for suffix in ("build.json", "cli-build.json"):
            value = load(ROOT / (variant + "-" + suffix))
            result.append((Path(value["binary"]), value["binary_sha256"]))
        value = load(ROOT / (variant + "-disk-build.json"))
        result.extend((Path(item["binary"]), item["binary_sha256"]) for item in value["binaries"].values())
    accepted = load(ROOT / "final-build-checks.json")
    expected = {(row["binary"], row["sha256"]) for row in accepted["accepted_binaries"]}
    if accepted.get("complete") is not True or len(result) != 15 or expected != {(str(p), h) for p, h in result}:
        raise ValueError("The fifteen exact binary copies differ from final build acceptance")
    return result


def main():
    argparse.ArgumentParser(description=__doc__).parse_args()
    target = ROOT / "build-scratch-cleanup.json"
    if target.exists():
        raise ValueError("Do not replace an earlier build-scratch cleanup receipt")
    receipts, entries = cleaned_runs(), binaries()
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
        raise ValueError("The temporary build-root identity differs from its owner receipt")
    directory = root / "bin"
    if ({path.name for path in root.iterdir()} != {"owner.json", "bin"} or directory.is_symlink()
            or set(directory.iterdir()) != {path for path, _ in entries}):
        raise ValueError("The owned build scratch contains an unexpected path")
    processes = subprocess.check_output(["ps", "-axo", "pid=,command="], text=True)
    if any(str(path) in processes for path, _ in entries):
        raise ValueError("An owned binary path appears in a current process command")
    for path, expected in entries:
        if (path.parent != directory or path.is_symlink() or not path.is_file()
                or path.stat().st_uid != os.getuid() or digest(path) != expected):
            raise ValueError("An exact recorded binary differs")
    result = {"owner": owner, "generated_data_cleanup": receipts, "removed": False,
              "binaries": [{"path": str(path), "sha256": checksum} for path, checksum in entries],
              "at_unix_ns": time.time_ns(), "driver_sha256": digest(Path(__file__))}
    target.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    for path, _ in entries:
        path.unlink()
    directory.rmdir()
    (root / "owner.json").unlink()
    root.rmdir()
    result["removed"] = not root.exists()
    target.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"removed": result["removed"], "scratch": str(root), "binaries": len(entries)}))


if __name__ == "__main__":
    main()
