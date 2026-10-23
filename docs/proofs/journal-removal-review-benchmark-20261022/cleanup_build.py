#!/usr/bin/env python3
"""Remove the exact owned build scratch after all product captures and their cleanup."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import time


def owned(path, directory=False):
    info = path.lstat()
    kind = stat.S_ISDIR if directory else stat.S_ISREG
    if info.st_uid != os.getuid() or not kind(info.st_mode):
        raise ValueError(f"Refuse a symlink, foreign owner, or wrong file type: {path}")
    return info


def load(path):
    if owned(path).st_size > 1024 * 1024:
        raise ValueError("A cleanup input exceeds one MiB")
    return json.loads(path.read_text())


def digest(path):
    if owned(path).st_size > 1024 * 1024 * 1024:
        raise ValueError("A build executable exceeds one GiB")
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def names(path, expected):
    with os.scandir(path) as entries:
        actual = set()
        for entry in entries:
            actual.add(entry.name)
            if len(actual) > len(expected):
                break
    if actual != expected:
        raise ValueError("The exact owned directory entries differ")


def finished(proof, name):
    folder = proof / name
    summary, cleanup = load(folder / "summary.json"), load(folder / "cleanup.json")
    if (summary.get("complete") is not True or summary.get("all_owned_processes_stopped") is not True
            or cleanup.get("removed") is not True or summary.get("scratch") != cleanup.get("scratch")
            or os.path.lexists(cleanup["scratch"])):
        raise ValueError(f"The product capture or its cleanup is incomplete: {name}")
    return {"run": name, "scratch": cleanup["scratch"], "removed": True}


def cleanup(proof):
    target = proof / "build-cleanup.json"
    if os.path.lexists(target):
        raise ValueError("Retain the existing build cleanup receipt; do not overwrite it")
    pairs = load(proof / "pair-runs.json")
    order = [("before", 1), ("after", 1), ("after", 2), ("before", 2), ("before", 3), ("after", 3)]
    runs = pairs.get("runs", [])
    if (pairs.get("complete") is not True or [(v.get("label"), v.get("pair")) for v in runs] != order
            or any(v.get("exit_code") != 0 or v.get("scratch_removed") is not True for v in runs)):
        raise ValueError("All six ordered benchmark runs must finish and remove their scratch")
    completed = [finished(proof, f"{label}-{pair}") for label, pair in order]
    completed.append(finished(proof, "erase-operational-3"))
    builds = [load(proof / f"{name}-build.json") for name in ("before", "after", "cli", "disk")]
    owner = builds[1]["build_scratch_owner"]
    if any(value.get("build_scratch_owner", owner) != owner for value in builds):
        raise ValueError("Build scratch ownership receipts differ")
    root = Path(owner["root"])
    temporary = Path(os.environ["TMPDIR"]).resolve(strict=True)
    info = owned(root, directory=True)
    if (root.resolve(strict=True) != root or root.parent != temporary or owner.get("primary_data") is not False
            or stat.S_IMODE(info.st_mode) != 0o700 or info.st_uid != owner["owner_uid"]
            or info.st_dev != owner["device"] or info.st_ino != owner["inode"]):
        raise ValueError("Refuse any path except the exact mode-0700 owned temporary build root")
    names(root, {"bin", "owner.json", "pr-body.md"})
    if load(root / "owner.json") != owner:
        raise ValueError("The build ownership marker differs")
    binary_dir = root / "bin"
    owned(binary_dir, directory=True)
    paths = [Path(value["binary"]) for value in builds]
    if len(set(paths)) != 4 or any(path.parent != binary_dir for path in paths):
        raise ValueError("The exact four build executable paths differ")
    names(binary_dir, {path.name for path in paths})
    hashes = {str(path): digest(path) for path in paths}
    if any(hashes[str(path)] != value["binary_sha256"] for path, value in zip(paths, builds)):
        raise ValueError("A build executable differs from its recorded SHA-256")
    files = [*paths, root / "pr-body.md", root / "owner.json"]
    identities = {path: owned(path) for path in files}
    started = time.monotonic()
    active = subprocess.run(["/usr/sbin/lsof", "-nP", "-t", "--", *map(str, paths)],
                            stdin=subprocess.DEVNULL, capture_output=True, timeout=10, check=False)
    if active.returncode != 1 or active.stdout or active.stderr:
        raise ValueError("An executable is in use, or the bounded active-file check cannot prove it is unused")
    receipt = {"scratch": str(root), "ownership": owner, "completed_captures": completed,
               "binary_sha256": hashes, "lsof_seconds": time.monotonic() - started,
               "removed": False, "events": [{"removed": False, "at_unix_ns": time.time_ns()}]}
    for path, previous in identities.items():
        current = owned(path)
        if (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns) != (
                previous.st_dev, previous.st_ino, previous.st_size, previous.st_mtime_ns):
            raise ValueError("A known build file changed before removal")
    target.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    for path in files:
        path.unlink()
    binary_dir.rmdir()
    root.rmdir()
    receipt.update(removed=True)
    receipt["events"].append({"removed": True, "at_unix_ns": time.time_ns()})
    target.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"removed": True, "scratch": str(root), "receipt": str(target)}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proof-root", type=Path, default=Path(__file__).resolve().parent)
    cleanup(parser.parse_args().proof_root.resolve(strict=True))


if __name__ == "__main__":
    main()
