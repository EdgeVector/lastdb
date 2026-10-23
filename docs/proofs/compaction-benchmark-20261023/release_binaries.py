#!/usr/bin/env python3
"""Remove only the two recorded benchmark executables after all node stops."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import time


def load(path):
    return json.loads(path.read_text())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cohort", type=Path, required=True)
    args = parser.parse_args()
    cohort = args.cohort.resolve(strict=True)
    rejected = cohort / "rejection.json"
    if rejected.exists():
        if load(rejected).get("rejected") is not True:
            raise ValueError("The cohort lacks an explicit rejection")
        runs = list((cohort / "measurements").glob("pair-*/summary.json"))
        if len(runs) != 2:
            raise ValueError("The rejected cohort differs from its two recorded runs")
    else:
        runs = list((cohort / "measurements").glob("pair-*/summary.json"))
        if len(runs) != 6 or not (cohort / "comparison.json").exists():
            raise ValueError("The accepted comparison remains incomplete")
    for path in runs:
        summary = load(path)
        if summary.get("all_owned_processes_stopped") is not True:
            raise ValueError("A benchmark node remains active")
        receipt = path.parent / ("cleanup.json" if summary.get("complete")
                                else "rejected-cleanup.json")
        if load(receipt).get("removed") is not True:
            raise ValueError("A generated home remains")
    owner, builds = load(cohort / "binary-ownership.json"), load(cohort / "builds.json")
    root = Path(owner["path"])
    if (root.is_symlink() or root.resolve(strict=True) != root
            or root.parent != Path(os.environ["TMPDIR"]).resolve(strict=True)
            or owner.get("primary_data") is not False):
        raise ValueError("Not the exact owned temporary binary directory")
    info = root.lstat()
    if info.st_uid != os.getuid() or info.st_uid != owner["uid"] or stat.S_IMODE(info.st_mode) != 0o700:
        raise ValueError("The binary directory owner differs")
    if load(root / "owner.json") != owner or {p.name for p in root.iterdir()} != {"before", "after", "owner.json"}:
        raise ValueError("The binary ownership marker or inventory differs")
    files = []
    for label in ("before", "after"):
        binary = root / label
        info = binary.lstat()
        digest = hashlib.sha256(binary.read_bytes()).hexdigest()
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or digest != builds[label]["sha256"] or str(binary) != builds[label]["binary"]):
            raise ValueError("An executable differs from its receipt")
        files.append({"name": label, "sha256": digest, "bytes": info.st_size})
    receipt = {"removed": False, "at_ns": time.time_ns(), "ownership": owner,
               "files": files, "all_generated_homes_removed": True}
    target = cohort / "binary-cleanup.json"
    target.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    for name in ("before", "after", "owner.json"):
        (root / name).unlink()
    root.rmdir()
    receipt["removed"] = True
    target.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"owned_binaries_removed": str(root)}))


if __name__ == "__main__":
    main()
