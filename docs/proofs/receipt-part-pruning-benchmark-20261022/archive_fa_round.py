#!/usr/bin/env python3
"""Preserve the complete intermediate fa06955 round without receipt edits."""

import hashlib
import json
from pathlib import Path
import stat
import time

ROOT = Path(__file__).resolve().parent
REVISION = "fa0695505d71b98718834480c10073bc4991d9f7"


def load(path):
    return json.loads(path.read_text())


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def main():
    target = ROOT / "intermediate-fa06955"
    receipt_path = ROOT / "intermediate-fa06955-archive.json"
    if target.exists() or receipt_path.exists():
        raise ValueError("Do not replace an earlier intermediate archive")
    manifest = load(ROOT / "review-runs.json")
    if manifest.get("complete") is not True or manifest["after_revision"] != REVISION:
        raise ValueError("Archive only the complete intermediate fa06955 window")
    owner = load(ROOT / "build-scratch-owner.json")
    directory = Path(owner["root"]) / "bin"
    items = [load(ROOT / name) for name in ("after-build.json", "after-cli-build.json")]
    disk = load(ROOT / "after-disk-build.json")
    if disk["revision"] != REVISION or any(item["revision"] != REVISION for item in items):
        raise ValueError("The intermediate binaries differ from their source revision")
    items += list(disk["binaries"].values())
    binaries = []
    for item in items:
        path = Path(item["binary"])
        renamed = directory / ("intermediate-fa06955-" + path.name.removeprefix("after-"))
        if (path.parent != directory or path.is_symlink() or renamed.exists()
                or not stat.S_ISREG(path.stat().st_mode) or digest(path) != item["binary_sha256"]):
            raise ValueError("An intermediate binary differs from its exact accepted copy")
        binaries.append({"original": str(path), "archived": str(renamed), "sha256": item["binary_sha256"]})
    folders = [f"{label}-{pair}" for label in ("before", "after") for pair in (1, 2, 3)]
    folders += ["retention-operational", "edge-operational", "overflow-operational", "auto-reclaim-operational"]
    names = folders + ["after-build.json", "after-cli-build.json", "after-disk-build.json", "final-build-checks.json",
                       "operational-runs.json", "pair-runs.json", "review-runs.json", "summary.json", "summary.txt"]
    hashes = {}
    for name in names:
        path = ROOT / name
        files = sorted(path.iterdir()) if path.is_dir() else [path]
        if len(files) > 512 or any(file.is_symlink() or not file.is_file() for file in files):
            raise ValueError("An intermediate output contains unexpected entries")
        for file in files:
            hashes[str(file.relative_to(ROOT))] = {"bytes": file.stat().st_size, "sha256": digest(file)}
    for name in folders:
        summary, cleanup = load(ROOT / name / "summary.json"), load(ROOT / name / "cleanup.json")
        if (summary.get("complete") is not True or summary.get("all_owned_processes_stopped") is not True
                or cleanup.get("removed") is not True or Path(cleanup["scratch"]).exists()):
            raise ValueError("An intermediate capture remains incomplete or active")
    target.mkdir()
    for name in names:
        (ROOT / name).rename(target / name)
    for item in binaries:
        Path(item["original"]).rename(item["archived"])
    for name, expected in hashes.items():
        path = target / name
        if path.stat().st_size != expected["bytes"] or digest(path) != expected["sha256"]:
            raise RuntimeError("An archived intermediate receipt changed")
    result = {"complete": True, "source_revision": REVISION, "owner": owner, "archive": str(target),
              "files": hashes, "binary_moves": binaries, "at_unix_ns": time.time_ns(),
              "all_bytes_unchanged": True, "baseline_build_receipts_untouched": True,
              "reason": "A later source fix adds periodic retry for never-receipted orphan files; fa06955 is intermediate"}
    receipt_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"archived_intermediate": str(target), "files": len(hashes), "binaries": len(binaries)}))


if __name__ == "__main__":
    main()
