#!/usr/bin/env python3
"""Move the rejected attempt unchanged before fresh canonical build receipts."""

import hashlib
import json
from pathlib import Path
import time

ROOT = Path(__file__).resolve().parent


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def inventory(path):
    if path.is_symlink():
        raise ValueError("Archive only the exact unlinked evidence paths")
    if path.is_file():
        return {path.name: digest(path)}
    rows = list(path.iterdir())
    if len(rows) > 512 or any(row.is_symlink() or not row.is_file() for row in rows):
        raise ValueError("An exact rejected capture has unexpected nested or oversized entries")
    return {row.name: digest(row) for row in rows}


def main():
    archive, target = ROOT / "rejected-build-1", ROOT / "rejected-build-1-archive.json"
    if archive.exists() or target.exists():
        raise ValueError("Do not replace a rejected attempt archive")
    rejected = json.loads((ROOT / "rejected-cache-build-1.json").read_text())
    if rejected.get("rejected_for_measurement") is not True or rejected.get("all_owned_processes_stopped") is not True:
        raise ValueError("A rejected, stopped attempt must precede the evidence move")
    names = [label + "-" + suffix for label in ("before", "after") for suffix in
             ("cache-preparation.json", "build.json", "cli-build.json", "disk-build.json")]
    names += ["final-build-checks.json", "followup-window.json", "review-runs.json", "operational-runs.json",
              "retention-operational", "after-profile-run.json", "after-profile-1"]
    expected = {name: inventory(ROOT / name) for name in names}
    archive.mkdir(mode=0o700)
    moves = []
    for name in names:
        source, destination = ROOT / name, archive / name
        source.rename(destination)
        if inventory(destination) != expected[name]:
            raise ValueError("An archived original changed bytes")
        moves.append({"original": name, "archived": str(destination.relative_to(ROOT)), "files_sha256": expected[name]})
    result = {"complete": True, "all_bytes_unchanged": True, "moves": moves, "at_unix_ns": time.time_ns(),
              "rejection_manifest_sha256": digest(ROOT / "rejected-cache-build-1.json"),
              "draft_copies_and_receipts_untouched": True, "old_owned_binaries_untouched": True,
              "old_false_summary_remains_false": True,
              "recovery_scope": "Fresh canonical receipts are allowed only for new isolated-target builds and fresh generated homes",
              "driver_sha256": digest(Path(__file__))}
    target.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"rejected_attempt_archived": True, "moved_paths": len(moves)}))


if __name__ == "__main__":
    main()
