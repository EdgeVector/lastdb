#!/usr/bin/env python3
"""Keep the genuine earlier baseline diagnostic before a fresh symmetric pair."""

import json
from pathlib import Path
import time

from archive_rejected import digest, inventory

ROOT = Path(__file__).resolve().parent


def main():
    target = ROOT / "historical-before-profile-archive.json"
    if target.exists():
        raise ValueError("Do not replace a historical diagnostic archive")
    names = {"before-profile-1": "historical-before-profile-1",
             "before-profile-run.json": "historical-before-profile-run.json",
             "before-profile-stack-analysis.json": "historical-before-profile-stack-analysis.json"}
    resumed = False
    for source, destination in names.items():
        if not (ROOT / source).exists() and (ROOT / destination).exists():
            inventory(ROOT / destination)
            (ROOT / destination).rename(ROOT / source)
            resumed = True
    summary = json.loads((ROOT / "before-profile-1/summary.json").read_text())
    if summary.get("complete") is not True or summary.get("all_owned_processes_stopped") is not True:
        raise ValueError("The historical baseline diagnostic must stay complete and stopped")
    values = {name: inventory(ROOT / name) for name in names}
    for source, destination in names.items():
        if (ROOT / destination).exists():
            raise ValueError("Do not replace historical evidence")
        (ROOT / source).rename(ROOT / destination)
        if sorted(inventory(ROOT / destination).values()) != sorted(values[source].values()):
            raise ValueError("An archived historical diagnostic changed bytes")
    target.write_text(json.dumps({"complete": True, "all_bytes_unchanged": True,
         "at_unix_ns": time.time_ns(), "moves": names, "original_files_sha256": values,
         "resumed_filename_comparison_failure": resumed,
         "genuine_old_baseline_preserved": True, "fresh_symmetric_before_after_profiles_planned": True,
         "driver_sha256": digest(Path(__file__))}, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
