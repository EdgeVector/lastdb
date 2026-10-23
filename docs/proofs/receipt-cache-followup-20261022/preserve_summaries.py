#!/usr/bin/env python3
"""Index all original summaries before exact saved-receipt compression."""

import hashlib
import json
from pathlib import Path
import shutil

ROOT = Path(__file__).resolve().parent


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    target = ROOT / "pre-packaging-summaries.json"
    if target.exists():
        raise ValueError("Do not replace the original-summary preservation index")
    if json.loads((ROOT / "build-scratch-cleanup.json").read_text()).get("removed") is not True:
        raise ValueError("Complete all generated-home and owned-build cleanup first")
    paths = sorted(ROOT.glob("*/summary.json"))
    paths += [ROOT / "rejected-build-1" / name / "summary.json" for name in ("after-profile-1", "retention-operational")]
    paths += [ROOT / name for name in ("summary.json", "summary.txt")]
    if len(paths) > 32 or len(paths) < 24:
        raise ValueError("The finite original-summary inventory differs from the completed proof scopes")
    files = {}
    for path in paths:
        if path.is_symlink() or path.stat().st_size > 2 * 1024 * 1024:
            raise ValueError("An original summary is linked or exceeds the blob limit")
        files[str(path.relative_to(ROOT))] = {"bytes": path.stat().st_size, "sha256": digest(path)}
    for suffix in ("json", "txt"):
        source, destination = ROOT / ("summary." + suffix), ROOT / ("analysis-original-summary." + suffix)
        if destination.exists():
            raise ValueError("Do not replace an original published-summary copy")
        shutil.copyfile(source, destination)
        if digest(source) != digest(destination):
            raise ValueError("The exact published-summary copy changed bytes")
    target.write_text(json.dumps({"complete": True, "files": files,
         "original_summary_files": len(files), "driver_sha256": digest(Path(__file__))}, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"original_summary_files_preserved": len(files)}))


if __name__ == "__main__":
    main()
