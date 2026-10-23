#!/usr/bin/env python3
"""Record exact source-only DEV identities for later worktree cleanup."""

import json
import os
from pathlib import Path
import time

from build_variant import ROOT, digest, frozen, output, save
from run_isolated_builds import SOURCES


def main():
    target = ROOT / "isolated-source-worktrees.json"
    if target.exists():
        raise ValueError("Do not replace an original source worktree identity receipt")
    rows = []
    for variant, (name, revision) in SOURCES.items():
        path = Path(name)
        frozen(path, revision)
        info = path.stat()
        if (path.is_symlink() or path.resolve(strict=True) != path or info.st_uid != os.getuid()
                or (path / "target").exists() or not (path / ".git").is_file()):
            raise ValueError("Require the exact owned source-only detached DEV worktree")
        rows.append({"variant": variant, "path": name, "revision": revision,
                     "device": info.st_dev, "inode": info.st_ino, "owner_uid": info.st_uid,
                     "git_common_dir": output(path, "git", "rev-parse", "--git-common-dir"),
                     "source_only": True, "primary_database_copied": False})
    save(target, {"complete": True, "worktrees": rows, "at_unix_ns": time.time_ns(),
                  "cleanup_owner": "parent agent after all builds and source guards finish",
                  "driver_sha256": digest(Path(__file__))})
    print(json.dumps({"source_worktree_identities_recorded": len(rows)}))


if __name__ == "__main__":
    main()
