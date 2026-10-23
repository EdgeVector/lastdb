"""Bounded metadata-only size samples that tolerate concurrent native cleanup."""

import hashlib
import os
from pathlib import Path
import stat
import time

LIMITS = {"native_bytes": 4 * 1024 ** 3, "native_files": 20000,
          "inventory_entries": 40000, "inventory_depth": 8, "inventory_seconds": 20}


def inventory(home):
    root = home / "data" / "disk-pack"
    if root.is_symlink() or not root.is_dir():
        raise RuntimeError("The generated native home is missing or linked")
    started, started_unix = time.monotonic_ns(), time.time_ns()
    deadline = time.monotonic() + LIMITS["inventory_seconds"]
    pending, files, families, entries, disappeared = [(root, 0)], {}, {}, 0, 0
    logical, allocated = 0, 0
    while pending:
        parent, depth = pending.pop()
        if depth > LIMITS["inventory_depth"] or time.monotonic() > deadline:
            raise RuntimeError("The concurrent inventory exceeds its depth or time bound")
        try:
            with os.scandir(parent) as rows:
                for row in rows:
                    entries += 1
                    if entries > LIMITS["inventory_entries"] or time.monotonic() > deadline:
                        raise RuntimeError("The concurrent inventory exceeds its entry or time bound")
                    try:
                        info = row.stat(follow_symlinks=False)
                    except FileNotFoundError:
                        disappeared += 1
                        continue
                    if info.st_uid != os.getuid() or stat.S_ISLNK(info.st_mode):
                        raise RuntimeError("The concurrent generated inventory is linked or foreign")
                    path = Path(row.path)
                    if stat.S_ISDIR(info.st_mode):
                        pending.append((path, depth + 1))
                    elif stat.S_ISREG(info.st_mode):
                        name = str(path.relative_to(root))
                        size, blocks = info.st_size, info.st_blocks * 512
                        logical += size
                        allocated += blocks
                        if (len(files) >= LIMITS["native_files"] or logical > LIMITS["native_bytes"]
                                or allocated > LIMITS["native_bytes"]):
                            raise RuntimeError("The concurrent generated home exceeds its file or byte bound")
                        files[name] = [size, blocks]
                        family = name.split("/")[0] if "/" in name else "root-markers"
                        total = families.setdefault(family, {"files": 0, "bytes": 0, "allocated_bytes": 0})
                        total["files"] += 1
                        total["bytes"] += size
                        total["allocated_bytes"] += blocks
                    else:
                        raise RuntimeError("The native metadata inventory contains an unsupported object")
        except (FileNotFoundError, NotADirectoryError):
            disappeared += 1
    digest = hashlib.sha256()
    for name, values in sorted(files.items()):
        digest.update(name.encode() + b"\0" + str(values[0]).encode() + b":" + str(values[1]).encode() + b"\n")
    return {"started_unix_ns": started_unix, "ended_unix_ns": time.time_ns(),
            "started_monotonic_ns": started, "ended_monotonic_ns": time.monotonic_ns(),
            "files": len(files), "bytes": logical, "allocated_bytes": allocated, "families": families,
            "disappeared_entries": disappeared, "visited_entries": entries,
            "inventory_sha256": digest.hexdigest(), "atomic_inventory": False,
            "scope": "Interval-stamped file metadata; concurrent writes or cleanup can change files during the walk"}
