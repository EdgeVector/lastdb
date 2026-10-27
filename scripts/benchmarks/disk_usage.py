"""One bounded file walk separates database storage from Search delivery."""

import stat
import time

CATEGORIES = ("database", "search_queue", "other_home")
SIZES = ("logical_bytes", "allocated_bytes")


def usage(home):
    result = {name: dict.fromkeys(SIZES, 0) for name in (*CATEGORIES, "whole_home")}
    result["search_queue_files"] = 0
    stack, entries, deadline = [(home, 0)], 0, time.monotonic() + 15
    while stack:
        parent, depth = stack.pop()
        if depth > 12 or time.monotonic() > deadline:
            raise RuntimeError("The owned-home size check exceeded its bound")
        try:
            children = parent.iterdir()
            for path in children:
                entries += 1
                if entries > 1_000_000 or time.monotonic() > deadline:
                    raise RuntimeError("The owned-home size check exceeded its bound")
                try:
                    info = path.lstat()
                except FileNotFoundError:
                    continue
                if stat.S_ISLNK(info.st_mode):
                    raise RuntimeError("A generated home contains an unexpected symlink")
                if stat.S_ISDIR(info.st_mode):
                    stack.append((path, depth + 1))
                    continue
                if not stat.S_ISREG(info.st_mode) or not hasattr(info, "st_blocks"):
                    raise RuntimeError("A generated home contains an unsupported disk entry")
                parts = path.relative_to(home).parts
                category = ("search_queue" if parts[:3] == ("apps", "search", "inbox") else
                            "database" if parts[0] in ("data", "blobs") else "other_home")
                result[category]["logical_bytes"] += info.st_size
                result[category]["allocated_bytes"] += info.st_blocks * 512
                if category == "search_queue":
                    result["search_queue_files"] += 1
        except FileNotFoundError:
            # Automatic maintenance can remove a retired directory during the walk.
            continue
    for name in SIZES:
        result["whole_home"][name] = sum(result[category][name] for category in CATEGORIES)
    validate(result)
    return result


def validate(result):
    if set(result) != {*CATEGORIES, "whole_home", "search_queue_files"}:
        raise ValueError("The disk breakdown has incomplete categories")
    for category in (*CATEGORIES, "whole_home"):
        if (set(result[category]) != set(SIZES)
                or any(type(result[category][name]) is not int or result[category][name] < 0 for name in SIZES)):
            raise ValueError("The disk breakdown has invalid sizes")
    if type(result["search_queue_files"]) is not int or result["search_queue_files"] < 0:
        raise ValueError("The Search queue file count is invalid")
    if any(result["whole_home"][name] != sum(result[category][name] for category in CATEGORIES) for name in SIZES):
        raise ValueError("The disk categories do not add to the whole home")
