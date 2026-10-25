"""Size samples of a node folder, read in a thread while the proof runs.

Each sample walks the node folder (`<home>/data`) and separates the v1
entries from the rest. Before the switch the rest is the v2 build folders;
after it, the v2 node folder. A walk is torn when a file or folder that it
listed goes away before its stat (a merge or the switch removed it): such a
walk can miss both the old and the new file, so it reads low. A torn walk
runs again, up to WALK_TRIES times, and a sample that stays torn is not used.
"""

import os
import threading
import time

V1_ENTRIES = ("disk-pack", "laststore-layout-v1")
MAX_DEPTH = 16
WALK_TRIES = 5


def tree(path, depth):
    """(apparent bytes, allocated bytes, files, torn entries) under path."""
    apparent = allocated = files = torn = 0
    try:
        entries = list(os.scandir(path))
    except (FileNotFoundError, NotADirectoryError):
        return 0, 0, 0, 1
    for entry in entries:
        try:
            if entry.is_dir(follow_symlinks=False):
                if depth >= MAX_DEPTH:
                    raise RuntimeError(f"the node folder is deeper than {MAX_DEPTH} levels")
                sub = tree(entry.path, depth + 1)
                apparent, allocated = apparent + sub[0], allocated + sub[1]
                files, torn = files + sub[2], torn + sub[3]
                continue
            info = entry.stat(follow_symlinks=False)
        except FileNotFoundError:
            torn += 1
            continue
        apparent += info.st_size
        allocated += info.st_blocks * 512
        files += 1
    return apparent, allocated, files, torn


def walk(data):
    """One walk of the node folder: all of it, its v1 entries, each entry,
    and the count of entries that went away during the walk."""
    sizes = {"apparent": 0, "allocated": 0, "files": 0, "v1_apparent": 0,
             "v1_allocated": 0, "entries": {}, "torn": 0}
    try:
        entries = list(os.scandir(data))
    except FileNotFoundError:
        return sizes
    for entry in entries:
        try:
            if entry.is_dir(follow_symlinks=False):
                apparent, allocated, files, torn = tree(entry.path, 1)
            else:
                info = entry.stat(follow_symlinks=False)
                apparent, allocated, files, torn = info.st_size, info.st_blocks * 512, 1, 0
        except FileNotFoundError:
            sizes["torn"] += 1
            continue
        sizes["apparent"] += apparent
        sizes["allocated"] += allocated
        sizes["files"] += files
        sizes["torn"] += torn
        if entry.name in V1_ENTRIES:
            sizes["v1_apparent"] += apparent
            sizes["v1_allocated"] += allocated
        sizes["entries"][entry.name] = allocated
    return sizes


def folder_sizes(data):
    """The sizes of the node folder from a walk that is not torn. When each
    try is torn, the result of the last try has a nonzero `torn`."""
    for _ in range(WALK_TRIES):
        sizes = walk(data)
        if not sizes["torn"]:
            break
    return sizes


def v2_bytes(sizes):
    """The node folder without its v1 entries: the larger of the two counts."""
    return max(sizes["apparent"] - sizes["v1_apparent"],
               sizes["allocated"] - sizes["v1_allocated"])


class Progress:
    """The share of a phase's mutations that the node acknowledged (0 to 1)."""

    def __init__(self):
        self.value = 1.0


class Sampler:
    def __init__(self, data, interval=0.005):
        self.data, self.interval = data, interval
        self.progress = Progress()
        self.lock = threading.Lock()
        self.halt = threading.Event()
        self.window = None
        self.errors = []
        self.thread = threading.Thread(target=self.loop, daemon=True)

    def start(self):
        self.thread.start()

    def stop(self):
        self.halt.set()
        self.thread.join(timeout=60)

    def alive(self):
        """False when the loop stopped before `stop`: after too many errors."""
        return self.thread.is_alive() or self.halt.is_set()

    def begin(self, name, writes=False):
        """Close the open window and start the next one in one step, so no
        time falls between them. A write window starts at progress 0."""
        with self.lock:
            previous = self.close()
            self.progress.value = 0.0 if writes else 1.0
            self.window = {"name": name, "started": time.monotonic(), "samples": [],
                           "torn": 0}
        return previous

    def end(self):
        with self.lock:
            window = self.close()
            self.progress.value = 1.0
        return window

    def close(self):
        window, self.window = self.window, None
        if window is not None:
            window["seconds"] = round(time.monotonic() - window["started"], 3)
        return window

    def loop(self):
        while not self.halt.is_set():
            # The walk runs inside the lock: a closed window holds each sample
            # that started in it.
            with self.lock:
                if self.window is not None:
                    # The progress is read before the walk: the denominator
                    # never counts a mutation that the node had not acknowledged.
                    progress = self.progress.value
                    try:
                        sizes = folder_sizes(self.data)
                    except (OSError, RuntimeError) as failure:
                        self.errors.append(f"{type(failure).__name__}: {failure}")
                        if len(self.errors) > 100:
                            return
                    else:
                        if sizes["torn"]:
                            self.window["torn"] += 1
                        else:
                            self.window["samples"].append({
                                "at": round(time.monotonic() - self.window["started"], 3),
                                "progress": progress, "v2": v2_bytes(sizes),
                                "allocated": sizes["allocated"], "apparent": sizes["apparent"]})
            self.halt.wait(self.interval)
