"""Named filesystem observations and small post-campaign durability probes in CI."""

import os
from pathlib import Path
import tempfile
import time


def location(path):
    target = Path(path).resolve(strict=True)
    device = os.stat(target).st_dev
    with Path("/proc/self/mountinfo").open() as source:
        text = source.read(131073)
    if len(text) > 131072:
        raise ValueError("Mount metadata exceeds the 128 KiB observation bound")
    matches = []
    for line in text.splitlines():
        fields = line.split()
        split = fields.index("-")
        mount = fields[4].replace("\\040", " ").replace("\\134", "\\")
        if str(target) == mount or str(target).startswith(mount.rstrip("/") + "/"):
            matches.append({"mount": mount, "device": fields[2], "filesystem": fields[split+1],
                            "source": fields[split+2]})
    if not matches:
        raise ValueError("The named filesystem has no observed mount")
    result = max(matches, key=lambda value: len(value["mount"]))
    expected = f"{os.major(device)}:{os.minor(device)}"
    if result["device"] != expected:
        raise ValueError("Mount metadata and the named directory disagree")
    return {**result, "path": str(target), "device_number": device}


def locations():
    return {name: location(path) for name, path in
            (("benchmark_default", "/tmp"), ("runner_temp", os.environ["RUNNER_TEMP"]))}


def probe(parent):
    rows = []
    with tempfile.TemporaryDirectory(prefix="ldb-flush-probe-", dir=parent) as owned:
        root = Path(owned)
        directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            for number in range(32):
                begin, started = time.time_ns() // 1000, time.monotonic_ns()
                with (root / "new").open("xb") as stream:
                    stream.write(b"x" * 4096)
                    stream.flush()
                    file_start = time.monotonic_ns()
                    os.fsync(stream.fileno())
                    file_us = (time.monotonic_ns() - file_start) / 1000
                rename_start = time.monotonic_ns()
                os.replace(root / "new", root / "CURRENT")
                rename_us = (time.monotonic_ns() - rename_start) / 1000
                dir_start = time.monotonic_ns()
                os.fsync(directory)
                rows.append({"sample": number, "started_us": begin, "ended_us": time.time_ns() // 1000,
                             "total_us": (time.monotonic_ns()-started)/1000,
                             "file_flush_us": file_us, "rename_us": rename_us,
                             "directory_flush_us": (time.monotonic_ns()-dir_start)/1000})
        finally:
            os.close(directory)
    return rows


def probe_locations(replica):
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
        raise ValueError("Filesystem probes require hosted CI")
    paths = [("benchmark_default", "/tmp"), ("runner_temp", os.environ["RUNNER_TEMP"])]
    if replica:
        paths.reverse()
    return {name: {"location": location(path), "samples": probe(path)} for name, path in paths}
