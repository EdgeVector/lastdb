"""Bounded Linux process and generated-home measurements for cloud benchmarks."""

import json
import os
from pathlib import Path
import shutil
import threading
import time


MIB = 1024 ** 2


def process_counters(pid):
    """Keep device byte counters distinct from bytes passed to file calls."""
    result = {}
    try:
        for line in Path(f"/proc/{pid}/io").read_text().splitlines():
            name, value = line.split(":", 1)
            result[name] = int(value)
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith(("VmRSS:", "VmHWM:")):
                name, value, _ = line.split()
                result[name.rstrip(":")] = int(value) * 1024
        # The command name can contain spaces and parentheses.
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        result["cpu_seconds"] = (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
    except (OSError, ValueError, IndexError):
        result["unavailable"] = True
    return result


def run_file_counts(home):
    result = {}
    for name in ("disk-run", "disk-cas"):
        path = home / "data" / name / "runs"
        count = 0
        if path.exists():
            for entry in path.iterdir():
                count += int(entry.suffix == ".ldrun")
                if count > 100_000:
                    raise RuntimeError("Generated run count exceeds the measurement bound")
        result[name] = count
    return result


class Telemetry:
    def __init__(self, node, output):
        self.node, self.output = node, output
        self.started = time.monotonic()
        self.lock = threading.Lock()
        self.stopped = threading.Event()
        self.load, self.errors = {}, []
        self.first, self.last = None, None
        self.peak_memory, self.peak_driver_memory, self.peak_disk = 0, 0, 0
        self.count = 0
        self.stream = (output / "resources.jsonl").open("w")
        self.thread = threading.Thread(target=self._loop, name="resource-sampler", daemon=True)
        self.thread_started = False

    def set_load(self, snapshot):
        with self.lock:
            if self.errors:
                raise RuntimeError(f"Measurement stopped: {self.errors[0]}")
            self.load = dict(snapshot)

    def note_error(self, message):
        with self.lock:
            if len(self.errors) < 16:
                self.errors.append(str(message)[:600])

    def start(self):
        self.sample()
        self.thread.start()
        self.thread_started = True

    def sample(self):
        at = time.monotonic()
        process = self.node.process
        if process is None:
            return None
        with self.lock:
            load = dict(self.load)
        counters = process_counters(process.pid)
        driver = process_counters(os.getpid())
        usage = self.node.disk_usage()
        sample = {"elapsed_seconds": time.monotonic() - self.started,
                  "pid": process.pid, "node": counters, "driver": driver,
                  "disk": usage, "free_bytes": shutil.disk_usage(self.node.home).free,
                  "run_files": run_file_counts(self.node.home), "load": load}
        sample["sample_seconds"] = time.monotonic() - at
        if counters.get("unavailable") or driver.get("unavailable"):
            self.note_error("Linux process counters unavailable")
        if sample["free_bytes"] < 4 * 1024 ** 3:
            self.note_error("Free disk space fell below the 4 GiB reserve")
        self.peak_memory = max(self.peak_memory, counters.get("VmRSS", 0))
        self.peak_driver_memory = max(self.peak_driver_memory, driver.get("VmRSS", 0))
        self.peak_disk = max(self.peak_disk, usage["allocated_bytes"])
        self.first = self.first or sample
        self.last = sample
        self.count += 1
        self.stream.write(json.dumps(sample, sort_keys=True) + "\n")
        self.stream.flush()
        return sample

    def _loop(self):
        while not self.stopped.wait(10):
            try:
                self.sample()
            except Exception as error:
                self.note_error(f"resource sample: {error}")
                self.stopped.set()

    def finish(self):
        self.stopped.set()
        try:
            if self.thread_started:
                self.thread.join(timeout=20)
                if self.thread.is_alive():
                    raise RuntimeError("Resource sampler did not stop")
            self.sample()
        finally:
            self.stream.close()
        return {"first": self.first, "last": self.last, "samples": self.count,
                "peak_node_memory_bytes": self.peak_memory,
                "peak_driver_memory_bytes": self.peak_driver_memory,
                "peak_allocated_bytes": self.peak_disk, "errors": self.errors}
