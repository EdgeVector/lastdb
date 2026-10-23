"""Timestamp the real owned sample process and its bounded progress streams."""

from pathlib import Path
import subprocess
import threading
import time


class Sampler:
    def __init__(self, pid, output):
        self.markers, self.errors, self.exit = [], [], None
        self.dispatch_monotonic_ns, self.dispatch_unix_ns = time.monotonic_ns(), time.time_ns()
        self.argv = ["/usr/bin/sample", str(pid), "20", "5", "-mayDie", "-file", str(output / "sample.txt")]
        self.process = subprocess.Popen(self.argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE)
        self.threads = []
        for name, stream in (("stdout", self.process.stdout), ("stderr", self.process.stderr)):
            thread = threading.Thread(target=self.read, args=(name, stream, output), daemon=True)
            thread.start()
            self.threads.append(thread)
        self.waiter = threading.Thread(target=self.wait, daemon=True)
        self.waiter.start()

    def read(self, name, stream, output):
        total = 0
        try:
            with (output / f"sample-{name}.txt").open("wb") as target:
                while True:
                    line = stream.readline(4097)
                    if not line:
                        return
                    total += len(line)
                    if len(line) > 4096 or total > 2 * 1024 ** 2:
                        raise RuntimeError("The sample progress stream exceeds its line or byte bound")
                    target.write(line)
                    text = line.decode(errors="replace")
                    if "Sampling process" in text or "Sampling completed" in text:
                        if len(self.markers) >= 32:
                            raise RuntimeError("The sample progress markers exceed their finite count")
                        self.markers.append({"stream": name, "text": text.rstrip(),
                                             "monotonic_ns": time.monotonic_ns(), "unix_ns": time.time_ns()})
        except BaseException as failure:
            self.errors.append(str(failure))
        finally:
            stream.close()

    def wait(self):
        code = self.process.wait()
        self.exit = {"exit_code": code, "monotonic_ns": time.monotonic_ns(), "unix_ns": time.time_ns()}

    def stop(self):
        if self.process.poll() is None:
            self.process.terminate()
        self.waiter.join(timeout=10)
        if self.waiter.is_alive():
            self.process.kill()
            self.waiter.join(timeout=10)
        for thread in self.threads:
            thread.join(timeout=10)
        if self.waiter.is_alive() or any(thread.is_alive() for thread in self.threads):
            raise RuntimeError("The owned sampler or its bounded stream readers do not stop")

    def finish(self, timeout):
        self.waiter.join(timeout=timeout)
        if self.waiter.is_alive():
            raise RuntimeError("The owned sampler exceeds its completion deadline")
        for thread in self.threads:
            thread.join(timeout=10)
        if self.errors or any(thread.is_alive() for thread in self.threads):
            raise RuntimeError("The sample progress capture failed: " + repr(self.errors))
        return {"argv": self.argv, "pid": self.process.pid,
                "dispatch_monotonic_ns": self.dispatch_monotonic_ns, "dispatch_unix_ns": self.dispatch_unix_ns,
                "actual_child_exit": self.exit, "progress_markers": self.markers}

    def overlaps(self, receipts):
        starts = [row for row in self.markers if "Sampling process" in row["text"]]
        ends = [row for row in self.markers if "Sampling completed" in row["text"]]
        if len(starts) != 1 or len(ends) != 1 or starts[0]["monotonic_ns"] >= ends[0]["monotonic_ns"]:
            raise RuntimeError("The sample command does not report one ordered real capture start and completion")
        begin, end = starts[0]["monotonic_ns"], ends[0]["monotonic_ns"]
        return {"reported_capture_start_monotonic_ns": begin, "reported_capture_end_monotonic_ns": end,
                "reported_capture_seconds": (end - begin) / 1e9,
                "requests_overlapping_capture": sum(max(row["started_monotonic_ns"], begin)
                    < min(row["ended_monotonic_ns"], end) for row in receipts),
                "timing_scope": "Host receipt times of the sampler's start and completed progress lines"}
