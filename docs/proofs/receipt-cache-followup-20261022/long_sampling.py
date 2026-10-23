"""Periodic status, process, and race-aware file-size samples for active runs."""

import threading
import time
from retained_inventory import inventory


class Samples:
    def __init__(self, capture, sample_process):
        self.capture, self.sample_process = capture, sample_process
        self.rows, self.errors = [], []
        self.stop_event, self.lock = threading.Event(), threading.Lock()
        self.origin = time.monotonic_ns()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def take(self, label, nominal=None):
        started = time.monotonic_ns()
        native = self.capture.call("/api/db/native-status")["native_disk"]
        if native.get("writer_failed") is not False:
            raise RuntimeError("The active native writer reports failure")
        row = {"label": label, "started_monotonic_ns": started,
               "ended_monotonic_ns": time.monotonic_ns(), "native": native,
               "inventory": inventory(self.capture.home),
               "process": self.sample_process(self.capture.process.pid)}
        row["ended_monotonic_ns"] = time.monotonic_ns()
        if nominal is not None:
            row.update(nominal_monotonic_ns=nominal, start_lateness_seconds=(started - nominal) / 1e9)
        with self.lock:
            if len(self.rows) >= 64:
                raise RuntimeError("The active size samples exceed their finite count")
            self.rows.append(row)
        self.capture.remember("retained-size-sample", **row)
        return row

    def run(self):
        try:
            for ordinal in range(1, 41):
                nominal = self.origin + ordinal * 30_000_000_000
                pause = max(0, (nominal - time.monotonic_ns()) / 1e9)
                if self.stop_event.wait(pause):
                    return
                self.take("periodic-" + str(ordinal), nominal)
        except BaseException as failure:
            self.errors.append(str(failure))

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=125)
        if self.thread.is_alive():
            raise RuntimeError("The owned size-sample thread does not stop within its request bound")
        if self.errors:
            raise RuntimeError("The active size sampler failed: " + repr(self.errors))

    def quiet(self):
        previous, deadline = None, min(self.capture.deadline, time.monotonic() + 30)
        for number in range(30):
            if time.monotonic() > deadline:
                break
            current = self.take("quiet-final-" + str(number))
            native, listing = current["native"], current["inventory"]
            parts = native["parts"]
            idle = (not parts["maintenance_running"] and not parts["reclaim_pending"]
                    and not native["accepted_batches"] and not native["core_warm_dirty"]
                    and native["accepted_sequence"] == native["completed_sequence"]
                    and listing["disappeared_entries"] == 0)
            marker = (listing["inventory_sha256"], native["generation"], native["frontier"],
                      native["accepted_sequence"], native["completed_sequence"])
            if idle and previous == marker:
                return current
            previous = marker if idle else None
            time.sleep(1)
        raise RuntimeError("The quiet tail lacks two stable idle metadata samples within thirty seconds")
