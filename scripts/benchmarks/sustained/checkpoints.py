"""Opt-in, quiescent logical checkpoints, distinct from the timed load windows."""

import time

from diagnostics import Diagnostics


def unix_us():
    return time.time_ns() // 1000


class Checkpoints:
    def __init__(self, node, output, enabled):
        self.node = node
        self.capture = Diagnostics(output, output / "node.log") if enabled else None

    def interval(self, name, start_us, expected_requests=None):
        if self.capture:
            self.capture.interval(name, start_us, unix_us(), expected_requests)

    def checkpoint(self, name):
        if not self.capture:
            return
        started = unix_us()
        audit = self.node.native_status(inventory=True)
        home_files = self.node.disk_usage()
        ended = unix_us()
        self.capture.checkpoint(name, audit, started, ended, home_files=home_files)
        self.capture.interval(f"checkpoint_{name}", started, ended)
        # Diagnostics validates completeness and fails the final result. Keep
        # independent load/timing evidence even when an audit reaches its cap.

    def settle(self, seconds, telemetry):
        if not self.capture:
            return
        started = unix_us()
        print(f"IDLE seconds={seconds}; no writes or forced compaction", flush=True)
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            telemetry.set_load({"phase": "idle", "seconds": seconds})
            time.sleep(min(1, max(0, deadline - time.monotonic())))
        self.interval("idle", started)

    def recovery(self, action):
        if not self.capture:
            return action()
        started = unix_us()
        log_path = self.node.output / "node.log"
        offset = log_path.stat().st_size
        try:
            return action()
        finally:
            self.capture.exclude_bytes("intentional_crash", offset, log_path.stat().st_size)
            self.interval("recovery", started)

    def finish(self):
        return self.capture.finish() if self.capture else None
