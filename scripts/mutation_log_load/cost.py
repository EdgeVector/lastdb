"""One ledger for every phase, with a six-dollar hard cap.

Writes use the transactional price, twice the on-demand write price.
Reads, Lambda requests, a 128 MB / 100 ms Lambda duration, storage, and
cleanup all add to the same total.
"""

import threading


WRITE_USD = 2 * 1.25 / 1_000_000
READ_USD = 0.25 / 1_000_000
LAMBDA_REQUEST_USD = 0.20 / 1_000_000
LAMBDA_DURATION_USD = 0.125 * 0.1 * 0.0000166667
STORAGE_USD_PER_BYTE = 0.25 / (1024 ** 3)
CAP_USD = 6.0


class CapExceeded(Exception):
    def __init__(self, estimate):
        super().__init__("cost cap")
        self.estimate = estimate


class Ledger:
    def __init__(self, cap_usd=CAP_USD):
        self.cap_usd = cap_usd
        self.lambda_requests = 0
        self.reads = 0
        self.write_units = 0
        self.stored_bytes = 0
        self._lock = threading.Lock()

    def estimate(self):
        request_usd = self.lambda_requests * (LAMBDA_REQUEST_USD + LAMBDA_DURATION_USD)
        read_usd = self.reads * READ_USD
        write_usd = self.write_units * WRITE_USD
        storage_usd = self.stored_bytes * STORAGE_USD_PER_BYTE
        return {
            "lambda_requests": self.lambda_requests,
            "reads": self.reads,
            "write_units": self.write_units,
            "stored_bytes": self.stored_bytes,
            "request_usd": request_usd,
            "read_usd": read_usd,
            "write_usd": write_usd,
            "storage_usd": storage_usd,
            "total_usd": request_usd + read_usd + write_usd + storage_usd,
            "cap_usd": self.cap_usd,
        }

    def project(self, kind, nbytes=None):
        reads = self.reads + (1 if kind == "read" else 0)
        writes = self.write_units
        stored = self.stored_bytes
        if kind == "write":
            size = 0 if nbytes is None else nbytes
            writes += max(1, (size + 1023) // 1024)
            stored += size
        request_usd = (self.lambda_requests + 1) * (LAMBDA_REQUEST_USD + LAMBDA_DURATION_USD)
        read_usd = reads * READ_USD
        write_usd = writes * WRITE_USD
        storage_usd = stored * STORAGE_USD_PER_BYTE
        return {
            "lambda_requests": self.lambda_requests + 1,
            "reads": reads,
            "write_units": writes,
            "stored_bytes": stored,
            "request_usd": request_usd,
            "read_usd": read_usd,
            "write_usd": write_usd,
            "storage_usd": storage_usd,
            "total_usd": request_usd + read_usd + write_usd + storage_usd,
            "cap_usd": self.cap_usd,
        }

    def charge(self, kind, nbytes=None):
        with self._lock:
            projected = self.project(kind, nbytes)
            if projected["total_usd"] >= self.cap_usd:
                raise CapExceeded(projected)
            self._apply(projected)
            return projected

    def record(self, kind, nbytes=None):
        """Count a cleanup call after the cap already stopped the load."""
        with self._lock:
            projected = self.project(kind, nbytes)
            self._apply(projected)
            return projected

    def _apply(self, projected):
        self.lambda_requests = projected["lambda_requests"]
        self.reads = projected["reads"]
        self.write_units = projected["write_units"]
        self.stored_bytes = projected["stored_bytes"]

    def snapshot(self):
        with self._lock:
            return self.estimate()

    def restore(self, saved):
        with self._lock:
            self.lambda_requests = saved["lambda_requests"]
            self.reads = saved["reads"]
            self.write_units = saved["write_units"]
            self.stored_bytes = saved["stored_bytes"]
            self.cap_usd = saved["cap_usd"]


def format_estimate(estimate):
    return "\n".join([
        "mutation-log load estimate",
        "  lambda: {lambda_requests}  ${request_usd:.6f}".format(**estimate),
        "  reads: {reads}  ${read_usd:.6f}".format(**estimate),
        "  writes: {write_units} transactional WRU  ${write_usd:.6f}".format(**estimate),
        "  storage: {stored_bytes} bytes  ${storage_usd:.6f}".format(**estimate),
        "  total: ${total_usd:.6f}  cap: ${cap_usd:.2f}".format(**estimate),
    ])
