"""One ledger for every phase, with a six-dollar hard cap.

Each API call loads the whole log partition with a strongly consistent
read: ``ceil(partition bytes / 4 KiB)`` read units, and at least one.
A mutating call also spends 4 transactional write units. Writes use twice
the on-demand write price. Cleanup Query and BatchWriteItem are counted
separately and do not use that API model.

Lambda duration is ``LAMBDA_MB / 1024`` GB for 100 ms at the published
GB-second price. 128 is the Lambda default. The mutation-log function's
configured MemorySize is not in this repo.
"""

import threading


PAGE_BYTES = 4096
MUTATING_WRITE_UNITS = 4
WRITE_USD = 2 * 1.25 / 1_000_000
READ_USD = 0.25 / 1_000_000
LAMBDA_REQUEST_USD = 0.20 / 1_000_000
LAMBDA_MB = 128
LAMBDA_SECONDS = 0.1
LAMBDA_GB_SECOND_USD = 0.0000166667
LAMBDA_DURATION_USD = (LAMBDA_MB / 1024) * LAMBDA_SECONDS * LAMBDA_GB_SECOND_USD
STORAGE_USD_PER_BYTE = 0.25 / (1024 ** 3)
CAP_USD = 6.0


class CapExceeded(Exception):
    def __init__(self, estimate):
        super().__init__("cost cap")
        self.estimate = estimate


def read_units(partition_bytes):
    if partition_bytes <= 0:
        return 1
    return (partition_bytes + PAGE_BYTES - 1) // PAGE_BYTES


class Ledger:
    def __init__(self, cap_usd=CAP_USD):
        self.cap_usd = cap_usd
        self.lambda_requests = 0
        self.reads = 0
        self.write_units = 0
        self.stored_bytes = 0
        self.partition_bytes = 0
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
            "partition_bytes": self.partition_bytes,
            "request_usd": request_usd,
            "read_usd": read_usd,
            "write_usd": write_usd,
            "storage_usd": storage_usd,
            "total_usd": request_usd + read_usd + write_usd + storage_usd,
            "cap_usd": self.cap_usd,
        }

    def project(self, kind, nbytes=None, direct=False):
        size = 0 if nbytes is None else nbytes
        reads = self.reads
        writes = self.write_units
        stored = self.stored_bytes
        partition = self.partition_bytes
        if direct:
            if kind == "read":
                reads += 1
            if kind == "write":
                writes += max(1, (size + 1023) // 1024)
                stored += size
        else:
            reads += read_units(partition)
            if kind == "write":
                writes += MUTATING_WRITE_UNITS
                stored += size
                partition += size
        return _money(self, reads, writes, stored, partition)

    def charge(self, kind, nbytes=None):
        with self._lock:
            projected = self.project(kind, nbytes, direct=False)
            if projected["total_usd"] >= self.cap_usd:
                raise CapExceeded(projected)
            self._apply(projected)
            return projected

    def record(self, kind, nbytes=None):
        """Count a cleanup call after the cap already stopped the load."""
        with self._lock:
            projected = self.project(kind, nbytes, direct=True)
            self._apply(projected)
            return projected

    def _apply(self, projected):
        self.lambda_requests = projected["lambda_requests"]
        self.reads = projected["reads"]
        self.write_units = projected["write_units"]
        self.stored_bytes = projected["stored_bytes"]
        self.partition_bytes = projected["partition_bytes"]

    def snapshot(self):
        with self._lock:
            return self.estimate()

    def restore(self, saved):
        with self._lock:
            self.lambda_requests = saved["lambda_requests"]
            self.reads = saved["reads"]
            self.write_units = saved["write_units"]
            self.stored_bytes = saved["stored_bytes"]
            self.partition_bytes = saved.get("partition_bytes", 0)
            self.cap_usd = saved["cap_usd"]


def _money(ledger, reads, writes, stored, partition):
    request_usd = (ledger.lambda_requests + 1) * (LAMBDA_REQUEST_USD + LAMBDA_DURATION_USD)
    read_usd = reads * READ_USD
    write_usd = writes * WRITE_USD
    storage_usd = stored * STORAGE_USD_PER_BYTE
    return {
        "lambda_requests": ledger.lambda_requests + 1,
        "reads": reads,
        "write_units": writes,
        "stored_bytes": stored,
        "partition_bytes": partition,
        "request_usd": request_usd,
        "read_usd": read_usd,
        "write_usd": write_usd,
        "storage_usd": storage_usd,
        "total_usd": request_usd + read_usd + write_usd + storage_usd,
        "cap_usd": ledger.cap_usd,
    }


def format_estimate(estimate):
    return "\n".join([
        "mutation-log load estimate",
        "  lambda: {lambda_requests}  ${request_usd:.6f}".format(**estimate),
        "  reads: {reads}  ${read_usd:.6f}".format(**estimate),
        "  writes: {write_units} transactional WRU  ${write_usd:.6f}".format(**estimate),
        "  storage: {stored_bytes} bytes  ${storage_usd:.6f}".format(**estimate),
        "  total: ${total_usd:.6f}  cap: ${cap_usd:.2f}".format(**estimate),
    ])
