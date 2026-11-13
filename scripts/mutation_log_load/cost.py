"""One ledger for every phase, with a six-dollar hard cap.

An append reads and conditionally updates the database head, stores the
entry, and stores its immutable acceptance fact. The ledger budgets
an extra read plus three transactional writes per attempt. It conservatively
charges the main partition and submission reads during an audit. Cleanup
Query and BatchWriteItem are counted on their own.

Lambda duration is ``LAMBDA_MB / 1024`` GB for 100 ms at the published
GB-second price. 128 is the Lambda default. The mutation-log function's
configured MemorySize is not in this repo.
"""

import threading

from faults import MAX_CONTENTION_TRIES


PAGE_BYTES = 4096
LIST_LIMIT = 90
SERVER_ATTEMPTS = 3
FULL_PARTITION_LOAD = True
MUTATING_WRITE_UNITS = 6
ENTRY_ROW_BYTES = 350
ROW_OVERHEAD_BYTES = 150
ACCEPTED_FACT_BYTES = 512
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


def stored_growth(nbytes):
    """Bytes an append adds. A large body is not rounded down."""
    body = 0 if nbytes is None else nbytes
    return max(body, ENTRY_ROW_BYTES) + ROW_OVERHEAD_BYTES + ACCEPTED_FACT_BYTES


def load_reads(main_bytes, sub_bytes):
    """Reads for one #778 load of the main partition and ``{id}#sub``."""
    sub = read_units(sub_bytes)
    if FULL_PARTITION_LOAD:
        return read_units(main_bytes) + sub + 1
    return 2 + sub


def recheck_hold(partition_bytes, sub_bytes):
    """Worst case for a quiet logcheck of this partition.

    One pass is a head, one list per 90 entries, and one extra list. The
    quiet recheck repeats that pass, and each call can take MAX_CONTENTION_TRIES.
    Page count comes from the partition bytes so a restored ledger still
    holds the same reserve. Dollars include each call's Lambda request.
    """
    per_entry = ENTRY_ROW_BYTES + ROW_OVERHEAD_BYTES
    pages = -(-max(partition_bytes, 0) // (per_entry * LIST_LIMIT))
    loads = 2 * MAX_CONTENTION_TRIES * (1 + pages + 1)
    reads = loads * load_reads(partition_bytes, sub_bytes)
    usd = reads * READ_USD + loads * (LAMBDA_REQUEST_USD + LAMBDA_DURATION_USD)
    return reads, usd


class Ledger:
    def __init__(self, cap_usd=CAP_USD):
        self.cap_usd = cap_usd
        self.lambda_requests = 0
        self.reads = 0
        self.write_units = 0
        self.stored_bytes = 0
        self.partition_bytes = 0
        self.sub_bytes = 0
        self._lock = threading.Lock()

    def estimate(self):
        request_usd = self.lambda_requests * (LAMBDA_REQUEST_USD + LAMBDA_DURATION_USD)
        read_usd = self.reads * READ_USD
        write_usd = self.write_units * WRITE_USD
        storage_usd = self.stored_bytes * STORAGE_USD_PER_BYTE
        built = {
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
        return self._stamp(built, self.partition_bytes)

    def _stamp(self, projected, partition):
        reads, usd = recheck_hold(partition, self.sub_bytes)
        projected["recheck_reserve_reads"] = reads
        projected["recheck_reserve_usd"] = usd
        return projected

    def project(self, kind, nbytes=None, direct=False, grow=None, attempts=1):
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
            reads += load_reads(partition, self.sub_bytes) * attempts
            if kind == "write":
                writes += MUTATING_WRITE_UNITS * attempts
                if grow is None:
                    grow = True
                if grow:
                    growth = stored_growth(size)
                    stored += growth
                    partition += growth
        return self._stamp(_money(self, reads, writes, stored, partition), partition)

    def charge(self, kind, nbytes=None, grow=None, attempts=1, hold_recheck=True):
        with self._lock:
            projected = self.project(kind, nbytes, direct=False, grow=grow, attempts=attempts)
            reserve = projected["recheck_reserve_usd"] if hold_recheck else 0
            if projected["total_usd"] + reserve >= self.cap_usd:
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
