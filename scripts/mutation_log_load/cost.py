"""One ledger for every phase, with a six-dollar hard cap.

#769 today loads a request with one strongly consistent query of the main
partition and one of ``{id}#sub``. ``load_reads`` is that pair. Head and
list do not conflict, so they are not charged three reloads. An ack
overwrites its row and does not grow the partition. When #769's bounded
reads land, set ``FULL_PARTITION_LOAD`` false and ``load_reads`` charges
the head row plus the submission query instead of the whole main partition.

A mutating call is one ``TransactWriteItems``, charged at twice the
on-demand write price. An append adds an entry row of 350 bytes plus 150
bytes of item overhead. Cleanup Query and BatchWriteItem are counted on
their own.

Lambda duration is ``LAMBDA_MB / 1024`` GB for 100 ms at the published
GB-second price. 128 is the Lambda default. The mutation-log function's
configured MemorySize is not in this repo.
"""

import threading


PAGE_BYTES = 4096
SERVER_ATTEMPTS = 3
FULL_PARTITION_LOAD = True
MUTATING_WRITE_UNITS = 4
ENTRY_ROW_BYTES = 350
ROW_OVERHEAD_BYTES = 150
DEVICE_ROW_BYTES = 800
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
    return max(body, ENTRY_ROW_BYTES) + ROW_OVERHEAD_BYTES


def load_reads(main_bytes, sub_bytes):
    """Reads for one #769 load of the main partition and ``{id}#sub``."""
    sub = read_units(sub_bytes)
    if FULL_PARTITION_LOAD:
        return read_units(main_bytes) + sub
    return 1 + sub


class Ledger:
    def __init__(self, cap_usd=CAP_USD):
        self.cap_usd = cap_usd
        self.lambda_requests = 0
        self.reads = 0
        self.write_units = 0
        self.stored_bytes = 0
        self.partition_bytes = 0
        self.sub_bytes = 0
        self._devices = set()
        self._lock = threading.Lock()

    def note_device(self, device_id):
        """Tip and ack rows are about 400 bytes each and stay in the partition."""
        with self._lock:
            if device_id in self._devices:
                return
            self._devices.add(device_id)
            self.partition_bytes += DEVICE_ROW_BYTES
            self.stored_bytes += DEVICE_ROW_BYTES

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

    def project(self, kind, nbytes=None, direct=False, grow=None):
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
            reads += load_reads(partition, self.sub_bytes)
            if kind == "write":
                writes += MUTATING_WRITE_UNITS
                if grow is None:
                    grow = True
                if grow:
                    growth = stored_growth(size)
                    stored += growth
                    partition += growth
        return _money(self, reads, writes, stored, partition)

    def charge(self, kind, nbytes=None, grow=None):
        with self._lock:
            projected = self.project(kind, nbytes, direct=False, grow=grow)
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
