"""Running cost estimate with a six-dollar hard cap.

Prices are public on-demand list rates, not a bill. A request is an API
Gateway call. A write is one DynamoDB write-request unit per KiB. Storage
is one month of those bytes, so a failed cleanup still counts.
"""

REQUEST_USD = 1.00 / 1_000_000
WRITE_USD = 1.25 / 1_000_000
STORAGE_USD_PER_BYTE = 0.25 / (1024 ** 3)
CAP_USD = 6.0


class CapExceeded(Exception):
    """The next call would reach the hard cap. ``estimate`` is the total."""

    def __init__(self, estimate):
        super().__init__("cost cap")
        self.estimate = estimate


class Ledger:
    def __init__(self, cap_usd=CAP_USD):
        self.cap_usd = cap_usd
        self.requests = 0
        self.write_units = 0
        self.stored_bytes = 0

    def estimate(self):
        request_usd = self.requests * REQUEST_USD
        write_usd = self.write_units * WRITE_USD
        storage_usd = self.stored_bytes * STORAGE_USD_PER_BYTE
        total = request_usd + write_usd + storage_usd
        return {
            "requests": self.requests,
            "write_units": self.write_units,
            "stored_bytes": self.stored_bytes,
            "request_usd": request_usd,
            "write_usd": write_usd,
            "storage_usd": storage_usd,
            "total_usd": total,
            "cap_usd": self.cap_usd,
        }

    def project(self, requests, nbytes):
        units = 0 if nbytes is None else max(1, (nbytes + 1023) // 1024)
        stored = self.stored_bytes if nbytes is None else self.stored_bytes + nbytes
        request_usd = (self.requests + requests) * REQUEST_USD
        write_usd = (self.write_units + units) * WRITE_USD
        storage_usd = stored * STORAGE_USD_PER_BYTE
        return {
            "requests": self.requests + requests,
            "write_units": self.write_units + units,
            "stored_bytes": stored,
            "request_usd": request_usd,
            "write_usd": write_usd,
            "storage_usd": storage_usd,
            "total_usd": request_usd + write_usd + storage_usd,
            "cap_usd": self.cap_usd,
        }

    def charge(self, requests=1, nbytes=None):
        """Count the call, or raise before it is sent once the cap is reached."""
        if nbytes is not None and nbytes < 0:
            raise ValueError("negative write")
        projected = self.project(requests, nbytes)
        if projected["total_usd"] >= self.cap_usd:
            raise CapExceeded(projected)
        self.requests = projected["requests"]
        self.write_units = projected["write_units"]
        self.stored_bytes = projected["stored_bytes"]
        return projected

    def record(self, requests=1, nbytes=None):
        """Count cleanup after the cap has already stopped the load."""
        if nbytes is not None and nbytes < 0:
            raise ValueError("negative write")
        projected = self.project(requests, nbytes)
        self.requests = projected["requests"]
        self.write_units = projected["write_units"]
        self.stored_bytes = projected["stored_bytes"]
        return projected


def format_estimate(estimate):
    return "\n".join([
        "mutation-log load estimate",
        "  requests: {requests}  ${request_usd:.6f}".format(**estimate),
        "  writes: {write_units} WRU  ${write_usd:.6f}".format(**estimate),
        "  storage: {stored_bytes} bytes  ${storage_usd:.6f}".format(**estimate),
        "  total: ${total_usd:.6f}  cap: ${cap_usd:.2f}".format(**estimate),
    ])
