"""Concurrent load against an injected transport. This module does no I/O."""

import threading
from concurrent.futures import ThreadPoolExecutor

from cost import CapExceeded
from plan import still_running, writer_calls
from routes import READS, RouteError, path_for, request_body, require_entries_above, require_trim
from stats import summarize


ERROR_RATE = 0.01


class LoadError(Exception):
    """A failed or ambiguous response. The run stops."""


class Flight:
    def __init__(self):
        self._lock = threading.Lock()
        self.current = 0
        self.max = 0

    def enter(self):
        with self._lock:
            self.current += 1
            self.max = max(self.max, self.current)

    def leave(self):
        with self._lock:
            self.current -= 1


def run_writers(count, worker, flight=None):
    """Run ``count`` workers at once. ``flight.max`` is the peak overlap."""
    def wrapped(index):
        if flight is not None:
            flight.enter()
        try:
            return worker(index)
        finally:
            if flight is not None:
                flight.leave()

    with ThreadPoolExecutor(max_workers=count) as pool:
        return list(pool.map(wrapped, range(count)))


class Session:
    def __init__(self, database_id, account_id, writer_key, transport, ledger, clock):
        self.database_id = database_id
        self.account_id = account_id
        self.writer_key = writer_key
        self.transport = transport
        self.ledger = ledger
        self.clock = clock
        self.samples = []
        self.errors = 0
        self.calls = 0
        self._lock = threading.Lock()
        self.stop = threading.Event()
        self.device_prefix = "d"
        self.created = False

    def call(self, operation, required=False, **fields):
        if self.stop.is_set():
            raise LoadError("stopped")
        body = self._body(operation, fields)
        kind = "read" if operation in READS else "write"
        try:
            self.ledger.charge(kind, None if kind == "read" else len(body))
        except CapExceeded:
            self.stop.set()
            raise
        status, payload, latency = self.transport(
            "POST", path_for(operation), body)
        self._count(status, latency, required)
        if status >= 400:
            return None
        if not isinstance(payload, dict):
            self.stop.set()
            raise LoadError("ambiguous")
        return payload

    def _count(self, status, latency, required):
        with self._lock:
            self.samples.append(latency)
            self.calls += 1
            client_error = status >= 400
            if client_error:
                self.errors += 1
            failed = status >= 500 or status <= 0
            rate = self.calls and self.errors / self.calls > ERROR_RATE
            if failed or rate or (client_error and required):
                self.stop.set()
                raise LoadError("http" if failed or required else "error-rate")

    def _body(self, operation, fields):
        common = {
            "version": 2,
            "account_id": self.account_id,
            "database_id": self.database_id,
            "stream_id": self.database_id,
        }
        if operation == "append":
            common.update({
                "writer_key": self.writer_key,
                "device": "{0}-d{1}".format(self.device_prefix, fields["index"]),
                "writer_seq": str(fields["seq"]),
                "authored_at_ms": str(self.clock()),
                "namespace": "load",
                "payload": "load",
            })
        elif operation == "list":
            common.update({"after": "0", "limit": "20"})
        elif operation == "ack":
            common.update({
                "writer_key": self.writer_key,
                "device": "{0}-d{1}".format(self.device_prefix, fields["index"]),
                "through": str(fields["seq"]),
            })
        elif operation == "snapshot":
            common.update({
                "at_ms": str(self.clock()),
                "snapshot_ref": "load-" + self.database_id,
            })
        elif operation == "trim":
            common.update({"now_ms": str(self.clock())})
        elif operation not in ("create-database", "head"):
            raise RouteError("route")
        return request_body(operation, common)


def drive(spec, session):
    """Create the database, run the writers, and trim when this level trims."""
    started = session.clock()
    session.device_prefix = "{0}{1}".format(spec.kind, spec.writers)
    try:
        if not session.created:
            if session.call("create-database", required=True) is None:
                raise LoadError("http")
            session.created = True
        if spec.kind == "steady":
            _steady(spec, session, started)
        else:
            run_writers(spec.writers, lambda index: _script(spec, session, index))
        if spec.trim:
            _trim(session)
    except CapExceeded:
        return _report(session, session.clock() - started, "cap")
    except (LoadError, RouteError):
        return _report(session, session.clock() - started, "error")
    return _report(session, session.clock() - started, "complete")


def _script(spec, session, index):
    for operation, writer, seq in writer_calls(spec, index):
        if session.call(operation, index=writer, seq=seq) is None:
            return


def _steady(spec, session, started):
    def worker(index):
        seq = 1
        while still_running(spec, session.clock() - started) and not session.stop.is_set():
            for operation, writer, _seq in writer_calls(spec, index):
                if not still_running(spec, session.clock() - started):
                    return
                use = seq if operation != "list" else 0
                if session.call(operation, index=writer, seq=use) is None:
                    return
            seq += 1

    run_writers(spec.writers, worker)


def _trim(session):
    head = session.call("head", required=True)
    if head is None or "trimmed_through" not in head:
        raise LoadError("ambiguous")
    session.call("snapshot", required=True)
    trimmed = session.call("trim", required=True)
    through, _dropped = require_trim(head["trimmed_through"], trimmed)
    listed = session.call("list", required=True)
    require_entries_above(through, listed)


def _report(session, elapsed_ms, stopped):
    summary = summarize(session.samples, session.errors, elapsed_ms)
    return {"stopped": stopped, "summary": summary, "estimate": session.ledger.snapshot()}
