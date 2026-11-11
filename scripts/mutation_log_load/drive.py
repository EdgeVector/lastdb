"""Concurrent load against an injected transport. This module does no I/O."""

import threading
from concurrent.futures import ThreadPoolExecutor

from cost import CapExceeded
from plan import still_running, writer_calls
from routes import (
    READS, RouteError, check_success, path_for, prove_trimmed_list, request_body,
    require_head, require_snapshot, require_trim,
)
from stats import summarize


ERROR_RATE = 0.01
DISABLED_FIELD = "mutation_log.disabled"
DISABLED = "mutation log is disabled"


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

    def call(self, operation, required=False, accept=(), **fields):
        status, payload = self.exchange(operation, required=required, accept=accept, **fields)
        if status >= 400 and status not in accept:
            return None
        return payload

    def exchange(self, operation, required=False, accept=(), **fields):
        if self.stop.is_set():
            raise LoadError("stopped")
        body = self._body(operation, fields)
        kind = "read" if operation in READS else "write"
        try:
            self.ledger.charge(kind, None if kind == "read" else len(body))
        except CapExceeded:
            self.stop.set()
            raise
        status, payload, latency = self.transport("POST", path_for(operation), body)
        self._observe(status, payload, latency, required, accept)
        if status < 400:
            try:
                check_success(operation, payload)
            except RouteError:
                self.stop.set()
                raise
        return status, payload

    def _observe(self, status, payload, latency, required, accept):
        disabled = status == 404 and _disabled(payload)
        failed = status >= 500 or status <= 0
        ambiguous = not isinstance(payload, dict)
        client_error = status >= 400 and status not in accept
        with self._lock:
            self.samples.append(latency)
            self.calls += 1
            if client_error or failed or disabled or ambiguous:
                self.errors += 1
            rate = self.errors / self.calls > ERROR_RATE
        if disabled:
            self.stop.set()
            raise LoadError(DISABLED)
        if failed:
            self.stop.set()
            raise LoadError("http")
        if ambiguous:
            self.stop.set()
            raise LoadError("ambiguous")
        if client_error and (required or rate):
            self.stop.set()
            raise LoadError("http" if required else "error-rate")

    def _body(self, operation, fields):
        if operation in ("head", "snapshot", "trim"):
            return request_body(operation, {})
        database = self.database_id
        if operation == "create-database":
            return request_body(operation, {"database_id": database, "kind": "personal"})
        device = "{0}-d{1}".format(self.device_prefix, fields["index"])
        if operation == "append":
            return request_body(operation, {
                "database_id": database,
                "device_id": device,
                "entries": [{
                    "authored_at_ms": self.clock(),
                    "namespace": "load",
                    "payload": "load",
                    "writer_seq": fields["seq"],
                }],
            })
        if operation == "list":
            return request_body(operation, {
                "after": fields.get("after", 0),
                "database_id": database,
                "device_id": device,
                "limit": 20,
            })
        if operation == "ack":
            return request_body(operation, {
                "database_id": database,
                "device_id": device,
                "through": fields["seq"],
            })
        raise RouteError("route")


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
        return _report(session, session.clock() - started, "cap", "")
    except (LoadError, RouteError) as error:
        return _report(session, session.clock() - started, "error", str(error))
    return _report(session, session.clock() - started, "complete", "")


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
    if require_head(head) is None:
        raise LoadError("ambiguous")
    require_snapshot(session.call("snapshot", required=True))
    through = require_trim(session.call("trim", required=True))
    status, listed = session.exchange(
        "list", required=True, accept=(409,), index=0, after=0)
    prove_trimmed_list(through, status, listed)


def _report(session, elapsed_ms, stopped, message):
    summary = summarize(session.samples, session.errors, elapsed_ms)
    report = {"stopped": stopped, "summary": summary, "estimate": session.ledger.snapshot()}
    if message:
        report["message"] = message
    return report


def _disabled(payload):
    if not isinstance(payload, dict):
        return False
    return payload.get("code") == DISABLED_FIELD or payload.get("field") == DISABLED_FIELD
