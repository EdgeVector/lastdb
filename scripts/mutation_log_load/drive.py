"""Concurrent load against an injected transport. This module does no I/O."""

import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from cost import CapExceeded
from faults import (
    UnresolvedContention, already_created, backoff_seconds, capacity_finding,
    is_contention, is_duplicate, is_throttle, retry_action,
)
from finish import check_log, read_log, trim_log
from plan import still_running, writer_calls
from routes import (
    PAYLOAD, READS, RouteError, check_success, path_for, request_body,
    require_ack, require_head,
)
from stats import finding_message, tier_summary


ERROR_RATE = 0.01
DISABLED_FIELD = "mutation_log.disabled"
DISABLED = "mutation log is disabled"
LIST_LIMIT = 90


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
    """Run ``count`` workers at once. A budget stop wins over ``stopped``."""
    failures = []

    def wrapped(index):
        if flight is not None:
            flight.enter()
        try:
            return worker(index)
        except Exception as exc:
            failures.append(exc)
            return None
        finally:
            if flight is not None:
                flight.leave()

    with ThreadPoolExecutor(max_workers=count) as pool:
        list(pool.map(wrapped, range(count)))
    for exc in failures:
        if isinstance(exc, CapExceeded):
            raise exc
    if failures:
        raise failures[0]


class Session:
    def __init__(self, database_id, account_id, writer_key, transport, ledger, clock,
                 sleeper=None, rng=None):
        self.database_id = database_id
        self.account_id = account_id
        self.writer_key = writer_key
        self.transport = transport
        self.ledger = ledger
        self.clock = clock
        self.sleeper = sleeper or time.sleep
        self.rng = rng or random.Random()
        self.samples = []
        self.errors = 0
        self.calls = 0
        self.contention_503 = 0
        self.throttle_503 = 0
        self.other_503 = 0
        self.write_attempts = 0
        self.contention_unresolved = 0
        self.unresolved = []
        self.acked_through = 0
        self.ack_holds = True
        self.audit_cursor = 0
        self.accepted = {}
        self._lock = threading.Lock()
        self.stop = threading.Event()
        self.capped = threading.Event()
        self.tier_reads = 0
        self.device_prefix = "d"
        self.allow_contention = False
        self.created = False

    def begin_tier(self, spec):
        self.device_prefix = "{0}{1}".format(spec.kind, spec.writers)
        self.allow_contention = spec.kind in ("full", "steady", "trim")
        self.tier_reads = self.ledger.snapshot()["reads"]
        self.samples = []
        self.errors = 0
        self.calls = 0
        self.contention_503 = 0
        self.throttle_503 = 0
        self.other_503 = 0
        self.write_attempts = 0
        self.contention_unresolved = 0
        self.unresolved = []
        self.ack_holds = True

    def call(self, operation, required=False, accept=(), **fields):
        status, payload = self.exchange(operation, required=required, accept=accept, **fields)
        if already_created(operation, status, payload):
            return payload
        if status >= 400 and status not in accept:
            return None
        return payload

    def exchange(self, operation, required=False, accept=(), **fields):
        tries = 0
        while True:
            if self.stop.is_set():
                if self.capped.is_set():
                    raise CapExceeded(self.ledger.snapshot())
                raise LoadError("stopped")
            body = self._body(operation, fields)
            self._charge(operation, body)
            status, payload, latency = self.transport("POST", path_for(operation), body)
            self._note_try(operation, latency)
            if self._retry(status, payload, tries, operation, fields):
                tries += 1
                self._pause(tries)
                continue
            self._finish(operation, status, payload, required, accept, fields)
            return status, payload

    def _charge(self, operation, body):
        kind = "read" if operation in READS else "write"
        try:
            self.ledger.charge(kind, None if kind == "read" else len(body))
        except CapExceeded:
            self.capped.set()
            self.stop.set()
            raise

    def _note_try(self, operation, latency):
        with self._lock:
            self.samples.append(latency)
            self.calls += 1
            # Appends and acks are both write attempts in the contention rate.
            if operation not in READS:
                self.write_attempts += 1

    def _retry(self, status, payload, tries, operation, fields):
        throttle = is_throttle(status, payload)
        contention = is_contention(status, payload)
        action = retry_action(throttle, contention, tries, self.allow_contention)
        if action == "done":
            return False
        with self._lock:
            if throttle:
                self.throttle_503 += 1
            else:
                self.contention_503 += 1
            if action == "unresolved":
                self._remember_unresolved(operation, fields)
        if action == "retry":
            return True
        if action == "unresolved":
            raise UnresolvedContention()
        self._fail()
        raise LoadError("http")

    def _remember_unresolved(self, operation, fields):
        self.contention_unresolved += 1
        if operation != "append" or "seq" not in fields:
            return
        device = fields.get("device_id") or _device(self.device_prefix, fields["index"])
        self.unresolved.append((device, fields["seq"]))

    def _pause(self, tries):
        with self._lock:
            delay = backoff_seconds(tries, self.rng.randrange)
        self.sleeper(delay)

    def _finish(self, operation, status, payload, required, accept, fields):
        if already_created(operation, status, payload):
            return
        if status == 503:
            with self._lock:
                self.other_503 += 1
            self._fail()
            raise LoadError("http")
        self._observe(status, payload, required, accept)
        if status >= 400:
            return
        if is_duplicate(payload):
            self._fail()
            raise LoadError("duplicate")
        try:
            check_success(operation, payload)
        except RouteError:
            self.stop.set()
            raise
        if operation == "append":
            self._note_append(fields)

    def _observe(self, status, payload, required, accept):
        disabled = status == 404 and _disabled(payload)
        failed = status >= 500 or status <= 0
        ambiguous = not isinstance(payload, dict)
        client_error = status >= 400 and status not in accept
        with self._lock:
            if client_error or failed or disabled or ambiguous:
                self.errors += 1
            rate = self.calls and self.errors / self.calls > ERROR_RATE
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

    def _fail(self):
        with self._lock:
            self.errors += 1
        self.stop.set()

    def _note_append(self, fields):
        device = fields.get("device_id") or _device(self.device_prefix, fields["index"])
        with self._lock:
            self.accepted.setdefault(device, set()).add(fields["seq"])

    def _body(self, operation, fields):
        database = self.database_id
        if "index" in fields or "device_id" in fields:
            device = fields.get("device_id") or _device(self.device_prefix, fields["index"])
            self.ledger.note_device(device)
        if operation in ("head", "snapshot", "trim", "create-database"):
            body = {"database_id": database}
            if operation == "create-database":
                body["kind"] = "personal"
            return request_body(operation, body)
        device = fields.get("device_id") or _device(self.device_prefix, fields["index"])
        if operation == "append":
            return request_body(operation, {
                "database_id": database,
                "device_id": device,
                "entries": [{
                    "authored_at_ms": self.clock(),
                    "namespace": "load",
                    "payload": PAYLOAD,
                    "writer_seq": fields["seq"],
                }],
            })
        if operation == "list":
            return request_body(operation, {
                "after": fields.get("after", 0),
                "database_id": database,
                "device_id": device,
                "limit": LIST_LIMIT,
            })
        if operation == "ack":
            return request_body(operation, {
                "database_id": database,
                "device_id": device,
                "through": fields["seq"],
            })
        raise RouteError("route")


def drive(spec, session):
    """Create the database, run the writers, check the log, and trim when asked."""
    started = session.clock()
    session.begin_tier(spec)
    try:
        if not session.created:
            if session.call("create-database", required=True) is None:
                raise LoadError("http")
            session.created = True
        if spec.kind == "steady":
            _steady(spec, session, started)
        else:
            run_writers(spec.writers, lambda index: _script(spec, session, index))
        check_log(session)
        if spec.trim and not session.contention_unresolved:
            trim_log(session)
    except CapExceeded:
        return _report(session, session.clock() - started, "cap", "")
    except (LoadError, RouteError) as error:
        return _report(session, session.clock() - started, "error", str(error))
    message = _finding(session, spec)
    if session.contention_unresolved and not session.ack_holds:
        return _report(session, session.clock() - started, "error", message)
    if session.contention_unresolved or capacity_finding(
            session.throttle_503, spec.kind, session.contention_503, session.write_attempts):
        return _report(session, session.clock() - started, "scaling", message)
    return _report(session, session.clock() - started, "complete", "")


def _finding(session, spec):
    return finding_message(
        session.throttle_503, spec.kind, session.contention_503,
        session.write_attempts, session.contention_unresolved)


def _script(spec, session, index):
    try:
        for operation, writer, seq in writer_calls(spec, index):
            if session.call(operation, index=writer, seq=seq) is None:
                raise LoadError("http")
        if spec.kind == "full":
            _ack_cursor(session, index)
    except UnresolvedContention:
        return


def _steady(spec, session, started):
    def worker(index):
        seq = 1
        try:
            while still_running(spec, session.clock() - started) and not session.stop.is_set():
                if session.call("append", index=index, seq=seq) is None:
                    raise LoadError("http")
                _ack_cursor(session, index)
                seq += 1
        except UnresolvedContention:
            return

    run_writers(spec.writers, worker)


def _ack_cursor(session, index):
    """Ack the head's delivery cursor. ``through`` is never a writer sequence."""
    if session.call("list", required=True, index=index, after=0) is None:
        raise LoadError("ack")
    head = session.call("head", required=True, index=index)
    if head is None:
        raise LoadError("ack")
    cursor = require_head(head)
    if cursor is None or cursor < 1:
        raise LoadError("ack")
    acked = session.call("ack", required=True, index=index, seq=cursor)
    through = None if acked is None else require_ack(acked)
    if through is None or through < cursor:
        raise LoadError("ack")
    with session._lock:
        if through > session.acked_through:
            session.acked_through = through


def _pages(session):
    return read_log(session)


def _report(session, elapsed_ms, stopped, message):
    estimate = session.ledger.snapshot()
    counters = {
        "contention_503": session.contention_503,
        "throttle_503": session.throttle_503,
        "other_503": session.other_503,
        "write_attempts": session.write_attempts,
        "contention_unresolved": session.contention_unresolved,
    }
    summary = tier_summary(
        session.samples, session.errors, elapsed_ms, counters, estimate, session.tier_reads)
    report = {"stopped": stopped, "summary": summary, "estimate": estimate}
    if message:
        report["message"] = message
    return report


def _device(prefix, index):
    return "{0}-d{1}".format(prefix, index)


def _disabled(payload):
    if not isinstance(payload, dict):
        return False
    return payload.get("code") == DISABLED_FIELD or payload.get("field") == DISABLED_FIELD
