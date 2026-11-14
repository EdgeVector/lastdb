"""Concurrent load against an injected transport. This module does no I/O."""

import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from cost import LIST_LIMIT, SERVER_ATTEMPTS, CapExceeded
from faults import (
    RetriesExhausted, already_created, backoff_seconds, capacity_finding,
    is_contention, is_duplicate, is_throttle, quiet_backoff_seconds, retry_action,
)
from finish import audit_log, finding_text, request_for, tier_report, verify_after_cap
from plan import APPENDS
from routes import READS, RouteError, check_success, path_for


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


def _prefer_failure(failures):
    """A real failure outranks an exhausted retry, which outranks the cap."""
    ranked = []
    for exc in failures:
        if isinstance(exc, CapExceeded):
            ranked.append((2, exc))
        elif isinstance(exc, RetriesExhausted):
            ranked.append((1, exc))
        else:
            ranked.append((0, exc))
    if not ranked:
        return None
    ranked.sort(key=lambda item: item[0])
    return ranked[0][1]


def run_writers(count, worker, flight=None):
    """Run ``count`` workers at once. Exit priority stays 1, then 3, then 2."""
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
    chosen = _prefer_failure(failures)
    if chosen is not None:
        raise chosen


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
        self.throttle_unresolved = 0
        self.unresolved = []
        self.no_partial_writes = True
        self.audited = False
        self.audit_cursor = 0
        self.in_logcheck = False
        self.log_read_throttled = False
        self.logcheck_recheck = False
        self.quiet = False
        self.accepted = {}
        self._lock = threading.Lock()
        self.stop = threading.Event()
        self.capped = threading.Event()
        self.tier_reads = 0
        self.device_prefix = "d"
        self.created = False

    def begin_tier(self, spec):
        self.device_prefix = "{0}{1}".format(spec.kind, spec.writers)
        self.tier_reads = self.ledger.snapshot()["reads"]
        self.samples = []
        self.errors = 0
        self.calls = 0
        self.contention_503 = 0
        self.throttle_503 = 0
        self.other_503 = 0
        self.write_attempts = 0
        self.contention_unresolved = 0
        self.throttle_unresolved = 0
        self.unresolved = []
        self.no_partial_writes = True
        self.audited = False
        self.in_logcheck = False
        self.log_read_throttled = False
        self.logcheck_recheck = False
        self.quiet = False

    def call(self, operation, required=False, **fields):
        if operation == "append":
            fields["authored_at_ms"] = self.clock()
        body = self._body(operation, fields)
        tries = 0
        while True:
            if self.stop.is_set() and not self.in_logcheck:
                if self.capped.is_set():
                    raise CapExceeded(self.ledger.snapshot())
                raise LoadError("stopped")
            self._charge(operation, body)
            status, payload, latency = self.transport("POST", path_for(operation), body)
            self._note_try(operation, latency)
            if self._retry(status, payload, tries, operation, fields):
                tries += 1
                self._pause(tries)
                continue
            self._finish(operation, status, payload, required, fields)
            if status < 400 or already_created(operation, status, payload):
                return payload
            return None

    def _charge(self, operation, body):
        kind = "read" if operation in READS else "write"
        try:
            attempts = SERVER_ATTEMPTS if operation == "append" else 1
            self.ledger.charge(
                kind, None if kind == "read" else len(body),
                grow=operation == "append", attempts=attempts,
                hold_recheck=not self.in_logcheck)
        except CapExceeded:
            self.capped.set()
            self.stop.set()
            raise

    def _note_try(self, operation, latency):
        with self._lock:
            self.samples.append(latency)
            self.calls += 1
            if operation not in READS:
                self.write_attempts += 1

    def _retry(self, status, payload, tries, operation, fields):
        throttle = is_throttle(status, payload)
        contention = is_contention(status, payload)
        action = retry_action(throttle, contention, tries)
        if action == "done":
            return False
        with self._lock:
            if throttle:
                self.throttle_503 += 1
            else:
                self.contention_503 += 1
            if action == "unresolved":
                self._remember_unresolved(operation, fields)
            elif throttle and action != "retry":
                self.throttle_unresolved += 1
                if operation in READS and self.in_logcheck:
                    self.log_read_throttled = True
        if action == "retry":
            return True
        if action == "unresolved" or throttle:
            raise RetriesExhausted()
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
            schedule = quiet_backoff_seconds if self.quiet else backoff_seconds
            delay = schedule(tries, self.rng.randrange)
        self.sleeper(delay)

    def _finish(self, operation, status, payload, required, fields):
        if already_created(operation, status, payload):
            return
        if status == 503:
            with self._lock:
                self.other_503 += 1
            self._fail()
            raise LoadError("http")
        self._observe(status, payload, required)
        if status >= 400:
            return
        if operation == "append" and is_duplicate(payload):
            self._fail()
            raise LoadError("duplicate")
        try:
            check_success(operation, payload, fields.get("seq"))
        except RouteError:
            self.stop.set()
            raise
        if operation == "append":
            self._note_append(fields)

    def _observe(self, status, payload, required):
        disabled = status == 404 and _disabled(payload)
        failed = status >= 500 or status <= 0
        ambiguous = not isinstance(payload, dict)
        client_error = status >= 400
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
        return request_for(self, operation, fields, LIST_LIMIT)


def drive(spec, session):
    """Create the database, run the writers, and check the complete log."""
    started = session.clock()
    session.begin_tier(spec)
    try:
        _run_tier(spec, session, started)
    except CapExceeded:
        return tier_report(session, session.clock() - started, _over_cap(session, spec), "")
    except RetriesExhausted:
        return tier_report(session, session.clock() - started, "error", finding_text(session, spec))
    except (LoadError, RouteError) as error:
        return tier_report(session, session.clock() - started, "error", str(error))
    return tier_report(
        session, session.clock() - started, _finished(session, spec), finding_text(session, spec))


def _run_tier(spec, session, started):
    if not session.created:
        if session.call("create-database", required=True) is None:
            raise LoadError("http")
        session.created = True
    if spec.kind == "steady":
        _steady(spec, session, started)
    else:
        run_writers(spec.writers, lambda index: _append_ten(session, index))
    audit_log(session)


def _exhausted(session):
    return session.contention_unresolved or session.throttle_unresolved


def _over_cap(session, spec):
    """The cap loses to a correctness failure and to a scaling finding."""
    if _exhausted(session) and not session.audited:
        return verify_after_cap(session)
    if _exhausted(session) and not session.no_partial_writes:
        return "error"
    if _exhausted(session) or _capacity(session, spec):
        return "scaling"
    return "cap"


def _finished(session, spec):
    if _exhausted(session) and not session.no_partial_writes:
        return "error"
    if _exhausted(session) or _capacity(session, spec):
        return "scaling"
    return "complete"


def _capacity(session, spec):
    return capacity_finding(
        session.throttle_503, spec.kind, session.contention_503, session.write_attempts)


def _append_ten(session, index):
    try:
        for seq in range(1, APPENDS + 1):
            if session.call("append", index=index, seq=seq) is None:
                raise LoadError("http")
    except RetriesExhausted:
        return


def _steady(spec, session, started):
    def worker(index):
        seq = 1
        try:
            while session.clock() - started < spec.steady_ms and not session.stop.is_set():
                if session.call("append", index=index, seq=seq) is None:
                    raise LoadError("http")
                seq += 1
        except RetriesExhausted:
            return

    run_writers(spec.writers, worker)


def _device(prefix, index):
    return "{0}-d{1}".format(prefix, index)


def _disabled(payload):
    if not isinstance(payload, dict):
        return False
    return payload.get("code") == DISABLED_FIELD or payload.get("field") == DISABLED_FIELD
