"""Appends and acks pay for #769's three server attempts. Head and list do not."""

import json
import sys
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from cost import (
    ENTRY_ROW_BYTES, LIST_LIMIT, ROW_OVERHEAD_BYTES, SERVER_ATTEMPTS,
    CapExceeded, Ledger, MUTATING_WRITE_UNITS, load_reads,
)
from drive import Session, drive
from faults import EXIT_CORRECTNESS, EXIT_SCALING, MAX_CONTENTION_TRIES, exit_code
from guard import database_id_for_run
from plan import Scenario, scenario
from test_cost import THROTTLED, MemoryLog


def _round(ledger, noted):
    for index in range(100):
        if not noted[index]:
            ledger.note_device("w{0}".format(index))
            noted[index] = True
        ledger.charge("write", 80, grow=True, attempts=SERVER_ATTEMPTS)
        ledger.charge("read")
        ledger.charge("read")
        ledger.charge("write", 80, grow=False, attempts=SERVER_ATTEMPTS)


class ServerAttempts(unittest.TestCase):
    def test_appends_and_acks_pay_for_three_server_attempts(self):
        session = Session(
            database_id_for_run("17"), "ab" * 16, "cd" * 32, MemoryLog(), Ledger(), lambda: 0)
        session.call("append", index=0, seq=1)
        self.assertGreater(session.ledger.partition_bytes, 800)
        self.assertEqual(session.ledger.reads, load_reads(800, 0) * SERVER_ATTEMPTS)
        self.assertEqual(session.ledger.write_units, MUTATING_WRITE_UNITS * SERVER_ATTEMPTS)
        before = session.ledger.reads
        session.call("head", index=0)
        self.assertEqual(
            session.ledger.reads - before, load_reads(session.ledger.partition_bytes, 0))
        held = session.ledger.partition_bytes
        writes = session.ledger.write_units
        before = session.ledger.reads
        session.call("ack", index=0, seq=1)
        self.assertEqual(session.ledger.partition_bytes, held)
        self.assertEqual(session.ledger.reads - before, load_reads(held, 0) * SERVER_ATTEMPTS)
        self.assertEqual(
            session.ledger.write_units - writes, MUTATING_WRITE_UNITS * SERVER_ATTEMPTS)
        fresh = Ledger()
        noted = [False] * 100
        fresh.charge("write", 40, grow=False)
        for _ in range(38):
            _round(fresh, noted)
        estimate = fresh.estimate()
        self.assertLess(estimate["total_usd"] + estimate["recheck_reserve_usd"], 6)
        self.assertEqual(estimate["recheck_reserve_reads"], _reserve_reads(estimate["partition_bytes"]))
        completed = 38
        tripped = None
        for _ in range(80):
            try:
                _round(fresh, noted)
            except CapExceeded:
                tripped = completed + 1
                break
            completed += 1
        self.assertEqual(tripped, 65)
        self.assertEqual(
            fresh.estimate()["recheck_reserve_reads"],
            _reserve_reads(fresh.partition_bytes))

    def test_snapshot_throttle_rereads_with_the_quiet_recheck(self):
        state = {"phase": "", "heads": 0}

        def hook(path, _parsed, _log):
            if path.endswith("/snapshot"):
                state["phase"] = "reread"
                return 503, SNAPSHOT_THROTTLE, 1
            if path.endswith("/head") and state["phase"] == "reread":
                state["heads"] += 1
                if state["heads"] <= 8:
                    return 503, QUERY_THROTTLE, 1
            return None

        session = _session(MemoryLog(hook))
        session.sleeper = lambda _delay: None
        report = drive(scenario("trim"), session)
        self.assertEqual(report["stopped"], "scaling")
        self.assertEqual(exit_code(report["stopped"]), EXIT_SCALING)
        self.assertTrue(report["summary"]["logcheck_recheck"])
        self.assertFalse(session.quiet)

    def test_an_exhausted_write_then_the_cap_runs_the_quiet_logcheck(self):
        report, session, log = _drive_capped(lose=False)
        spent = session.ledger.snapshot()["total_usd"]
        self.assertEqual(report["stopped"], "scaling")
        self.assertEqual(exit_code(report["stopped"]), EXIT_SCALING)
        self.assertTrue(session.audited)
        self.assertTrue(session.ack_holds)
        self.assertGreater(session.throttle_unresolved, 0)
        self.assertLess(spent, 0.25)
        self.assertTrue(any(path.endswith("/list") for path, _body in log.seen))

    def test_a_cap_logcheck_that_loses_entries_is_exit_1(self):
        report, session, log = _drive_capped(lose=True)
        spent = session.ledger.snapshot()["total_usd"]
        self.assertEqual(report["stopped"], "error")
        self.assertEqual(exit_code(report["stopped"]), EXIT_CORRECTNESS)
        self.assertFalse(session.audited)
        self.assertLess(spent, 0.25)
        self.assertTrue(any(path.endswith("/list") for path, _body in log.seen))


QUERY_THROTTLE = {"code": "retry_later", "field": "mutation_log.query.throttled"}
SNAPSHOT_THROTTLE = {"code": "retry_later", "field": "mutation_log.snapshot.throttled"}


def _reserve_reads(partition_bytes):
    """The page-count reserve, written out so it is not only the helper."""
    per_entry = ENTRY_ROW_BYTES + ROW_OVERHEAD_BYTES
    pages = -(-max(partition_bytes, 0) // (per_entry * LIST_LIMIT))
    loads = 2 * MAX_CONTENTION_TRIES * (1 + pages + 1)
    return loads * load_reads(partition_bytes, 0)


class _Gated(MemoryLog):
    """The other writer waits until the throttle has exhausted, without the log lock."""

    def __init__(self, ready, hook):
        super().__init__(hook)
        self.ready = ready

    def __call__(self, method, path, body):
        parsed = json.loads(body) if body else {}
        device = parsed.get("device_id") or ""
        if path.endswith("/append") and not str(device).endswith("-d0"):
            self.ready.wait(10)
        return super().__call__(method, path, body)


def _drive_capped(lose):
    """Real $0.25 ledger. One writer exhausts, then the cap trips, then logcheck."""
    ready = threading.Event()
    seen = {"d0": 0, "lost": False}

    def hook(path, parsed, log):
        device = str(parsed.get("device_id") or "")
        if path.endswith("/append") and device.endswith("-d0"):
            seen["d0"] += 1
            if seen["d0"] >= 8:
                ready.set()
            return 503, THROTTLED, 1
        if lose and path.endswith("/list") and not seen["lost"]:
            seen["lost"] = True
            log.entries.append({
                "delivery_seq": len(log.entries) + 1,
                "writer_device": "other",
                "writer_seq": 1,
            })
        return None

    log = _Gated(ready, hook)
    session = _session(log, Ledger(cap_usd=0.25))
    session.sleeper = lambda _delay: None
    report = drive(Scenario("steady", 2, None, False), session)
    return report, session, log


def _session(transport, ledger=None):
    return Session(
        database_id_for_run("17"), "ab" * 16, "cd" * 32, transport,
        ledger or Ledger(), lambda: 0)


if __name__ == "__main__":
    unittest.main()
