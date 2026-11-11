"""Appends and acks pay for #769's three server attempts. Head and list do not."""

import sys
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from cost import (
    SERVER_ATTEMPTS, CapExceeded, Ledger, MUTATING_WRITE_UNITS, load_reads,
)
from drive import Session, drive
from faults import EXIT_SCALING, exit_code
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
        self.assertEqual(
            estimate["recheck_reserve_reads"],
            8 * load_reads(estimate["partition_bytes"], 0))
        completed = 38
        tripped = None
        for _ in range(80):
            try:
                _round(fresh, noted)
            except CapExceeded:
                tripped = completed + 1
                break
            completed += 1
        self.assertEqual(tripped, 66)

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
        go = threading.Event()
        heads = {"n": 0}

        def hook(path, parsed, _log):
            if path.endswith("/append") and parsed["device_id"].endswith("-d0"):
                return 503, THROTTLED, 1
            if path.endswith("/append"):
                go.wait(5)
                return None
            if path.endswith("/head"):
                heads["n"] += 1
                if heads["n"] <= 8:
                    return 503, QUERY_THROTTLE, 1
            return None

        class CapAfter(Ledger):
            def charge(self, kind, nbytes=None, grow=None, attempts=1, hold_recheck=True):
                if hold_recheck and go.is_set():
                    raise CapExceeded(self.estimate())
                return super().charge(
                    kind, nbytes, grow=grow, attempts=attempts, hold_recheck=hold_recheck)

        # The eighth d0 throttle opens the gate inside the response hook, before
        # the driver counts the write exhausted. Arm it on that response.
        seen = {"d0": 0}

        def armed(path, parsed, log):
            if path.endswith("/append") and parsed["device_id"].endswith("-d0"):
                seen["d0"] += 1
                if seen["d0"] >= 8:
                    go.set()
            return hook(path, parsed, log)

        session = _session(MemoryLog(armed), CapAfter())
        session.sleeper = lambda _delay: None
        report = drive(Scenario("full", 2, None, False), session)
        self.assertEqual(report["stopped"], "scaling")
        self.assertEqual(exit_code(report["stopped"]), EXIT_SCALING)
        self.assertTrue(report["summary"]["logcheck_recheck"])
        self.assertGreater(session.throttle_unresolved, 0)
        self.assertFalse(session.quiet)


QUERY_THROTTLE = {"code": "retry_later", "field": "mutation_log.query.throttled"}
SNAPSHOT_THROTTLE = {"code": "retry_later", "field": "mutation_log.snapshot.throttled"}


def _session(transport, ledger=None):
    return Session(
        database_id_for_run("17"), "ab" * 16, "cd" * 32, transport,
        ledger or Ledger(), lambda: 0)


if __name__ == "__main__":
    unittest.main()
