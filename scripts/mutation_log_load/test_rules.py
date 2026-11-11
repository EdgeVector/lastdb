"""Exit rules for a retried conflict and a query throttle."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from cost import Ledger
from drive import Session, drive
from faults import EXIT_CORRECTNESS, EXIT_OK, EXIT_SCALING, exit_code
from guard import database_id_for_run
from logcheck import ack_holds
from plan import Scenario, scenario
from test_cost import CONFLICT, CapAndRun, MemoryLog


QUERY_THROTTLE = {"code": "retry_later", "field": "mutation_log.query.throttled"}
PLAIN_QUERY = {"code": "retry_later", "field": "mutation_log.query"}


class AmendedExits(unittest.TestCase):
    def test_a_retried_conflict_at_five_and_twenty_is_scaling(self):
        for level in ("5", "20"):
            state = {"left": 1}

            def hook(path, _parsed, _log, state=state):
                if path.endswith("/append") and state["left"]:
                    state["left"] -= 1
                    return 503, CONFLICT, 3
                return None

            delays = []
            session = self._session(MemoryLog(hook))
            session.sleeper = delays.append
            report = drive(scenario(level), session)
            self.assertEqual(report["stopped"], "scaling", level)
            self.assertEqual(exit_code(report["stopped"]), EXIT_SCALING, level)
            self.assertGreater(delays[0], 0, level)
            self.assertEqual(session.contention_unresolved, 0, level)
            self.assertIn("contention retried", report["message"])
            self.assertGreater(report["summary"]["partition_bytes"], 0, level)
            self.assertGreater(report["summary"]["reads"], 0, level)

    def test_a_retried_conflict_at_or_under_one_percent_passes(self):
        state = {"left": 1}

        def hook(path, _parsed, _log):
            if path.endswith("/append") and state["left"]:
                state["left"] -= 1
                return 503, CONFLICT, 2
            return None

        session = self._session(MemoryLog(hook), CapAndRun()._clock(400))
        session.sleeper = lambda _delay: None
        report = drive(Scenario("steady", 1, 1, False), session)
        self.assertEqual(report["stopped"], "complete")
        self.assertEqual(exit_code(report["stopped"]), EXIT_OK)
        self.assertLessEqual(report["summary"]["contention_rate"], 0.01)
        self.assertEqual(session.contention_unresolved, 0)
        self.assertNotIn("message", report)

    def test_a_partial_conflict_is_a_correctness_failure(self):
        def hook(path, _parsed, _log):
            if path.endswith("/append"):
                return 503, CONFLICT, 1
            return None

        log = MemoryLog(hook)
        log.entries.append({
            "delivery_seq": 1, "writer_device": "full1-d0", "writer_seq": 1,
            "accepted_at_ms": 1, "writer_key": "k", "authored_at_ms": 1,
            "namespace": "load", "payload": "bG9hZA",
        })
        session = self._session(log)
        session.sleeper = lambda _delay: None
        report = drive(Scenario("full", 1, None, False), session)
        self.assertEqual(report["stopped"], "error")
        self.assertEqual(exit_code(report["stopped"]), EXIT_CORRECTNESS)
        self.assertEqual(report["summary"]["contention_unresolved"], 1)

    def test_a_lost_acknowledgement_is_a_correctness_failure(self):
        state = {"appends": 0, "hide": False}

        def hook(path, _parsed, _log):
            if path.endswith("/append"):
                state["appends"] += 1
                if state["appends"] > 1:
                    return 503, CONFLICT, 1
            elif path.endswith("/ack"):
                state["hide"] = True
            elif path.endswith("/list") and state["hide"]:
                return 200, {"entries": [], "next": 1}, 1
            return None

        session = self._session(MemoryLog(hook), CapAndRun()._clock(40))
        session.sleeper = lambda _delay: None
        report = drive(Scenario("steady", 1, 1, False), session)
        self.assertEqual(report["stopped"], "error")
        self.assertGreater(report["summary"]["contention_unresolved"], 0)

    def test_query_throttle_is_scaling_and_plain_query_is_not(self):
        state = {"left": 1}

        def hook(path, _parsed, _log):
            if path.endswith("/list") and state["left"]:
                state["left"] -= 1
                return 503, QUERY_THROTTLE, 2
            return None

        session = self._session(MemoryLog(hook))
        session.sleeper = lambda _delay: None
        report = drive(scenario("5"), session)
        self.assertEqual(report["stopped"], "scaling")
        self.assertGreater(session.throttle_503, 0)

        def plain(path, _parsed, _log):
            if path.endswith("/head"):
                return 503, PLAIN_QUERY, 1
            return None

        session = self._session(MemoryLog(plain))
        session.sleeper = lambda _delay: None
        report = drive(Scenario("full", 1, None, False), session)
        self.assertEqual(report["stopped"], "error")
        self.assertEqual(session.throttle_503, 0)
        self.assertGreater(session.other_503, 0)

    def test_trim_retries_a_conflict_and_exits_3(self):
        state = {"left": 1}

        def hook(path, _parsed, _log):
            if path.endswith("/append") and state["left"]:
                state["left"] -= 1
                return 503, CONFLICT, 3
            return None

        delays = []
        session = self._session(MemoryLog(hook))
        session.sleeper = delays.append
        report = drive(scenario("trim"), session)
        self.assertEqual(report["stopped"], "scaling")
        self.assertEqual(exit_code(report["stopped"]), EXIT_SCALING)
        self.assertGreater(delays[0], 0)
        self.assertEqual(session.contention_unresolved, 0)
        self.assertIn("contention retried", report["message"])

    def test_trim_unresolved_conflict_with_an_intact_log_is_scaling(self):
        def hook(path, _parsed, _log):
            if path.endswith("/append"):
                return 503, CONFLICT, 1
            return None

        session = self._session(MemoryLog(hook))
        session.sleeper = lambda _delay: None
        report = drive(scenario("trim"), session)
        self.assertEqual(report["stopped"], "scaling")
        self.assertGreater(report["summary"]["contention_unresolved"], 0)

    def test_trim_through_below_128_stays_a_correctness_failure(self):
        def hook(path, _parsed, _log):
            if path.endswith("/trim"):
                return 200, {"through": 1, "snapshot_due": False}, 1
            return None

        report = drive(scenario("trim"), self._session(MemoryLog(hook)))
        self.assertEqual(report["stopped"], "error")
        self.assertEqual(exit_code(report["stopped"]), EXIT_CORRECTNESS)
        self.assertEqual(report["summary"]["contention_unresolved"], 0)

    def test_trim_cursor_below_128_stays_a_correctness_failure(self):
        state = {"heads": 0}

        def hook(path, _parsed, _log):
            if path.endswith("/head"):
                state["heads"] += 1
                if state["heads"] > 1:
                    return 200, {"cursor": 1}, 1
            return None

        report = drive(scenario("trim"), self._session(MemoryLog(hook)))
        self.assertEqual(report["stopped"], "error")
        self.assertEqual(exit_code(report["stopped"]), EXIT_CORRECTNESS)

    def test_ack_holds_rejects_a_partial_row_and_a_lost_delivery(self):
        entry = {"delivery_seq": 1, "writer_device": "d", "writer_seq": 1}
        self.assertTrue(ack_holds([entry], [], 1))
        self.assertFalse(ack_holds([entry], [("d", 1)], 0))
        self.assertFalse(ack_holds([entry], [], 2))

    def _session(self, transport, clock=None):
        return Session(
            database_id_for_run("17"), "ab" * 16, "cd" * 32, transport, Ledger(),
            clock or (lambda: 0))


if __name__ == "__main__":
    unittest.main()
