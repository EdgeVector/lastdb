"""Exit rules for a retried conflict and a query throttle."""

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from cost import CapExceeded, Ledger
from drive import LoadError, Session, drive, run_writers
from faults import EXIT_CORRECTNESS, EXIT_OK, EXIT_SCALING, exit_code
from finish import trim_appends
from guard import GuardError, database_id_for_run
from logcheck import ack_holds
from plan import Scenario, scenario
from run import batch_retry, keep_going
from stats import render_summaries, write_summaries
from test_cost import CONFLICT, THROTTLED, CapAndRun, MemoryLog


QUERY_THROTTLE = {"code": "retry_later", "field": "mutation_log.query.throttled"}
PLAIN_QUERY = {"code": "retry_later", "field": "mutation_log.query"}


class _ZeroDraw:
    def randrange(self, _span):
        return 0


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

    def test_zero_trim_appends_are_trim_not_exercised(self):
        def hook(path, _parsed, _log):
            if path.endswith("/append"):
                return 503, CONFLICT, 1
            return None

        session = self._session(MemoryLog(hook))
        session.sleeper = lambda _delay: None
        report = drive(scenario("trim"), session)
        self.assertEqual(report["stopped"], "scaling")
        self.assertEqual(exit_code(report["stopped"]), EXIT_SCALING)
        self.assertEqual(report["message"], "trim not exercised")
        self.assertEqual(report["summary"]["unresolved_appends"], 128)
        self.assertFalse(any(
            path.endswith("/snapshot") or path.endswith("/trim")
            for path, _body in session.transport.seen))

    def test_one_landed_trim_append_still_runs_trim(self):
        def hook(path, parsed, _log):
            if path.endswith("/append") and parsed["device_id"].endswith("-d0"):
                return 503, CONFLICT, 1
            return None

        session = self._session(MemoryLog(hook))
        session.sleeper = lambda _delay: None
        report = drive(scenario("trim"), session)
        self.assertEqual(report["stopped"], "scaling")
        self.assertEqual(report["summary"]["unresolved_appends"], 1)
        self.assertIn("unresolved appends 1", report["message"])
        self.assertTrue(any(path.endswith("/trim") for path, _body in session.transport.seen))

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

    def test_an_exhausted_trim_ack_is_classified(self):
        def hook(path, _parsed, _log):
            if path.endswith("/ack"):
                return 503, CONFLICT, 1
            return None

        session = self._session(MemoryLog(hook))
        session.sleeper = lambda _delay: None
        report = drive(scenario("trim"), session)
        self.assertEqual(report["stopped"], "scaling")
        self.assertEqual(exit_code(report["stopped"]), EXIT_SCALING)
        self.assertGreater(session.contention_unresolved, 0)
        self.assertFalse(any(path.endswith("/snapshot") for path, _body in session.transport.seen))

    def test_a_sibling_failure_outranks_the_budget_stop(self):
        def worker(index):
            if index == 0:
                raise LoadError("duplicate")
            raise CapExceeded({})

        with self.assertRaises(LoadError):
            run_writers(2, worker)

    def test_counted_throttles_outrank_the_budget_stop(self):
        state = {"left": 1}

        def hook(path, _parsed, _log):
            if path.endswith("/append") and state["left"]:
                state["left"] -= 1
                return 503, THROTTLED, 2
            return None

        class CapLater(Ledger):
            def charge(self, kind, nbytes=None, grow=None, attempts=1, hold_recheck=True):
                if self.lambda_requests >= 2:
                    raise CapExceeded(self.estimate())
                return super().charge(
                    kind, nbytes, grow=grow, attempts=attempts, hold_recheck=hold_recheck)

        session = self._session(MemoryLog(hook), ledger=CapLater())
        session.sleeper = lambda _delay: None
        report = drive(Scenario("full", 1, None, False), session)
        self.assertEqual(report["stopped"], "scaling")
        self.assertEqual(exit_code(report["stopped"]), EXIT_SCALING)
        self.assertGreater(session.throttle_503, 0)

    def test_a_scaling_tier_does_not_stop_the_next_tier(self):
        state = {"left": 1}

        def hook(path, _parsed, _log):
            if path.endswith("/append") and state["left"]:
                state["left"] -= 1
                return 503, CONFLICT, 2
            return None

        session = self._session(MemoryLog(hook))
        session.sleeper = lambda _delay: None
        reports = []
        for spec in (scenario("5"), scenario("trim")):
            report = drive(spec, session)
            reports.append(report)
            if not keep_going(report["stopped"]):
                break
        self.assertEqual([item["stopped"] for item in reports], ["scaling", "complete"])
        self.assertTrue(keep_going("scaling"))
        self.assertFalse(keep_going("error"))
        self.assertFalse(keep_going("cap"))

    def test_trim_does_not_borrow_an_earlier_cursor(self):
        def hook(path, _parsed, _log):
            if path.endswith("/append"):
                return 503, CONFLICT, 1
            return None

        log = MemoryLog(hook)
        for seq in range(1, 129):
            log.entries.append({
                "delivery_seq": seq, "writer_device": "full5-d0", "writer_seq": seq,
                "accepted_at_ms": 1, "writer_key": "k", "authored_at_ms": 1,
                "namespace": "load", "payload": "bG9hZA",
            })
        session = self._session(log)
        session.accepted["full5-d0"] = set(range(1, 129))
        session.sleeper = lambda _delay: None
        report = drive(scenario("trim"), session)
        self.assertEqual(report["stopped"], "scaling")
        self.assertEqual(report["message"], "trim not exercised")
        self.assertEqual(trim_appends(session), 0)
        self.assertGreaterEqual(session.audit_cursor, 128)
        self.assertFalse(any(path.endswith("/trim") for path, _body in session.transport.seen))

    def test_cleanup_retries_unprocessed_writes_with_backoff(self):
        delays = []
        misses = 0
        for _ in range(8):
            misses = batch_retry(misses, delays.append, lambda span: span - 1)
        self.assertEqual(misses, 8)
        self.assertGreater(delays[0], 0)
        with self.assertRaises(GuardError):
            batch_retry(misses, delays.append, lambda span: 0)

    def test_a_read_throttle_during_logcheck_is_reread_after_the_drain(self):
        state = {"heads": 0}

        def hook(path, _parsed, _log):
            if path.endswith("/head"):
                state["heads"] += 1
                if 2 <= state["heads"] <= 9:
                    return 503, QUERY_THROTTLE, 1
            return None

        session = self._session(MemoryLog(hook))
        delays = []
        session.sleeper = delays.append
        session.rng = _ZeroDraw()
        report = drive(Scenario("full", 1, None, False), session)
        self.assertNotEqual(report["stopped"], "error")
        self.assertTrue(report["summary"]["logcheck_recheck"])
        self.assertGreater(delays[-1], delays[0])
        self.assertGreaterEqual(delays[-1], 0.2)

    def test_a_read_throttle_that_survives_the_quiet_recheck_is_exit_1(self):
        def hook(path, _parsed, _log):
            if path.endswith("/head"):
                return 503, QUERY_THROTTLE, 1
            return None

        session = self._session(MemoryLog(hook))
        session.sleeper = lambda _delay: None
        report = drive(Scenario("full", 1, None, False), session)
        self.assertEqual(report["stopped"], "error")
        self.assertEqual(exit_code(report["stopped"]), EXIT_CORRECTNESS)
        self.assertTrue(report["summary"]["logcheck_recheck"])

    def test_an_exhausted_snapshot_rereads_before_exit_3(self):
        def hook(path, _parsed, _log):
            if path.endswith("/snapshot"):
                return 503, CONFLICT, 1
            return None

        session = self._session(MemoryLog(hook))
        session.sleeper = lambda _delay: None
        report = drive(scenario("trim"), session)
        seen = [path for path, _body in session.transport.seen]
        snap = seen.index(next(path for path in seen if path.endswith("/snapshot")))
        self.assertTrue(any(path.endswith("/head") for path in seen[snap:]))
        self.assertEqual(report["stopped"], "scaling")

    def test_the_summary_file_keeps_every_tier(self):
        reports = [
            {"summary": {"run": "preliminary correctness-and-contention run",
                         "p50_ms": 1, "p95_ms": 1, "p99_ms": 1, "errors": 0,
                         "throughput_per_s": 1}},
            {"summary": {"run": "preliminary correctness-and-contention run",
                         "p50_ms": 2, "p95_ms": 2, "p99_ms": 2, "errors": 0,
                         "throughput_per_s": 2}},
        ]
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "summary.json"
            write_summaries(path, reports)
            saved = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(len(saved), 2)
        text = render_summaries(saved)
        self.assertEqual(text.count("preliminary correctness-and-contention run"), 2)

    def test_ack_holds_rejects_a_partial_row_and_a_lost_delivery(self):
        entry = {"delivery_seq": 1, "writer_device": "d", "writer_seq": 1}
        self.assertTrue(ack_holds([entry], [], 1))
        self.assertFalse(ack_holds([entry], [("d", 1)], 0))
        self.assertFalse(ack_holds([entry], [], 2))

    def _session(self, transport, clock=None, ledger=None):
        return Session(
            database_id_for_run("17"), "ab" * 16, "cd" * 32, transport,
            ledger or Ledger(), clock or (lambda: 0))


if __name__ == "__main__":
    unittest.main()
