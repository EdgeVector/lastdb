"""Offline checks for the shared cap, concurrency, and loud failures."""

import json
import sys
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from cost import CAP_USD, PAGE_BYTES, READ_USD, WRITE_USD, CapExceeded, Ledger
from drive import DISABLED, Flight, Session, drive, run_writers
from faults import scaling_finding
from guard import database_id_for_run
from logcheck import verify_log
from plan import Scenario, scenario
from routes import PAYLOAD, RouteError
from stats import combine_exit


CONFLICT = {"code": "retry_later", "field": "mutation_log.transaction.conflict"}
THROTTLED = {"code": "retry_later", "field": "mutation_log.transaction.throttled"}


class MemoryLog:
    """A thread-safe log the audit can read back."""

    def __init__(self, hook=None):
        self.entries = []
        self.seen = []
        self.trimmed = False
        self.hook = hook
        self._lock = threading.Lock()

    def __call__(self, _method, path, body):
        parsed = json.loads(body) if body else {}
        with self._lock:
            self.seen.append((path, body))
            if self.hook is not None:
                hooked = self.hook(path, parsed, self)
                if hooked is not None:
                    return hooked
            return self._reply(path, parsed)

    def _reply(self, path, parsed):
        if path.endswith("/append"):
            entry = parsed["entries"][0]
            self.entries.append({
                "delivery_seq": len(self.entries) + 1,
                "accepted_at_ms": 1,
                "writer_key": "k",
                "writer_device": parsed["device_id"],
                "writer_seq": entry["writer_seq"],
                "authored_at_ms": entry["authored_at_ms"],
                "namespace": entry["namespace"],
                "payload": entry["payload"],
            })
            return 200, {}, 1
        if path.endswith("/head"):
            cursor = len(self.entries) if self.entries else None
            return 200, {"cursor": cursor}, 1
        if path.endswith("/list"):
            if self.trimmed and parsed.get("after") == 0:
                return 409, {
                    "code": "predecessor_conflict", "field": "cursor",
                    "snapshot_through": len(self.entries),
                }, 1
            after = parsed["after"]
            page = [item for item in self.entries if item["delivery_seq"] > after]
            page = page[:parsed["limit"]]
            nxt = page[-1]["delivery_seq"] if page else None
            return 200, {"entries": page, "next": nxt}, 1
        if path.endswith("/ack"):
            return 200, {"through": parsed["through"]}, 1
        if path.endswith("/snapshot"):
            return 200, {
                "through_delivery_seq": len(self.entries),
                "at_ms": 1,
                "snapshot_ref": "load",
            }, 1
        if path.endswith("/trim"):
            self.trimmed = True
            return 200, {"through": len(self.entries), "snapshot_due": False}, 1
        return 200, {}, 1


class CapAndRun(unittest.TestCase):
    def test_cap_is_six_dollars_and_reads_the_whole_partition(self):
        self.assertEqual(CAP_USD, 6)
        self.assertEqual(WRITE_USD, 2 * 1.25 / 1_000_000)
        ledger = Ledger()
        ledger.charge("read")
        ledger.charge("write", 2048)
        estimate = ledger.estimate()
        self.assertEqual(estimate["reads"], 2)
        self.assertEqual(estimate["write_units"], 4)
        self.assertEqual(estimate["partition_bytes"], 2048)
        self.assertEqual(estimate["lambda_requests"], 2)
        self.assertAlmostEqual(estimate["read_usd"], 2 * READ_USD)
        self.assertAlmostEqual(estimate["write_usd"], 4 * WRITE_USD)
        ledger.charge("write", PAGE_BYTES)
        before = ledger.reads
        ledger.charge("read")
        self.assertEqual(ledger.reads - before, 2)
        with self.assertRaises(CapExceeded):
            Ledger(cap_usd=READ_USD).charge("read")
        capped = Ledger(cap_usd=0)
        with self.assertRaises(CapExceeded):
            capped.charge("write", 1)
        capped.record("write", 1)
        self.assertEqual(capped.write_units, 1)

    def test_cleanup_failure_does_not_hide_the_load(self):
        self.assertEqual(combine_exit(1, 1), 1)
        self.assertEqual(combine_exit(2, 1), 2)
        self.assertEqual(combine_exit(3, 1), 3)
        self.assertEqual(combine_exit(0, 1), 1)

    def test_writers_run_together(self):
        flight = Flight()
        barrier = threading.Barrier(4)

        def worker(_index):
            barrier.wait(timeout=2)

        run_writers(4, worker, flight)
        self.assertEqual(flight.max, 4)

    def test_full_level_keeps_every_entry(self):
        log = MemoryLog()
        report = drive(scenario("5"), self._session(log))
        self.assertEqual(report["stopped"], "complete")
        self.assertEqual(len(log.entries), 50)
        appended = json.loads(next(body for path, body in log.seen if path.endswith("/append")))
        self.assertEqual(appended["entries"][0]["payload"], PAYLOAD)
        self.assertEqual(PAYLOAD, "bG9hZA")
        database = database_id_for_run("17")
        for suffix in ("/head", "/snapshot", "/trim"):
            matches = [body for path, body in log.seen if path.endswith(suffix)]
            self.assertFalse(matches)
        self.assertTrue(all(
            json.loads(body)["database_id"] == database
            for path, body in log.seen if path.endswith("/create-database")))
        self.assertEqual(report["summary"]["contention_503"], 0)
        self.assertIn("p50_ms", report["summary"])
        self.assertIn("p99_ms", report["summary"])

    def test_trim_acks_the_head_cursor(self):
        log = MemoryLog()
        report = drive(scenario("trim"), self._session(log))
        self.assertEqual(report["stopped"], "complete")
        acks = [json.loads(body) for path, body in log.seen if path.endswith("/ack")]
        self.assertEqual(len(acks), 128)
        self.assertTrue(all(item["through"] >= 128 for item in acks))
        self.assertGreaterEqual(log.entries and max(
            item["delivery_seq"] for item in log.entries), 128)
        heads = [body for path, body in log.seen if path.endswith("/head")]
        self.assertEqual(json.loads(heads[0]), {"database_id": database_id_for_run("17")})

    def test_any_503_at_five_writers_is_a_correctness_failure(self):
        def hook(path, _parsed, _log):
            if path.endswith("/append"):
                return 503, CONFLICT, 3
            return None

        for level in ("5", "20"):
            session = self._session(MemoryLog(hook))
            session.sleeper = self.fail
            report = drive(scenario(level), session)
            self.assertEqual(report["stopped"], "error", level)
            self.assertGreater(session.contention_503, 0, level)
            self.assertEqual(session.other_503, 0, level)

    def test_a_throttled_503_is_not_contention(self):
        unknown = {"code": "outcome_unknown", "field": "mutation_log.transaction"}
        for body in (THROTTLED, unknown):
            def hook(path, _parsed, _log, body=body):
                if path.endswith("/append"):
                    return 503, body, 3
                return None

            session = self._session(MemoryLog(hook))
            report = drive(Scenario("steady", 1, 1, False), session)
            self.assertEqual(report["stopped"], "error")
            self.assertEqual(session.contention_503, 0)
            self.assertGreater(session.other_503, 0)

    def test_steady_retries_contention_until_the_write_succeeds(self):
        state = {"left": 1}

        def hook(path, _parsed, _log):
            if path.endswith("/append") and state["left"]:
                state["left"] -= 1
                return 503, CONFLICT, 5
            return None

        delays = []
        session = self._session(MemoryLog(hook), clock=self._clock(12))
        session.sleeper = delays.append
        spec = Scenario("steady", 1, 1, False)
        report = drive(spec, session)
        self.assertEqual(report["stopped"], "scaling")
        self.assertEqual(session.contention_503, 1)
        self.assertEqual(session.errors, 0)
        self.assertGreater(delays[0], 0)
        self.assertTrue(scaling_finding("steady", session.contention_503, session.write_attempts))
        self.assertGreater(report["summary"]["contention_rate"], 0.01)

    def test_a_gap_stays_a_correctness_failure(self):
        state = {"left": 1}

        def hook(path, _parsed, _log):
            if path.endswith("/append") and state["left"]:
                state["left"] -= 1
                return 503, CONFLICT, 1
            if path.endswith("/list"):
                return 200, {"entries": [], "next": None}, 1
            return None

        session = self._session(MemoryLog(hook), clock=self._clock(12))
        session.sleeper = lambda _delay: None
        report = drive(Scenario("steady", 1, 1, False), session)
        self.assertEqual(report["stopped"], "error")
        self.assertGreater(session.contention_503, 0)

    def test_exhausted_retries_are_a_lost_write(self):
        def hook(path, _parsed, _log):
            if path.endswith("/append"):
                return 503, CONFLICT, 1
            return None

        session = self._session(MemoryLog(hook))
        session.sleeper = lambda _delay: None
        report = drive(Scenario("steady", 1, 1, False), session)
        self.assertEqual(report["stopped"], "error")
        self.assertEqual(session.contention_503, 8)

    def test_duplicate_and_disabled_and_cap_stop(self):
        def duplicate(path, _parsed, _log):
            if path.endswith("/append"):
                return 200, {"result": "duplicate"}, 1
            return None

        dup = drive(scenario("5"), self._session(MemoryLog(duplicate)))
        self.assertEqual(dup["stopped"], "error")
        self.assertEqual(dup["message"], "duplicate")

        def created(_path, _parsed, _log):
            return 409, {"code": "identity_conflict", "field": "database_id"}, 1

        rerun = MemoryLog(created)
        rerun.hook = lambda path, parsed, log: (
            created(path, parsed, log) if path.endswith("/create-database") else None)
        report = drive(scenario("5"), self._session(rerun))
        self.assertEqual(report["stopped"], "complete")
        self.assertEqual(len(rerun.entries), 50)

        def disabled(_method, _path, _body):
            return 404, {"code": "not_found", "field": "mutation_log.disabled"}, 1

        stopped = drive(scenario("5"), self._session(disabled))
        self.assertEqual(stopped["message"], DISABLED)
        capped = drive(scenario("20"), self._session(MemoryLog(), Ledger(cap_usd=1e-12)))
        self.assertEqual(capped["stopped"], "cap")

    def test_log_invariants(self):
        entries = [{
            "delivery_seq": 1, "writer_device": "d", "writer_seq": 1,
            "accepted_at_ms": 1, "writer_key": "k", "authored_at_ms": 1,
            "namespace": "load", "payload": PAYLOAD,
        }]
        verify_log(entries, {"d": {1}})
        with self.assertRaises(RouteError):
            verify_log(entries, {"d": {1, 2}})
        entries.append(dict(entries[0], delivery_seq=1, writer_seq=2))
        with self.assertRaises(RouteError):
            verify_log(entries, {"d": {1, 2}})

    def _session(self, transport, ledger=None, clock=None):
        return Session(database_id_for_run("17"), "ab" * 16, "cd" * 32, transport,
                       ledger or Ledger(), clock or (lambda: 0))

    def _clock(self, zeros):
        state = {"n": 0}

        def clock():
            state["n"] += 1
            return 0 if state["n"] <= zeros else 10 ** 9

        return clock

    def fail(self, _delay):
        raise AssertionError("a closed tier must not retry a 503")


if __name__ == "__main__":
    unittest.main()
