"""Offline checks for the shared cap, concurrency, and loud failures."""

import json
import sys
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from cost import (
    CAP_USD, FULL_PARTITION_LOAD, PAGE_BYTES, READ_USD, WRITE_USD, CapExceeded,
    Ledger, MUTATING_WRITE_UNITS, load_reads, stored_growth,
)
from drive import DISABLED, LIST_LIMIT, Flight, Session, _pages, drive, run_writers
from faults import EXIT_CAP, exit_code, scaling_finding
from guard import database_id_for_run
from logcheck import verify_log
from plan import Scenario, scenario
from routes import PAYLOAD, RouteError
from stats import combine_exit


CONFLICT = {"code": "retry_later", "field": "mutation_log.transaction.conflict"}
THROTTLED = {"code": "retry_later", "field": "mutation_log.transaction.throttled"}
UNKNOWN = {"code": "outcome_unknown", "field": "mutation_log.transaction"}


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
                    "code": "predecessor_conflict",
                    "field": "mutation_log.cursor",
                    "snapshot_through": len(self.entries),
                    "snapshot_ref": None,
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
        self.assertTrue(FULL_PARTITION_LOAD)
        ledger = Ledger()
        ledger.charge("read")
        ledger.charge("write", 2048)
        estimate = ledger.estimate()
        self.assertEqual(estimate["reads"], load_reads(0, 0) * 2)
        self.assertEqual(estimate["write_units"], MUTATING_WRITE_UNITS)
        self.assertEqual(estimate["partition_bytes"], stored_growth(2048))
        self.assertEqual(estimate["lambda_requests"], 2)
        held = ledger.partition_bytes
        ledger.charge("write", 80, grow=False)
        self.assertEqual(ledger.partition_bytes, held)
        ledger.charge("write", PAGE_BYTES)
        before = ledger.reads
        expected = load_reads(ledger.partition_bytes, ledger.sub_bytes)
        ledger.charge("read")
        self.assertEqual(ledger.reads - before, expected)
        self.assertEqual(load_reads(0, 0), 2)
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

    def test_a_budget_stop_inside_a_writer_exits_2(self):
        class StopAfterCreate(Ledger):
            def charge(self, kind, nbytes=None, grow=None, attempts=1, hold_recheck=True):
                if self.lambda_requests >= 1:
                    raise CapExceeded(self.estimate())
                return super().charge(
                    kind, nbytes, grow=grow, attempts=attempts, hold_recheck=hold_recheck)
        report = drive(scenario("5"), self._session(MemoryLog(), StopAfterCreate()))
        self.assertEqual((report["stopped"], exit_code(report["stopped"])), ("cap", EXIT_CAP))

    def test_a_client_error_fails_the_tier(self):
        def hook(path, _parsed, _log):
            if path.endswith("/append"):
                return 400, {"code": "invalid_input", "field": "mutation_log.sequence"}, 1
            return None

        report = drive(Scenario("steady", 2, 1, False), self._session(MemoryLog(hook)))
        self.assertEqual(report["stopped"], "error")

    def test_log_check_reads_past_1280_entries(self):
        count = 1281
        device = "bulk-d0"
        log = MemoryLog()
        for seq in range(1, count + 1):
            log.entries.append({
                "delivery_seq": seq, "accepted_at_ms": 1, "writer_key": "k",
                "writer_device": device, "writer_seq": seq, "authored_at_ms": 1,
                "namespace": "load", "payload": PAYLOAD,
            })
        session = self._session(log)
        session.accepted[device] = set(range(1, count + 1))
        found = _pages(session)
        self.assertEqual(len(found), count)
        verify_log(found, session.accepted)
        limits = [json.loads(body)["limit"] for path, body in log.seen if path.endswith("/list")]
        self.assertGreater(len(limits), 1280 // 90)
        self.assertTrue(all(limit == 90 for limit in limits))

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
        for suffix in ("/snapshot", "/trim"):
            matches = [body for path, body in log.seen if path.endswith(suffix)]
            self.assertFalse(matches)
        heads = [json.loads(body) for path, body in log.seen if path.endswith("/head")]
        self.assertTrue(heads)
        self.assertTrue(all(item == {"database_id": database} for item in heads))
        acks = [json.loads(body) for path, body in log.seen if path.endswith("/ack")]
        self.assertTrue(acks)
        self.assertTrue(all(item["through"] >= 1 for item in acks))
        self.assertTrue(all(
            json.loads(body)["database_id"] == database
            for path, body in log.seen if path.endswith("/create-database")))
        self.assertEqual(report["summary"]["contention_503"], 0)
        self.assertIn("p50_ms", report["summary"])
        self.assertIn("p99_ms", report["summary"])
        self.assertGreater(report["summary"]["partition_bytes"], 0)
        self.assertGreater(report["summary"]["reads"], 0)
        self.assertEqual(LIST_LIMIT, 90)

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

    def test_a_throttle_is_a_capacity_finding_when_the_write_succeeds(self):
        state = {"left": 1}

        def hook(path, _parsed, _log):
            if path.endswith("/append") and state["left"]:
                state["left"] -= 1
                return 503, THROTTLED, 4
            return None

        delays = []
        session = self._session(MemoryLog(hook))
        session.sleeper = delays.append
        report = drive(scenario("5"), session)
        self.assertEqual(report["stopped"], "scaling")
        self.assertEqual(session.throttle_503, 1)
        self.assertEqual(session.contention_503, 0)
        self.assertEqual(session.errors, 0)
        self.assertEqual(report["message"], "throttle")
        self.assertGreater(delays[0], 0)
        self.assertGreater(report["summary"]["throttle_rate"], 0)
        self.assertEqual(report["summary"]["contention_rate"], 0)

    def test_a_throttle_that_never_succeeds_exits_incomplete_3(self):
        def hook(path, _parsed, _log):
            if path.endswith("/append"):
                return 503, THROTTLED, 1
            return None

        session = self._session(MemoryLog(hook))
        session.sleeper = lambda _delay: None
        report = drive(Scenario("full", 1, None, False), session)
        self.assertEqual(report["stopped"], "scaling")
        self.assertEqual(session.throttle_503, 8)
        self.assertEqual(session.throttle_unresolved, 1)

    def test_outcome_unknown_and_other_503s_stop_the_run(self):
        query = {"code": "retry_later", "field": "mutation_log.query"}
        for body in (UNKNOWN, query):
            def hook(path, _parsed, _log, body=body):
                if path.endswith("/append"):
                    return 503, body, 1
                return None

            session = self._session(MemoryLog(hook))
            session.sleeper = self.refuse_retry
            report = drive(Scenario("steady", 1, 1, False), session)
            self.assertEqual(report["stopped"], "error")
            self.assertEqual(session.throttle_503, 0)
            self.assertEqual(session.contention_503, 0)
            self.assertGreater(session.other_503, 0)

    def test_ack_through_is_the_head_cursor(self):
        state = {"heads": 0}

        def hook(path, _parsed, _log):
            if path.endswith("/head"):
                state["heads"] += 1
                if state["heads"] == 1:
                    return 200, {"cursor": 40}, 1
            return None

        log = MemoryLog(hook)
        report = drive(Scenario("full", 1, None, False), self._session(log))
        self.assertEqual(report["stopped"], "complete")
        acks = [json.loads(body) for _path, body in log.seen if _path.endswith("/ack")]
        self.assertEqual([item["through"] for item in acks], [40])
        self.assertNotEqual(acks[0]["through"], 10)

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

    def test_exhausted_retries_with_an_intact_log_are_scaling(self):
        def hook(path, _parsed, _log):
            if path.endswith("/append"):
                return 503, CONFLICT, 1
            return None

        session = self._session(MemoryLog(hook))
        session.sleeper = lambda _delay: None
        report = drive(Scenario("steady", 1, 1, False), session)
        self.assertEqual(report["stopped"], "scaling")
        self.assertEqual(session.contention_503, 8)
        self.assertEqual(report["summary"]["contention_unresolved"], 1)

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

    def refuse_retry(self, _delay):
        raise AssertionError("a closed tier must not retry a 503")


if __name__ == "__main__":
    unittest.main()
