"""Offline checks for the shared cap, concurrency, and loud failures."""

import json
import sys
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from cost import CAP_USD, READ_USD, WRITE_USD, CapExceeded, Ledger
from drive import DISABLED, Flight, Session, drive, run_writers
from guard import database_id_for_run
from plan import scenario
from stats import combine_exit


def ok_transport(method, path, body):
    if path.endswith("/head"):
        return 200, {"cursor": "128"}, 1
    if path.endswith("/snapshot"):
        return 200, {
            "through_delivery_seq": "128", "at_ms": "1", "snapshot_ref": "load",
        }, 1
    if path.endswith("/trim"):
        return 200, {"through": "128", "snapshot_due": False}, 1
    if path.endswith("/list"):
        return 200, {"entries": [], "next": "0"}, 1
    if path.endswith("/ack"):
        return 200, {"through": "1"}, 1
    return 200, {}, 1


class CapAndRun(unittest.TestCase):
    def test_cap_is_six_dollars_and_sums_every_charge(self):
        self.assertEqual(CAP_USD, 6)
        self.assertEqual(WRITE_USD, 2 * 1.25 / 1_000_000)
        ledger = Ledger()
        ledger.charge("read")
        ledger.charge("write", 2048)
        estimate = ledger.estimate()
        self.assertEqual(estimate["reads"], 1)
        self.assertEqual(estimate["write_units"], 2)
        self.assertEqual(estimate["lambda_requests"], 2)
        self.assertAlmostEqual(estimate["read_usd"], READ_USD)
        self.assertAlmostEqual(estimate["write_usd"], 2 * WRITE_USD)
        self.assertGreater(estimate["total_usd"], estimate["read_usd"] + estimate["write_usd"])
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
        self.assertEqual(combine_exit(0, 1), 1)

    def test_writers_run_together(self):
        flight = Flight()
        barrier = threading.Barrier(4)

        def worker(_index):
            barrier.wait(timeout=2)

        run_writers(4, worker, flight)
        self.assertEqual(flight.max, 4)

    def test_full_level_finishes_and_a_server_error_stops_the_run(self):
        calls = []

        def transport(method, path, body):
            calls.append(path)
            return ok_transport(method, path, body)

        session = self._session(transport)
        report = drive(scenario("5"), session)
        self.assertEqual(report["stopped"], "complete")
        self.assertEqual(len(calls), 1 + 5 * 12)
        self.assertTrue(all("/api/cloud/backups/v2/mutation-log/" in path for path in calls))

        def fail(_method, _path, _body):
            return 500, {}, 1

        failed = drive(scenario("5"), self._session(fail))
        self.assertEqual(failed["stopped"], "error")

    def test_trim_moves_the_floor_and_the_cap_stops_the_next_call(self):
        seen = []

        def transport(method, path, body):
            seen.append((path, body))
            if path.endswith("/list") and any(item[0].endswith("/trim") for item in seen):
                return 409, {
                    "code": "predecessor_conflict", "field": "cursor",
                    "snapshot_through": "128",
                }, 1
            return ok_transport(method, path, body)

        report = drive(scenario("trim"), self._session(transport))
        self.assertEqual(report["stopped"], "complete")
        created = json.loads(seen[0][1])
        self.assertEqual(set(created), {"database_id", "kind"})
        self.assertEqual(created["kind"], "personal")
        appended = json.loads(next(body for path, body in seen if path.endswith("/append")))
        self.assertEqual(set(appended), {"database_id", "device_id", "entries"})
        self.assertEqual(appended["entries"][0]["payload"], "load")
        self.assertIsInstance(appended["entries"][0]["writer_seq"], int)
        self.assertTrue(all(
            body == b"" for path, body in seen
            if path.endswith(("/head", "/snapshot", "/trim"))))
        stopped = drive(scenario("20"), self._session(ok_transport, Ledger(cap_usd=1e-12)))
        self.assertEqual(stopped["stopped"], "cap")

    def test_a_disabled_log_stops_with_a_clear_message(self):
        def transport(_method, _path, _body):
            return 404, {"code": "not_found", "field": "mutation_log.disabled"}, 1

        report = drive(scenario("5"), self._session(transport))
        self.assertEqual(report["stopped"], "error")
        self.assertEqual(report["message"], DISABLED)

    def _session(self, transport, ledger=None):
        return Session(database_id_for_run("17"), "ab" * 16, "cd" * 32, transport,
                       ledger or Ledger(), lambda: 0)


if __name__ == "__main__":
    unittest.main()
