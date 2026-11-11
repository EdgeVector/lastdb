"""Offline checks for the shared cap, concurrency, and loud failures."""

import sys
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from cost import CAP_USD, READ_USD, WRITE_USD, CapExceeded, Ledger
from drive import Flight, Session, drive, run_writers
from plan import scenario
from stats import combine_exit


def ok_transport(method, path, body):
    if path.endswith("/head"):
        return 200, {"trimmed_through": "0", "delivery_seq": "128"}, 1
    if path.endswith("/trim"):
        return 200, {"through": "128", "dropped": "128"}, 1
    if path.endswith("/list"):
        return 200, {"entries": []}, 1
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

    def test_trim_drops_entries_and_the_cap_stops_the_next_call(self):
        report = drive(scenario("trim"), self._session(ok_transport))
        self.assertEqual(report["stopped"], "complete")
        stopped = drive(scenario("20"), self._session(ok_transport, Ledger(cap_usd=1e-12)))
        self.assertEqual(stopped["stopped"], "cap")

    def _session(self, transport, ledger=None):
        return Session("c1da11d0-load-17", "ab" * 16, "cd" * 32, transport,
                       ledger or Ledger(), lambda: 0)


if __name__ == "__main__":
    unittest.main()
