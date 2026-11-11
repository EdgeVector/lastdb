"""Offline checks for the six-dollar cap. No network."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from cost import CAP_USD, REQUEST_USD, WRITE_USD, CapExceeded, Ledger, format_estimate
from drive import drive
from plan import STEADY_MS, scenario


class Cap(unittest.TestCase):
    def test_cap_is_six_dollars(self):
        self.assertEqual(CAP_USD, 6)
        self.assertEqual(Ledger().cap_usd, 6)

    def test_request_write_and_storage_each_count(self):
        ledger = Ledger()
        ledger.charge(1, 2048)
        estimate = ledger.estimate()
        self.assertEqual(estimate["requests"], 1)
        self.assertEqual(estimate["write_units"], 2)
        self.assertEqual(estimate["stored_bytes"], 2048)
        self.assertAlmostEqual(estimate["request_usd"], REQUEST_USD)
        self.assertAlmostEqual(estimate["write_usd"], 2 * WRITE_USD)
        self.assertGreater(estimate["storage_usd"], 0)
        self.assertIn("total:", format_estimate(estimate))

    def test_crossing_the_cap_aborts_before_the_call_is_counted(self):
        ledger = Ledger(cap_usd=REQUEST_USD)
        with self.assertRaises(CapExceeded) as caught:
            ledger.charge(1, None)
        self.assertGreaterEqual(caught.exception.estimate["total_usd"], REQUEST_USD)
        self.assertEqual(ledger.requests, 0)
        self.assertIn("cap: $0.00", format_estimate(caught.exception.estimate))

    def test_a_write_alone_can_reach_the_cap(self):
        ledger = Ledger(cap_usd=WRITE_USD)
        with self.assertRaises(CapExceeded):
            ledger.charge(0, 1)

    def test_full_levels_finish_and_the_cap_stops_the_next_call(self):
        calls = []

        def transport(method, path, body):
            calls.append(path)
            return 200, 1

        report = drive(scenario("5"), "c1da11d0-load-17", transport, Ledger(), lambda: 0)
        self.assertEqual(report["stopped"], "complete")
        self.assertEqual(len(calls), 5 * 12)
        self.assertTrue(all(path.startswith("/mutation-log/c1da11d0-load-17/") for path in calls))

        capped = Ledger(cap_usd=REQUEST_USD)
        stopped = drive(scenario("20"), "c1da11d0-load-17", transport, capped, lambda: 0)
        self.assertEqual(stopped["stopped"], "cap")
        self.assertEqual(capped.requests, 0)

    def test_one_hundred_writers_stop_at_three_minutes(self):
        seen = {"n": 0}

        def clock():
            seen["n"] += 1
            return 0 if seen["n"] < 5 else STEADY_MS

        def transport(_method, _path, _body):
            return 200, 1

        report = drive(scenario("100"), "c1da11d0-load-17", transport, Ledger(), clock)
        self.assertEqual(report["stopped"], "complete")
        self.assertLess(report["summary"]["requests"], 100)
        self.assertGreater(report["summary"]["requests"], 0)


if __name__ == "__main__":
    unittest.main()
