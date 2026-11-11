"""Offline checks for 503 classification and the load exit codes."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from faults import (
    EXIT_CAP, EXIT_CORRECTNESS, EXIT_OK, EXIT_SCALING, already_created,
    backoff_seconds, capacity_finding, exit_code, is_contention, is_duplicate,
    is_throttle, scaling_finding, worst_exit,
)


CONFLICT = {"code": "retry_later", "field": "mutation_log.transaction.conflict"}


class Classifier(unittest.TestCase):
    def test_only_the_head_row_conflict_is_contention(self):
        self.assertTrue(is_contention(503, CONFLICT))
        others = (
            (503, {"code": "retry_later", "field": "mutation_log.transaction.throttled"}),
            (503, {"code": "retry_later", "field": "mutation_log.submission.throttled"}),
            (503, {"code": "outcome_unknown", "field": "mutation_log.transaction"}),
            (503, {"code": "retry_later", "field": "other"}),
            (500, CONFLICT),
            (503, {"code": "retry_later"}),
            (503, None),
        )
        for status, body in others:
            self.assertFalse(is_contention(status, body), (status, body))

    def test_throttles_are_capacity_and_other_503s_are_not(self):
        throttled = (
            {"code": "retry_later", "field": "mutation_log.transaction.throttled"},
            {"code": "retry_later", "field": "mutation_log.submission.throttled"},
        )
        for body in throttled:
            self.assertTrue(is_throttle(503, body))
            self.assertFalse(is_contention(503, body))
        rejected = (
            CONFLICT,
            {"code": "outcome_unknown", "field": "mutation_log.transaction"},
            {"code": "retry_later", "field": "mutation_log.query"},
            {"code": "retry_later", "field": "mutation_log.transaction"},
        )
        for body in rejected:
            self.assertFalse(is_throttle(503, body), body)
        self.assertTrue(capacity_finding(1, "full", 0, 10))
        self.assertFalse(capacity_finding(0, "full", 5, 10))
        self.assertTrue(capacity_finding(0, "steady", 2, 100))
        self.assertFalse(capacity_finding(0, "steady", 1, 100))

    def test_duplicate_results_are_recognized(self):
        self.assertTrue(is_duplicate({"result": "duplicate"}))
        self.assertTrue(is_duplicate({"outcome": "duplicate"}))
        self.assertTrue(is_duplicate({"duplicate": {"next_seq": 2}}))
        self.assertFalse(is_duplicate({"result": "accepted"}))
        self.assertFalse(is_duplicate({}))

    def test_a_rerun_create_conflict_is_the_same_database(self):
        body = {"code": "identity_conflict", "field": "database_id"}
        self.assertTrue(already_created("create-database", 409, body))
        self.assertFalse(already_created("append", 409, body))
        self.assertFalse(already_created("create-database", 409, {"code": "predecessor_conflict"}))

    def test_backoff_is_bounded_and_jittered(self):
        low = backoff_seconds(1, lambda _span: 0)
        high = backoff_seconds(1, lambda span: span - 1)
        self.assertLess(low, high)
        self.assertLessEqual(backoff_seconds(20, lambda span: span - 1), 2.0)
        self.assertGreater(low, 0)

    def test_exit_codes_keep_correctness_ahead_of_scaling(self):
        self.assertEqual(exit_code("complete"), EXIT_OK)
        self.assertEqual(exit_code("error"), EXIT_CORRECTNESS)
        self.assertEqual(exit_code("cap"), EXIT_CAP)
        self.assertEqual(exit_code("scaling"), EXIT_SCALING)
        self.assertFalse(scaling_finding("full", 5, 10))
        self.assertFalse(scaling_finding("steady", 1, 100))
        self.assertTrue(scaling_finding("steady", 2, 100))
        self.assertEqual(worst_exit(["complete", "scaling", "cap"]), EXIT_SCALING)
        self.assertEqual(worst_exit(["scaling", "error"]), EXIT_CORRECTNESS)
        self.assertEqual(worst_exit(["complete"]), EXIT_OK)


if __name__ == "__main__":
    unittest.main()
