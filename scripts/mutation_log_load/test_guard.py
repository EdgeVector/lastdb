"""Offline checks for the DEV load target. No network."""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from guard import (DEV_TABLE, GuardError, allow_bucket, allow_endpoint,
                   allow_prefix, allow_role, allow_table, allow_table_arn,
                   cleanup_plan, database_id_for_run, load_endpoints,
                   refuse_prod_environment, retain_keys)
from plan import TRIM_WRITERS, STEADY_MS, operations, scenario, still_running
from sign import canonical


DEV = "https://dev.example.test"
PROD = "https://prod.example.test"
PREFIX = "mutation-log/c1da11d0-load-17/"
ROLE = "arn:aws:iam::152335099025:role/LastDBMutationLogDev"


class TargetGuard(unittest.TestCase):
    def test_dev_table_and_load_prefix_are_accepted(self):
        self.assertEqual(allow_table(DEV_TABLE), DEV_TABLE)
        self.assertEqual(allow_prefix(PREFIX), PREFIX)
        self.assertEqual(allow_endpoint(DEV, DEV, PROD), DEV)
        self.assertEqual(database_id_for_run("17"), "c1da11d0-load-17")

    def test_other_table_is_refused(self):
        for name in ("ExememMutationLog-prod", "ExememCloudControl-dev", "other"):
            with self.assertRaises(GuardError):
                allow_table(name)

    def test_other_prefix_is_refused(self):
        for prefix in (
            "mutation-log/c1da11d0-real/",
            "mutation-log/c1da11d0-load",
            "mutation-log/other/c1da11d0-load-1/",
            "mutation-log/c1da11d0-load-1/../secret",
            "/mutation-log/c1da11d0-load-1/",
        ):
            with self.assertRaises(GuardError):
                allow_prefix(prefix)

    def test_prod_endpoint_is_refused(self):
        with self.assertRaises(GuardError):
            allow_endpoint(PROD, DEV, PROD)
        with self.assertRaises(GuardError):
            allow_endpoint("https://other.example.test", DEV, PROD)
        with self.assertRaises(GuardError):
            refuse_prod_environment(PROD, ["keep", "note " + PROD])

    def test_prod_role_and_bucket_are_refused(self):
        with self.assertRaises(GuardError):
            allow_role("arn:aws:iam::152335099025:role/LastDBProd")
        with self.assertRaises(GuardError):
            allow_role("arn:aws:iam::999999999999:role/LastDBMutationLogDev")
        self.assertEqual(allow_role(ROLE), ROLE)
        with self.assertRaises(GuardError):
            allow_bucket("mutation-log-prod")
        self.assertEqual(allow_bucket("mutation-log-dev"), "mutation-log-dev")

    def test_table_arn_must_be_the_dev_table(self):
        arn = "arn:aws:dynamodb:us-west-2:152335099025:table/" + DEV_TABLE
        self.assertEqual(allow_table_arn(arn, DEV_TABLE), arn)
        with self.assertRaises(GuardError):
            allow_table_arn(arn.replace(DEV_TABLE, "ExememMutationLog-prod"), DEV_TABLE)

    def test_cleanup_stays_on_the_load_prefix(self):
        plan = cleanup_plan(DEV_TABLE, PREFIX, "mutation-log-dev")
        self.assertEqual(plan["dynamodb"]["operation"], "Query")
        self.assertEqual(plan["dynamodb"]["TableName"], DEV_TABLE)
        self.assertNotIn("Scan", json.dumps(plan))
        self.assertEqual(plan["s3"]["Prefix"], PREFIX)
        self.assertEqual(retain_keys([PREFIX + "a"], PREFIX), [PREFIX + "a"])
        with self.assertRaises(GuardError):
            retain_keys(["mutation-log/customer/a"], PREFIX)
        with self.assertRaises(GuardError):
            cleanup_plan("ExememMutationLog-prod", PREFIX, "mutation-log-dev")

    def test_registry_dev_is_not_prod(self):
        registry = Path(__file__).resolve().parents[2] / "folddb_profile" / "environments.json"
        dev, prod = load_endpoints(registry)
        self.assertNotEqual(dev, prod)
        self.assertEqual(allow_endpoint(dev, dev, prod), dev)
        with self.assertRaises(GuardError):
            allow_endpoint(prod, dev, prod)

    def test_trim_has_more_than_100_writers_and_steady_is_three_minutes(self):
        trim = scenario("trim")
        self.assertGreater(trim.writers, 100)
        self.assertEqual(trim.writers, TRIM_WRITERS)
        ops = list(operations(trim))
        self.assertEqual(sum(1 for op in ops if op[0] == "append"), TRIM_WRITERS)
        self.assertEqual(ops[-1][0], "trim")
        steady = scenario("100")
        self.assertEqual(steady.steady_ms, STEADY_MS)
        self.assertTrue(still_running(steady, STEADY_MS - 1, 0))
        self.assertFalse(still_running(steady, STEADY_MS, 0))
        full = scenario("5")
        self.assertIsNone(full.steady_ms)
        self.assertTrue(still_running(full, STEADY_MS * 10, 0))
        self.assertFalse(still_running(full, 0, 1))
        self.assertEqual(scenario("20").writers, 20)

    def test_signature_text_is_body_hash_method_path_database_and_time(self):
        text = canonical("post", "/mutation-log/c1da11d0-load-17/append",
                         "c1da11d0-load-17", 10, b"{}").decode("utf-8")
        body_hash, method, path, database_id, timestamp = text.split("\n")
        self.assertEqual(len(body_hash), 64)
        self.assertEqual(method, "POST")
        self.assertTrue(path.startswith("/mutation-log/c1da11d0-load-"))
        self.assertEqual(database_id, "c1da11d0-load-17")
        self.assertEqual(timestamp, "10")
        allow_prefix(path.lstrip("/"))


if __name__ == "__main__":
    unittest.main()
