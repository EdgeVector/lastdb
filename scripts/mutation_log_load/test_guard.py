"""Offline checks for the DEV table, region, and load database. No network."""

import hashlib
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from guard import (DEV_ACCOUNT, DEV_REGION, DEV_TABLE, GuardError, allow_endpoint,
                   allow_partition, allow_region, allow_stream, allow_table,
                   allow_table_arn, continue_page, database_id_for_run, delete_batch,
                   load_endpoints, query_request, record_ids_from_query,
                   snap_partition, unprocessed_ids)
from plan import STEADY_MS, TRIM_WRITERS, scenario, still_running
from routes import PAYLOAD, path_for, prove_trimmed_list, request_body, require_trim


DEV = "https://dev.example.test"
PROD = "https://prod.example.test"


class TargetGuard(unittest.TestCase):
    def test_only_the_dev_table_in_us_west_2(self):
        self.assertEqual(allow_region(DEV_REGION), DEV_REGION)
        self.assertEqual(allow_table(DEV_TABLE), DEV_TABLE)
        arn = "arn:aws:dynamodb:{0}:{1}:table/{2}".format(DEV_REGION, DEV_ACCOUNT, DEV_TABLE)
        self.assertEqual(allow_table_arn(arn, DEV_TABLE), arn)
        prod = arn.replace(DEV_REGION, "us-east-1")
        with self.assertRaises(GuardError):
            allow_table_arn(prod, DEV_TABLE)
        with self.assertRaises(GuardError):
            allow_region("us-east-1")
        with self.assertRaises(GuardError):
            allow_table("ExememMutationLog-prod")

    def test_load_database_id_is_accepted_and_other_ids_are_not(self):
        digest = hashlib.sha256(b"17").hexdigest()
        expected = "c1da11d0-0000-4000-8000-" + digest[:12]
        self.assertEqual(len(expected), 36)
        self.assertEqual(database_id_for_run("17"), expected)
        self.assertEqual(allow_stream(expected), expected)
        snap = snap_partition(expected)
        self.assertEqual(snap, expected + "#snap")
        self.assertEqual(allow_partition(snap), snap)
        self.assertEqual(query_request(DEV_TABLE, snap)["ExpressionAttributeValues"][":id"]["S"],
                         snap)
        for value in ("c1da11d0-load-17", "mutation-log/c1da11d0-load-17",
                      "c1da11d0-load-", "", expected + "#other", expected + "#snap-extra"):
            with self.assertRaises(GuardError):
                allow_partition(value)
        self.assertFalse(continue_page(3, None))
        self.assertTrue(continue_page(63, {"database_id": {"S": expected}}))
        with self.assertRaises(GuardError):
            continue_page(64, {"database_id": {"S": expected}})

    def test_prod_endpoint_is_refused(self):
        self.assertEqual(allow_endpoint(DEV, DEV, PROD), DEV)
        with self.assertRaises(GuardError):
            allow_endpoint(PROD, DEV, PROD)
        registry = Path(__file__).resolve().parents[2] / "lastdb_node" / "environments.json"
        dev, prod = load_endpoints(registry)
        self.assertNotEqual(dev, prod)
        with self.assertRaises(GuardError):
            allow_endpoint(prod, dev, prod)

    def test_cleanup_uses_record_id_and_retries_unprocessed_keys(self):
        database = database_id_for_run("17")
        plan = delete_batch(DEV_TABLE, database, ["entry-1"])
        encoded = json.dumps(plan)
        self.assertNotIn("sk", encoded)
        self.assertIn("record_id", encoded)
        self.assertEqual(plan["RequestItems"][DEV_TABLE][0]["DeleteRequest"]["Key"]["record_id"]["S"],
                         "entry-1")
        query = query_request(DEV_TABLE, database)
        self.assertEqual(query["TableName"], DEV_TABLE)
        self.assertNotIn("Scan", json.dumps(query))
        response = {"Items": [{"database_id": {"S": database},
                               "record_id": {"S": "entry-1"}}]}
        self.assertEqual(record_ids_from_query(response, database), ["entry-1"])
        pending = {"UnprocessedItems": {DEV_TABLE: plan["RequestItems"][DEV_TABLE]}}
        self.assertEqual(unprocessed_ids(pending, DEV_TABLE, database), ["entry-1"])
        with self.assertRaises(GuardError):
            unprocessed_ids({"UnprocessedItems": {"Other": []}}, DEV_TABLE, database)

    def test_routes_and_trim_proof(self):
        self.assertEqual(path_for("create-database"),
                         "/api/cloud/backups/v2/mutation-log/create-database")
        self.assertEqual(scenario("trim").writers, TRIM_WRITERS)
        self.assertGreater(TRIM_WRITERS, 100)
        self.assertEqual(require_trim({"through": "4", "snapshot_due": False}), 4)
        with self.assertRaises(Exception):
            require_trim({"through": "0", "snapshot_due": False})
        with self.assertRaises(Exception):
            require_trim({"through": "4", "dropped": "4"})
        prove_trimmed_list(4, 409, {
            "code": "predecessor_conflict", "field": "cursor", "snapshot_through": "4",
        })
        with self.assertRaises(Exception):
            prove_trimmed_list(4, 409, {
                "code": "identity_conflict", "field": "cursor", "snapshot_through": "4",
            })
        entry = {
            "delivery_seq": "5", "accepted_at_ms": "1", "writer_key": "k",
            "writer_device": "d", "writer_seq": "1", "authored_at_ms": "1",
            "namespace": "load", "payload": "load",
        }
        prove_trimmed_list(4, 200, {"entries": [entry], "next": "5"})
        prove_trimmed_list(4, 200, {"entries": [], "next": "4"})
        entry["delivery_seq"] = "4"
        with self.assertRaises(Exception):
            prove_trimmed_list(4, 200, {"entries": [entry], "next": "4"})
        database = database_id_for_run("17")
        created = json.loads(request_body("create-database", {
            "database_id": database, "kind": "personal"}))
        self.assertEqual(created["kind"], "personal")
        self.assertEqual(PAYLOAD, "bG9hZA")
        for operation in ("head", "snapshot", "trim"):
            self.assertEqual(json.loads(request_body(operation, {"database_id": database})),
                             {"database_id": database})
        self.assertNotIn("dropped", json.dumps(created))
        steady = scenario("100")
        self.assertEqual(steady.writers, 100)
        self.assertEqual(steady.steady_ms, STEADY_MS)
        self.assertTrue(still_running(steady, STEADY_MS - 1))
        self.assertFalse(still_running(steady, STEADY_MS))
        self.assertTrue(still_running(scenario("5"), STEADY_MS * 10))


if __name__ == "__main__":
    unittest.main()
