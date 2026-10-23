"""Actual TTL, molecule-drain, and counter-verifier use on the owned product node."""
import time


class LifecycleOperations:
    def ttl_and_drop(self):
        schema = "AtomLifecycleExpiry"
        fields = ["bucket", "id", "value"]
        self.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
            "source": "system_seed", "name": schema, "descriptive_name": schema,
            "schema_type": "HashRange", "key": {"hash_field": "bucket", "range_field": "id"},
            "fields": fields, "field_descriptions": {
                field: f"generated lifecycle expiry {field}" for field in fields}}})
        prefix = f"{int(time.time()) - 60:020d}"
        mutations = []
        for number in range(24):
            key = f"{prefix}-{number:04d}"
            mutations.append({"type": "mutation", "schema": schema, "mutation_type": "create",
                "durability": "durable", "key_value": {"hash": "expiry", "range": key},
                "fields_and_values": {"bucket": "expiry", "id": key, "value": f"value-{number}"}})
        self.call("/api/mutations/batch", {"mutations": mutations})
        self.call("/api/db/schema-retention", {"action": "set", "schema": schema,
            "ttl_seconds": 1, "hash_partitions": ["expiry"], "confirm_expire_existing": True})
        started, deadline, updates = time.monotonic(), time.monotonic() + 90, 0
        while time.monotonic() < deadline:
            result = self.call("/api/query", {"schema_name": schema, "fields": fields,
                "filter": {"HashRangeRange": {"hash": "expiry", "start": "00000000000000000000",
                    "end": "99999999999999999999"}}, "limit": 64})
            if not result.get("results"):
                break
            self.mutate("update", [127], 500 + updates, durability="durable")
            updates += 1
            time.sleep(0.1)
        else:
            raise RuntimeError("The actual TTL sweep did not remove the expired generated keys")
        self.remember("ttl-expiry", schema=schema, expired_records=24,
                      elapsed_seconds=time.monotonic() - started, foreground_updates=updates)
        self.call("/api/schemas/drop", {"schema": schema, "must_exist": True})
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            report = self.call("/api/db/molecule-drain", {})["molecule_drain"]
            if report.get("candidate_rows") == 0:
                self.remember("molecule-drain-after-drop", report=report)
                break
            time.sleep(0.25)
        else:
            raise RuntimeError("The dropped generated schema left drain candidates")
        self.verify("after-ttl-and-drop")

    def verify_counters(self):
        cursor = None
        for _ in range(256):
            request = {"max_pages": 64, "max_ms": 3000, "pause_ms": 0}
            if cursor:
                request["cursor"] = cursor
            report = self.call("/api/db/verify-refs", request)["verify_refs"]
            if report.get("done"):
                self.remember("counter-verifier-complete", report=report)
                if report.get("mismatches_total") != 0 or report.get("tally_complete") is not True:
                    raise RuntimeError("The complete product counter verifier reports a fault or incomplete tally")
                return
            following = report.get("next_cursor")
            if not following or following == cursor:
                raise RuntimeError("The product counter verifier did not advance")
            cursor = following
        raise RuntimeError("The product counter verifier exceeded its finite step bound")
