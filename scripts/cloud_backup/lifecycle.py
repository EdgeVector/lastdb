"""Real TTL mutations across a frozen snapshot and its accepted log successor."""

import time

from support import OperationError, require


class Lifecycle:
    def __init__(self, source, baseline):
        self.source, self.baseline = source, baseline
        self.schema = "CloudAtomLifecycle" + source.source[:12]
        key = baseline["records"][0]["id"]
        query = {"schema_name": baseline["schema"], "fields": ["file"], "filter": {"HashKey": key}, "limit": 2}
        reply = source.node.call("/api/queries/batch", {"queries": [query]})["results"][0]
        value = reply["response"]
        require(reply["status"] == 200 and value["ok"] is True and value["returned_count"] == 1
                and len(value["results"]) == 1 and not value.get("has_more", False)
                and value.get("unresolved_rows", 0) == 0
                and value["results"][0]["key"] == {"hash": key, "range": None}, "lifecycle_shared_file")
        self.pointer = value["results"][0]["fields"]["file"]
        require(isinstance(self.pointer, dict) and "$lastdb_file" in self.pointer, "lifecycle_file_pointer")
        self.expired = [f"{1:020d}-expired-{n}" for n in range(4)]
        self.live = f"{int(time.time()) + 604800:020d}-live"

    def seed(self):
        fields = ["bucket", "id", "note", "file"]
        self.source.node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
            "source": "system_seed", "name": self.schema, "descriptive_name": self.schema,
            "schema_type": "HashRange", "key": {"hash_field": "bucket", "range_field": "id"},
            "fields": fields, "field_types": {"bucket": "String", "id": "String"},
            "field_descriptions": {name: self.schema + " " + name for name in fields}}})
        self.write([*self.expired[:2], self.live])
        self.source.node.call("/api/db/schema-retention", {"action": "set", "schema": self.schema,
            "ttl_seconds": 30, "hash_partitions": ["lifecycle"], "confirm_expire_existing": True})
        self.verify(self.source.node, initial=True)

    def write(self, keys):
        mutations = [{"type": "mutation", "schema": self.schema, "mutation_type": "create",
            "durability": "durable", "key_value": {"hash": "lifecycle", "range": key},
            "fields_and_values": {"bucket": "lifecycle", "id": key,
                                  "note": "shared immutable lifecycle value", "file": self.pointer}}
            for key in keys]
        reply = self.source.node.call("/api/mutations/batch", {"mutations": mutations})
        require(len(reply["mutation_ids"]) == len(keys), "lifecycle_writes_acknowledged")

    def replies(self, node):
        keys = [*self.expired, self.live]
        queries = [{"schema_name": self.schema, "fields": ["bucket", "id", "note", "file"],
                    "filter": {"HashRangeKey": {"hash": "lifecycle", "range": key}},
                    "limit": 2, "offset": 0} for key in keys]
        result = node.call("/api/queries/batch", {"queries": queries})["results"]
        require(len(result) == len(keys), "lifecycle_keyed_read_count")
        for reply in result:
            require(reply["status"] == 200 and reply["response"]["ok"] is True
                    and reply["response"].get("unresolved_rows", 0) == 0
                    and not reply["response"].get("has_more", False), "lifecycle_complete_read")
        return list(zip(keys, (reply["response"] for reply in result)))

    def verify(self, node, initial=False, after_second=False):
        for key, reply in self.replies(node):
            present = key == self.live or (initial and key in self.expired[:2])
            rows = reply["results"]
            require(reply["returned_count"] == int(present) and len(rows) == int(present),
                    "lifecycle_exact_presence_at_selected_cut")
            if present:
                require(rows[0]["key"] == {"hash": "lifecycle", "range": key}
                        and rows[0]["fields"] == {"bucket": "lifecycle", "id": key,
                            "note": "shared immutable lifecycle value", "file": self.pointer},
                        "lifecycle_live_shared_value")
        node.verify_file(self.pointer, self.baseline)
        return {"expired_absent": 0 if initial else 4, "expired_present_at_base": 2 if initial else 0,
                "ttl_erased_records": 0 if initial else (4 if after_second else 2),
                "not_yet_created": 0 if after_second else 2,
                "live_exact": 1, "shared_file_bytes": self.baseline["file_bytes"],
                "shared_file_sha256": self.baseline["file_sha256"], "ttl_worker_disabled": not node.ttl_sweep}

    def await_expiry(self, count):
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            rows = self.replies(self.source.node)
            if all(reply["returned_count"] == 0 for _, reply in rows[:count]):
                ledger = self.source.node.call("/api/db/delete-ledger")["delete_ledger"]["entries"]
                removed = sum(entry["records_converged"] for entry in ledger
                              if entry["schema"] == self.schema and entry["committed"]
                              and entry["caller"] == "ttl-sweep-purge" and entry["verb"] == "purge")
                require(removed >= count, "ttl_native_committed_delete_evidence")
                require(rows[-1][1]["returned_count"] == 1, "ttl_kept_live_key")
                status = self.source.cloud("status")
                journal = status["mutation_log"]
                require(journal["enabled"] and journal["gap"] is None
                        and not journal["snapshot_required"], "ttl_complete_log_coverage")
                return dict(journal["head"]["frontiers"])
            time.sleep(0.5)
        raise OperationError("ttl_cloud_expiry_deadline")
