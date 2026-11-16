"""Explicit benchmark-only client for current record operations.

Partial-edit benchmark workloads become client read/edit/replace operations.
Reads, serialization, conflicts, and bounded retries remain inside the measured
call; no record payload cache or server compatibility path exists.
"""

from collections import Counter
import copy
import json
import threading
import time


def coordinate(key):
    return key.get("hash"), key.get("range")


def declaration(body):
    body = copy.deepcopy(body)
    schema = body["schema"]
    if "record_keys" in schema:
        return body
    primary = schema.pop("key", {}) or {}
    ranged = primary.get("range_field")
    definition = {"kind": "hash_range" if ranged else "hash"}
    if ranged:
        definition["range_type"] = schema.get("field_types", {}).get(ranged, "String")
    schema["record_keys"] = {"primary": definition, "alternates": {}}
    return body


class RecordProtocol:
    def __init__(self):
        self.lock, self.totals = threading.Lock(), Counter()

    def status(self):
        with self.lock:
            return {"client": "explicit benchmark protocol adapter", "totals": dict(self.totals),
                    "payload_cache": False, "retry_limit": 8,
                    "mutation_timing_includes_all_client_work": True}

    def call(self, exchange, path, body, *, transport=None, **options):
        started, physical = time.perf_counter_ns(), []

        def send(route, value, **overrides):
            metrics = {}
            settings = {**options, **overrides, "transport": metrics}
            # Distinct IDs retain every physical request in phase correlation.
            request_id = options.get("request_id")
            settings["request_id"] = f"{request_id}/rpc-{len(physical)}" if request_id else None
            try:
                return exchange(route, value, **settings)
            finally:
                physical.append({"path": route, "request_id": settings["request_id"], **metrics})

        try:
            if path not in ("/api/schemas/seed-system", "/api/mutation", "/api/mutations/batch"):
                return send(path, body)
            decoded = json.loads(body) if isinstance(body, bytes) else body
            if path == "/api/schemas/seed-system":
                result = send(path, declaration(decoded))
            else:
                operations = [decoded] if path == "/api/mutation" else decoded["mutations"]
                result = (send(path, decoded) if all("record" in item for item in operations)
                          else self.mutations(send, path, decoded))
            return result[0], (time.perf_counter_ns() - started) / 1e6, result[2]
        finally:
            total = {name: sum(item.get(name, 0) for item in physical)
                     for name in ("request_body_bytes", "response_body_bytes")}
            total.update(rpc_count=len(physical), physical_requests=physical,
                         retries=sum(item.get("response_status") == 409 for item in physical))
            if transport is not None:
                transport.update(total)
            with self.lock:
                self.totals.update(logical_calls=1, physical_rpcs=len(physical), **{
                    name: total[name] for name in ("request_body_bytes", "response_body_bytes")})

    def read_current(self, send, mutations):
        targets = [m for m in mutations if m["mutation_type"] != "create"]
        if not targets:
            return {}
        schemas = {m["schema"] for m in targets}
        if len(schemas) != 1:
            raise ValueError("benchmark batch must target a single schema")
        query = {"schema_name": next(iter(schemas)), "filter": {"HashRangeKeys": [
            [m["key_value"]["hash"], m["key_value"].get("range") or ""] for m in targets]},
            "limit": len(targets)}
        reply = send("/api/query", query, receipt=True)[0]
        return {coordinate(row["key"]): row for row in reply["results"]}

    def mutations(self, send, path, body):
        original = [body] if path == "/api/mutation" else body["mutations"]
        for attempt in range(9):
            current = self.read_current(send, original)
            converted = []
            for mutation in original:
                prior = current.get(coordinate(mutation["key_value"]))
                revision = prior and prior["revision"]
                fields = dict(prior["fields"]) if prior else {}
                fields.update(mutation["fields_and_values"])
                deleting = mutation["mutation_type"] in ("delete", "purge")
                if deleting and not prior:
                    raise RuntimeError("benchmark delete target is absent")
                metadata = (prior or {}).get("metadata") or {}
                converted.append({
                    "type": "mutation", "schema": mutation["schema"], "key_value": mutation["key_value"],
                    "mutation_type": mutation["mutation_type"] if deleting else "replace" if revision else "create",
                    "record": {} if deleting else fields, "alternates": {},
                    "expected": {"type": "revision", "revision": revision} if revision else {"type": "absent"},
                    "metadata": None if deleting else mutation.get("metadata", metadata.get("metadata")),
                    "source_file_name": None if deleting else mutation.get("source_file_name", metadata.get("source_file_name")),
                    "file": None if deleting else metadata.get("file"),
                    "durability": mutation.get("durability", "durable"),
                    "convergence": mutation.get("convergence", "async"),
                })
            try:
                reply = send(path, converted[0] if path == "/api/mutation" else {"mutations": converted})
            except RuntimeError as error:
                if getattr(error, "status", None) != 409 or attempt == 8:
                    raise
                with self.lock:
                    self.totals.update(conflicts=1, retries=1)
                continue
            ids = [reply[0]["mutation_id"]] if path == "/api/mutation" else reply[0]["mutation_ids"]
            if len(ids) != len(converted) or len(set(ids)) != len(ids):
                raise RuntimeError("benchmark response has incomplete server identities")
            return reply
